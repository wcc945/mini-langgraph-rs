use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::channel::channel_writer::{
    ChannelTupleMapper, ChannelWriteEntry, ChannelWriteTupleEntry, ChannelWriteValue,
    ChannelWriter, ChannelWriterEntry,
};
use crate::channel::ephemeral_value::EphemeralValue;
use crate::channel::named_barrier_value::NamedBarrierValue;
use crate::channel::{DynChannel, StateValue};
use crate::error::GraphError;
use crate::graph::branch::{BranchOutput, BranchSpec};
use crate::graph::consts::{END, START};
use crate::graph::node::{NodeOutput, StateNodeSpec};
use crate::managed::ManagedValueSpec;
use crate::pregel::node::PregelNode;
use crate::pregel::pregel::{Pregel, PregelStreamItem, StreamMode};
use tokio::sync::mpsc;

pub struct CompiledStateGraph<StateT, UpdateT, ContextT = (), InputT = StateT, OutputT = StateT> {
    pub(crate) pregel: Arc<Pregel<StateT, UpdateT, ContextT>>,
    _marker: PhantomData<(InputT, OutputT)>,
}

impl<StateT, UpdateT, ContextT, InputT, OutputT>
    CompiledStateGraph<StateT, UpdateT, ContextT, InputT, OutputT>
where
    StateT: 'static,
    ContextT: 'static,
{
    pub(crate) fn new(
        channels: HashMap<String, Box<DynChannel>>,
        managed: HashMap<String, Box<dyn ManagedValueSpec>>,
        output_channels: Vec<String>,
    ) -> Self {
        Self {
            pregel: Arc::new(Pregel {
                nodes: HashMap::new(),
                channels,
                managed,
                input_channels: vec![START.to_string()],
                output_channels: output_channels.clone(),
                stream_channels: Some(output_channels),
                stream_mode: StreamMode::Values,
                recursion_limit: 25,
                trigger_to_nodes: HashMap::new(),
                name: "LangGraph".to_string(),
            }),
            _marker: PhantomData,
        }
    }

    fn pregel_mut(&mut self) -> &mut Pregel<StateT, UpdateT, ContextT> {
        Arc::get_mut(&mut self.pregel).expect("compiled graph spec is shared during construction")
    }

    pub(crate) fn attach_node(
        &mut self,
        key: String,
        node: Option<StateNodeSpec<StateT, UpdateT, ContextT>>,
    ) {
        if key == START {
            self.pregel_mut()
                .channels
                .entry(START.to_string())
                .or_insert_with(|| Box::new(EphemeralValue::new(false)) as Box<DynChannel>);
            self.pregel_mut().nodes.insert(
                key,
                PregelNode::new(
                    vec![START.to_string()],
                    vec![START.to_string()],
                    None,
                    Vec::new(),
                    Box::new(|_, _| Ok(NodeOutput::None)),
                ),
            );
            return;
        }

        let Some(node) = node else {
            return;
        };

        let read_channels = self.node_read_channels();
        let writers = vec![Self::state_writer(read_channels.clone())];
        let trigger = Self::branch_trigger_channel(&key);
        self.pregel_mut()
            .channels
            .entry(trigger.clone())
            .or_insert_with(|| Box::new(EphemeralValue::new(false)) as Box<DynChannel>);

        self.pregel_mut().nodes.insert(
            key,
            PregelNode::new(read_channels, vec![trigger], None, writers, node.runnable),
        );
    }

    pub(crate) fn attach_edge(&mut self, starts: Vec<String>, end: &str) {
        if starts.is_empty() || end == END {
            return;
        }

        if starts.len() > 1 {
            let trigger = Self::join_trigger_channel(&starts, end);

            self.pregel_mut().channels.insert(
                trigger.clone(),
                Box::new(NamedBarrierValue::new(starts.iter().cloned())) as Box<DynChannel>,
            );

            if let Some(node) = self.pregel_mut().nodes.get_mut(end) {
                node.triggers.push(trigger.clone());
                node.triggers.sort();
                node.triggers.dedup();
            }

            for start in starts {
                if let Some(node) = self.pregel_mut().nodes.get_mut(&start) {
                    node.writers.push(Self::trigger_writer(
                        trigger.clone(),
                        StateValue::String(start),
                    ));
                }
            }

            return;
        }

        let start = &starts[0];
        let trigger = Self::branch_trigger_channel(end);

        if let Some(node) = self.pregel_mut().nodes.get_mut(start) {
            node.writers
                .push(Self::trigger_writer(trigger, StateValue::Null));
        }
    }

    pub(crate) fn attach_branch(
        &mut self,
        start: &str,
        _name: &str,
        branch: BranchSpec<StateT, ContextT>,
    ) -> Result<(), GraphError> {
        let writer = Self::branch_writer(branch);

        let Some(node) = self.pregel_mut().nodes.get_mut(start) else {
            return Err(GraphError::UnknownEdgeSource(start.to_string()));
        };

        node.writers.push(writer);

        Ok(())
    }

    pub(crate) fn validate(mut self) -> Result<Self, GraphError> {
        self.pregel_mut().validate()?;
        Ok(self)
    }

    pub fn stream(
        &self,
        input: Option<StateValue>,
    ) -> Result<mpsc::Receiver<Result<PregelStreamItem, GraphError>>, GraphError>
    where
        StateT: From<StateValue> + Send + 'static,
        UpdateT: Into<StateValue> + Send + 'static,
        ContextT: Default + Send + Sync + 'static,
    {
        Arc::clone(&self.pregel).stream(input)
    }

    pub fn stream_with_mode(
        &self,
        input: Option<StateValue>,
        stream_mode: StreamMode,
    ) -> Result<mpsc::Receiver<Result<PregelStreamItem, GraphError>>, GraphError>
    where
        StateT: From<StateValue> + Send + 'static,
        UpdateT: Into<StateValue> + Send + 'static,
        ContextT: Default + Send + Sync + 'static,
    {
        Arc::clone(&self.pregel).stream_with_mode(input, stream_mode)
    }

    pub fn invoke(&self, input: Option<StateValue>) -> Result<StateValue, GraphError>
    where
        StateT: From<StateValue> + Send + 'static,
        UpdateT: Into<StateValue> + Send + 'static,
        ContextT: Default + Send + Sync + 'static,
    {
        Arc::clone(&self.pregel).invoke(input)
    }

    fn branch_trigger_channel(target: &str) -> String {
        format!("branch:to:{target}")
    }

    fn join_trigger_channel(starts: &[String], target: &str) -> String {
        format!("join:{}:{target}", starts.join("+"))
    }

    fn trigger_writer(channel: String, value: StateValue) -> ChannelWriter<StateT, ContextT> {
        ChannelWriter::new(vec![ChannelWriterEntry::Channel(ChannelWriteEntry {
            channel,
            value: ChannelWriteValue::Value(value),
            skip_none: false,
            mapper: None,
        })])
    }

    fn branch_writer(branch: BranchSpec<StateT, ContextT>) -> ChannelWriter<StateT, ContextT> {
        ChannelWriter::new(vec![ChannelWriterEntry::Executable(Box::new(
            move |state, context| {
                let Some(key) = (branch.path)(state, context) else {
                    return Ok(Vec::new());
                };

                branch
                    .resolve(BranchOutput::One(key))?
                    .into_iter()
                    .filter(|target| target != END)
                    .map(|target| {
                        Ok(ChannelWriteEntry {
                            channel: Self::branch_trigger_channel(&target),
                            value: ChannelWriteValue::Value(StateValue::Null),
                            skip_none: false,
                            mapper: None,
                        })
                    })
                    .collect()
            },
        ))])
    }

    fn node_read_channels(&self) -> Vec<String> {
        let mut channels: Vec<_> = self
            .pregel
            .channels
            .keys()
            .filter(|channel| channel.as_str() != START && !channel.starts_with("branch:to:"))
            .cloned()
            .chain(self.pregel.managed.keys().cloned())
            .collect();

        channels.sort();
        channels.dedup();
        channels
    }

    fn state_writer(output_channels: Vec<String>) -> ChannelWriter<StateT, ContextT> {
        let mapper: ChannelTupleMapper = Box::new(move |value| match value {
            StateValue::Object(values) => Ok(output_channels
                .iter()
                .filter_map(|channel| {
                    values
                        .get(channel)
                        .cloned()
                        .map(|value| (channel.clone(), value))
                })
                .collect()),
            StateValue::Null => Ok(Vec::new()),
            other => Err(GraphError::InvalidChannelUpdate(format!(
                "expected object update, got {other:?}"
            ))),
        });

        ChannelWriter::new(vec![ChannelWriterEntry::Tuple(ChannelWriteTupleEntry {
            mapper,
        })])
    }
}
