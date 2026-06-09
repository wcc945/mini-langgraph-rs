use std::collections::HashMap;
use std::marker::PhantomData;

use crate::channel::channel_writer::{
    ChannelTupleMapper, ChannelWriteEntry, ChannelWriteTupleEntry, ChannelWriteValue,
    ChannelWriter, ChannelWriterEntry,
};
use crate::channel::ephemeral_value::EphemeralValue;
use crate::channel::named_barrier_value::NamedBarrierValue;
use crate::channel::{DynChannel, StateValue};
use crate::error::GraphError;
use crate::graph::branch::BranchSpec;
use crate::graph::consts::{END, START};
use crate::graph::node::StateNodeSpec;
use crate::managed::ManagedValueSpec;
use crate::pregel::node::PregelNode;
use crate::pregel::pregel::{Pregel, StreamMode};

pub struct CompiledStateGraph<StateT, UpdateT, ContextT = (), InputT = StateT, OutputT = StateT> {
    pub(crate) pregel: Pregel<StateT, UpdateT, ContextT>,
    _marker: PhantomData<(InputT, OutputT)>,
}

impl<StateT, UpdateT, ContextT, InputT, OutputT>
    CompiledStateGraph<StateT, UpdateT, ContextT, InputT, OutputT>
{
    pub(crate) fn new(
        channels: HashMap<String, Box<DynChannel>>,
        managed: HashMap<String, Box<dyn ManagedValueSpec>>,
        output_channels: Vec<String>,
    ) -> Self {
        Self {
            pregel: Pregel {
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
            },
            _marker: PhantomData,
        }
    }

    pub(crate) fn attach_node(
        &mut self,
        key: String,
        node: StateNodeSpec<StateT, UpdateT, ContextT>,
    ) {
        let read_channels = self.node_read_channels();
        let writers = vec![Self::state_writer(read_channels.clone())];
        let trigger = Self::branch_trigger_channel(&key);
        self.pregel
            .channels
            .entry(trigger.clone())
            .or_insert_with(|| Box::new(EphemeralValue::new(false)) as Box<DynChannel>);

        self.pregel.nodes.insert(
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

            self.pregel.channels.insert(
                trigger.clone(),
                Box::new(NamedBarrierValue::new(starts.iter().cloned())) as Box<DynChannel>,
            );

            if let Some(node) = self.pregel.nodes.get_mut(end) {
                node.triggers.push(trigger.clone());
                node.triggers.sort();
                node.triggers.dedup();
            }

            for start in starts {
                if let Some(node) = self.pregel.nodes.get_mut(&start) {
                    node.writers.push(Self::trigger_writer(
                        trigger.clone(),
                        StateValue::String(start),
                    ));
                }
            }

            return;
        }

        let start = &starts[0];
        let trigger = if start == START {
            START.to_string()
        } else {
            Self::branch_trigger_channel(end)
        };

        self.pregel
            .channels
            .entry(trigger.clone())
            .or_insert_with(|| Box::new(EphemeralValue::new(false)) as Box<DynChannel>);

        if let Some(node) = self.pregel.nodes.get_mut(end) {
            node.triggers.push(trigger.clone());
            node.triggers.sort();
            node.triggers.dedup();
        }

        if start != START
            && let Some(node) = self.pregel.nodes.get_mut(start)
        {
            node.writers
                .push(Self::trigger_writer(trigger, StateValue::Null));
        }
    }

    pub(crate) fn attach_branch(
        &mut self,
        _start: &str,
        _name: &str,
        _branch: BranchSpec<StateT, ContextT>,
    ) -> Result<(), GraphError> {
        Err(GraphError::UnsupportedCompiledBranches)
    }

    pub(crate) fn validate(mut self) -> Result<Self, GraphError> {
        self.pregel.validate()?;
        Ok(self)
    }

    fn branch_trigger_channel(target: &str) -> String {
        format!("branch:to:{target}")
    }

    fn join_trigger_channel(starts: &[String], target: &str) -> String {
        format!("join:{}:{target}", starts.join("+"))
    }

    fn trigger_writer(channel: String, value: StateValue) -> ChannelWriter {
        ChannelWriter::new(vec![ChannelWriterEntry::Channel(ChannelWriteEntry {
            channel,
            value: ChannelWriteValue::Value(value),
            skip_none: false,
            mapper: None,
        })])
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

    fn state_writer(output_channels: Vec<String>) -> ChannelWriter {
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
