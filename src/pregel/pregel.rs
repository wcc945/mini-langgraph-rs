use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::channel::{DynChannel, StateValue};
use crate::error::GraphError;
use crate::managed::ManagedValueSpec;
use crate::pregel::consts::{DEFAULT_NAME, DEFAULT_RECURSION_LIMIT};
use crate::pregel::loops::PregelLoop;
use crate::pregel::node::PregelNode;
use crate::runtime::RuntimeContext;
use tokio::sync::mpsc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamMode {
    Values,
    Updates,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PregelStreamItem {
    pub step: usize,
    pub mode: StreamMode,
    pub data: StateValue,
}

pub(crate) struct Pregel<StateT, UpdateT, ContextT> {
    pub(crate) nodes: HashMap<String, PregelNode<StateT, UpdateT, ContextT>>,
    pub(crate) channels: HashMap<String, Box<DynChannel>>,
    pub(crate) managed: HashMap<String, Box<dyn ManagedValueSpec>>,
    pub(crate) input_channels: Vec<String>,
    pub(crate) output_channels: Vec<String>,
    pub(crate) stream_channels: Option<Vec<String>>,
    pub(crate) stream_mode: StreamMode,
    pub(crate) recursion_limit: usize,
    pub(crate) trigger_to_nodes: HashMap<String, Vec<String>>,
    pub(crate) name: String,
}

impl<StateT, UpdateT, ContextT> Pregel<StateT, UpdateT, ContextT> {
    pub(crate) fn new(
        nodes: HashMap<String, PregelNode<StateT, UpdateT, ContextT>>,
        channels: HashMap<String, Box<DynChannel>>,
        managed: HashMap<String, Box<dyn ManagedValueSpec>>,
        input_channels: Vec<String>,
        output_channels: Vec<String>,
    ) -> Result<Self, GraphError> {
        let mut pregel = Self {
            nodes,
            channels,
            managed,
            input_channels,
            output_channels,
            stream_channels: None,
            stream_mode: StreamMode::Values,
            recursion_limit: DEFAULT_RECURSION_LIMIT,
            trigger_to_nodes: HashMap::new(),
            name: DEFAULT_NAME.to_string(),
        };

        pregel.validate()?;
        Ok(pregel)
    }

    pub(crate) fn validate(&mut self) -> Result<(), GraphError> {
        if self.recursion_limit == 0 {
            return Err(GraphError::InvalidPregelRecursionLimit(
                self.recursion_limit,
            ));
        }

        let mut subscribed_channels = HashSet::new();

        for (node_name, node) in &self.nodes {
            for channel in &node.channels {
                if !self.channels.contains_key(channel) && !self.managed.contains_key(channel) {
                    return Err(GraphError::UnknownPregelReadChannel {
                        node: node_name.clone(),
                        channel: channel.clone(),
                    });
                }
            }

            for trigger in &node.triggers {
                if !self.channels.contains_key(trigger) {
                    return Err(GraphError::UnknownPregelTriggerChannel {
                        node: node_name.clone(),
                        channel: trigger.clone(),
                    });
                }
                subscribed_channels.insert(trigger.clone());
            }
        }

        for channel in &self.input_channels {
            if !self.channels.contains_key(channel) {
                return Err(GraphError::UnknownPregelInputChannel(channel.clone()));
            }
        }

        if self
            .input_channels
            .iter()
            .all(|channel| !subscribed_channels.contains(channel))
        {
            return Err(GraphError::PregelInputChannelNotSubscribed(
                self.input_channels.clone(),
            ));
        }

        for channel in &self.output_channels {
            if !self.channels.contains_key(channel) {
                return Err(GraphError::UnknownPregelOutputChannel(channel.clone()));
            }
        }

        if let Some(stream_channels) = &self.stream_channels {
            for channel in stream_channels {
                if !self.channels.contains_key(channel) {
                    return Err(GraphError::UnknownPregelStreamChannel(channel.clone()));
                }
            }
        }

        self.trigger_to_nodes = Self::build_trigger_to_nodes(&self.nodes);
        Ok(())
    }

    fn build_trigger_to_nodes(
        nodes: &HashMap<String, PregelNode<StateT, UpdateT, ContextT>>,
    ) -> HashMap<String, Vec<String>> {
        let mut trigger_to_nodes: HashMap<String, Vec<String>> = HashMap::new();

        for (node_name, node) in nodes {
            for trigger in &node.triggers {
                trigger_to_nodes
                    .entry(trigger.clone())
                    .or_default()
                    .push(node_name.clone());
            }
        }

        for nodes in trigger_to_nodes.values_mut() {
            nodes.sort();
        }

        trigger_to_nodes
    }

    pub(crate) fn copy_channels(&self) -> Result<HashMap<String, Box<DynChannel>>, GraphError> {
        self.channels
            .iter()
            .map(|(name, channel)| {
                channel
                    .copy_box()
                    .map(|channel| (name.clone(), channel))
                    .map_err(|error| GraphError::PregelChannelCopyFailed {
                        channel: name.clone(),
                        message: error.to_string(),
                    })
            })
            .collect()
    }

    pub(crate) fn copy_managed(&self) -> HashMap<String, Box<dyn ManagedValueSpec>> {
        self.managed
            .iter()
            .map(|(name, managed)| (name.clone(), managed.copy_box()))
            .collect()
    }
}

impl<StateT, UpdateT, ContextT> Pregel<StateT, UpdateT, ContextT>
where
    StateT: From<crate::channel::StateValue> + Send + 'static,
    UpdateT: Into<crate::channel::StateValue> + Send + 'static,
    ContextT: Default + Send + Sync + 'static,
{
    pub(crate) fn stream(
        self: Arc<Self>,
        input: Option<StateValue>,
        runtime_context: RuntimeContext<ContextT>,
    ) -> Result<mpsc::Receiver<Result<PregelStreamItem, GraphError>>, GraphError> {
        let (sender, receiver) = mpsc::channel(16);

        tokio::spawn(async move {
            let new_error_sender = sender.clone();
            let mut loop_state = match PregelLoop::new(&self, input, runtime_context, sender) {
                Ok(loop_state) => loop_state,
                Err(error) => {
                    let _ = new_error_sender.send(Err(error)).await;
                    return;
                }
            };

            if let Err(error) = loop_state.enter() {
                let _ = loop_state.stream_sender.send(Err(error)).await;
                return;
            }

            loop {
                let should_continue = match loop_state.tick() {
                    Ok(should_continue) => should_continue,
                    Err(error) => {
                        let _ = loop_state.stream_sender.send(Err(error)).await;
                        break;
                    }
                };

                if !should_continue {
                    break;
                }

                if let Err(error) = loop_state.execute() {
                    let _ = loop_state.stream_sender.send(Err(error)).await;
                    break;
                }

                if let Err(error) = loop_state.after_tick() {
                    let _ = loop_state.stream_sender.send(Err(error)).await;
                    break;
                }

                if loop_state.is_stream_closed() {
                    break;
                }
            }
        });

        Ok(receiver)
    }

    pub(crate) fn invoke(
        self: Arc<Self>,
        input: Option<StateValue>,
        runtime_context: RuntimeContext<ContextT>,
    ) -> Result<StateValue, GraphError> {
        let stream_mode = runtime_context.stream_mode.unwrap_or(self.stream_mode);

        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| GraphError::PregelStreamRuntimeFailed(error.to_string()))?;

            runtime.block_on(async move {
                let mut receiver = Arc::clone(&self).stream(input, runtime_context)?;
                let mut latest = StateValue::Null;
                let mut chunks = Vec::new();

                while let Some(item) = receiver.recv().await {
                    let item = item?;
                    match stream_mode {
                        StreamMode::Values if item.mode == StreamMode::Values => {
                            latest = item.data;
                        }
                        StreamMode::Values => {}
                        StreamMode::Updates => chunks.push(Self::stream_item_to_state_value(item)),
                    }
                }

                match stream_mode {
                    StreamMode::Values => Ok(latest),
                    StreamMode::Updates => Ok(StateValue::List(chunks)),
                }
            })
        })
        .join()
        .map_err(|_| GraphError::PregelStreamRuntimeFailed("invoke worker panicked".to_string()))?
    }

    fn stream_item_to_state_value(item: PregelStreamItem) -> StateValue {
        StateValue::Object(HashMap::from([
            ("step".to_string(), StateValue::Number(item.step as f64)),
            (
                "mode".to_string(),
                StateValue::String(match item.mode {
                    StreamMode::Values => "values".to_string(),
                    StreamMode::Updates => "updates".to_string(),
                }),
            ),
            ("data".to_string(), item.data),
        ]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channel::StateValue;
    use crate::channel::channel_writer::{
        ChannelWriteEntry, ChannelWriteValue, ChannelWriter, ChannelWriterEntry,
    };
    use crate::channel::last_value::LastValue;
    use crate::graph::node::NodeOutput;
    use std::sync::Arc;

    struct TestManagedValue;

    impl ManagedValueSpec for TestManagedValue {
        fn copy_box(&self) -> Box<dyn ManagedValueSpec> {
            Box::new(TestManagedValue)
        }
    }

    fn channel() -> Box<DynChannel> {
        Box::new(LastValue::new())
    }

    fn channels(names: &[&str]) -> HashMap<String, Box<DynChannel>> {
        names
            .iter()
            .map(|name| ((*name).to_string(), channel()))
            .collect()
    }

    fn node(channels: Vec<&str>, triggers: Vec<&str>) -> PregelNode<i64, i64, ()> {
        PregelNode::new(
            channels.into_iter().map(str::to_string).collect(),
            triggers.into_iter().map(str::to_string).collect(),
            None,
            Vec::new(),
            Box::new(|_, _| Ok(NodeOutput::None)),
        )
    }

    fn nodes(
        items: Vec<(&str, PregelNode<i64, i64, ()>)>,
    ) -> HashMap<String, PregelNode<i64, i64, ()>> {
        items
            .into_iter()
            .map(|(name, node)| (name.to_string(), node))
            .collect()
    }

    fn valid_pregel() -> Pregel<i64, i64, ()> {
        Pregel::new(
            nodes(vec![
                ("a", node(vec!["input"], vec!["input"])),
                ("b", node(vec!["state"], vec!["state"])),
            ]),
            channels(&["input", "state", "output"]),
            HashMap::new(),
            vec!["input".to_string()],
            vec!["output".to_string()],
        )
        .unwrap()
    }

    fn passthrough_writer(channel: &str) -> ChannelWriter<StateValue, ()> {
        ChannelWriter::new(vec![ChannelWriterEntry::Channel(ChannelWriteEntry {
            channel: channel.to_string(),
            value: ChannelWriteValue::Passthrough,
            skip_none: false,
            mapper: None,
        })])
    }

    fn trigger_writer(channel: &str) -> ChannelWriter<StateValue, ()> {
        ChannelWriter::new(vec![ChannelWriterEntry::Channel(ChannelWriteEntry {
            channel: channel.to_string(),
            value: ChannelWriteValue::Value(StateValue::Null),
            skip_none: false,
            mapper: None,
        })])
    }

    fn stream_pregel() -> Pregel<StateValue, StateValue, ()> {
        Pregel::new(
            HashMap::from([
                (
                    "a".to_string(),
                    PregelNode::<StateValue, StateValue, ()>::new(
                        vec!["input".to_string()],
                        vec!["input".to_string()],
                        None,
                        vec![passthrough_writer("mid"), trigger_writer("to_b")],
                        Box::new(|_, _| {
                            Ok(NodeOutput::Update(StateValue::String("a".to_string())))
                        }),
                    ),
                ),
                (
                    "b".to_string(),
                    PregelNode::<StateValue, StateValue, ()>::new(
                        vec!["mid".to_string()],
                        vec!["to_b".to_string()],
                        None,
                        vec![passthrough_writer("output")],
                        Box::new(|state, _| match state {
                            StateValue::Object(values) => Ok(NodeOutput::Update(
                                values.get("mid").cloned().unwrap_or(StateValue::Null),
                            )),
                            _ => Ok(NodeOutput::None),
                        }),
                    ),
                ),
            ]),
            channels(&["input", "mid", "to_b", "output"]),
            HashMap::new(),
            vec!["input".to_string()],
            vec!["output".to_string()],
        )
        .unwrap()
    }

    fn expect_pregel_error(result: Result<Pregel<i64, i64, ()>, GraphError>) -> GraphError {
        match result {
            Ok(_) => panic!("Pregel construction should fail"),
            Err(error) => error,
        }
    }

    #[test]
    fn new_validates_and_builds_trigger_index() {
        let pregel = valid_pregel();

        assert_eq!(
            pregel.trigger_to_nodes.get("input"),
            Some(&vec!["a".to_string()])
        );
        assert_eq!(
            pregel.trigger_to_nodes.get("state"),
            Some(&vec!["b".to_string()])
        );
        assert_eq!(pregel.stream_mode, StreamMode::Values);
        assert_eq!(pregel.recursion_limit, DEFAULT_RECURSION_LIMIT);
        assert_eq!(pregel.name, DEFAULT_NAME);
    }

    #[test]
    fn validate_rejects_unknown_read_channel() {
        let error = expect_pregel_error(Pregel::new(
            nodes(vec![("a", node(vec!["missing"], vec!["input"]))]),
            channels(&["input", "output"]),
            HashMap::new(),
            vec!["input".to_string()],
            vec!["output".to_string()],
        ));

        assert!(matches!(
            error,
            GraphError::UnknownPregelReadChannel { node, channel }
                if node == "a" && channel == "missing"
        ));
    }

    #[test]
    fn validate_rejects_unknown_trigger_channel() {
        let error = expect_pregel_error(Pregel::new(
            nodes(vec![("a", node(vec!["input"], vec!["missing"]))]),
            channels(&["input", "output"]),
            HashMap::new(),
            vec!["input".to_string()],
            vec!["output".to_string()],
        ));

        assert!(matches!(
            error,
            GraphError::UnknownPregelTriggerChannel { node, channel }
                if node == "a" && channel == "missing"
        ));
    }

    #[test]
    fn validate_allows_node_to_read_managed_value() {
        let mut managed: HashMap<String, Box<dyn ManagedValueSpec>> = HashMap::new();
        managed.insert("managed".to_string(), Box::new(TestManagedValue));

        let pregel = Pregel::new(
            nodes(vec![("a", node(vec!["managed"], vec!["input"]))]),
            channels(&["input", "output"]),
            managed,
            vec!["input".to_string()],
            vec!["output".to_string()],
        );

        assert!(pregel.is_ok());
    }

    #[test]
    fn validate_rejects_input_channel_without_subscriber() {
        let error = expect_pregel_error(Pregel::new(
            nodes(vec![("a", node(vec!["state"], vec!["state"]))]),
            channels(&["input", "state", "output"]),
            HashMap::new(),
            vec!["input".to_string()],
            vec!["output".to_string()],
        ));

        assert!(matches!(
            error,
            GraphError::PregelInputChannelNotSubscribed(channels)
                if channels == vec!["input".to_string()]
        ));
    }

    #[test]
    fn validate_rejects_unknown_output_or_stream_channel() {
        let output_error = expect_pregel_error(Pregel::new(
            nodes(vec![("a", node(vec!["input"], vec!["input"]))]),
            channels(&["input"]),
            HashMap::new(),
            vec!["input".to_string()],
            vec!["missing".to_string()],
        ));
        let mut pregel = valid_pregel();
        pregel.stream_channels = Some(vec!["missing".to_string()]);
        let stream_error = pregel.validate().unwrap_err();

        assert!(matches!(
            output_error,
            GraphError::UnknownPregelOutputChannel(channel) if channel == "missing"
        ));
        assert!(matches!(
            stream_error,
            GraphError::UnknownPregelStreamChannel(channel) if channel == "missing"
        ));
    }

    #[test]
    fn validate_rejects_zero_recursion_limit() {
        let mut pregel = valid_pregel();
        pregel.recursion_limit = 0;

        let error = pregel.validate().unwrap_err();

        assert!(matches!(error, GraphError::InvalidPregelRecursionLimit(0)));
    }

    #[tokio::test]
    async fn stream_sends_values_items() {
        let pregel = Arc::new(stream_pregel());
        let mut receiver = pregel
            .stream(
                Some(StateValue::String("start".to_string())),
                RuntimeContext::default(),
            )
            .unwrap();

        let item = receiver.recv().await.unwrap().unwrap();

        assert_eq!(item.step, 1);
        assert_eq!(item.mode, StreamMode::Values);
        assert_eq!(item.data, StateValue::String("a".to_string()));
        assert!(receiver.recv().await.is_none());
    }

    #[tokio::test]
    async fn stream_sends_updates_items() {
        let mut pregel = stream_pregel();
        pregel.stream_mode = StreamMode::Updates;
        let mut receiver = Arc::new(pregel)
            .stream(
                Some(StateValue::String("start".to_string())),
                RuntimeContext::default(),
            )
            .unwrap();

        let item = receiver.recv().await.unwrap().unwrap();

        assert_eq!(item.step, 1);
        assert_eq!(item.mode, StreamMode::Updates);
        assert_eq!(
            item.data,
            StateValue::Object(HashMap::from([(
                "b".to_string(),
                StateValue::String("a".to_string())
            )]))
        );
        assert!(receiver.recv().await.is_none());
    }

    #[tokio::test]
    async fn stream_context_overrides_default_stream_mode() {
        let pregel = Arc::new(stream_pregel());
        let context = RuntimeContext::new(()).with_stream_mode(StreamMode::Updates);
        let mut receiver = pregel
            .stream(Some(StateValue::String("start".to_string())), context)
            .unwrap();

        let item = receiver.recv().await.unwrap().unwrap();

        assert_eq!(item.step, 1);
        assert_eq!(item.mode, StreamMode::Updates);
        assert_eq!(
            item.data,
            StateValue::Object(HashMap::from([(
                "b".to_string(),
                StateValue::String("a".to_string())
            )]))
        );
        assert!(receiver.recv().await.is_none());
    }

    #[tokio::test]
    async fn stream_uses_runtime_context_stream_mode() {
        let pregel = Arc::new(stream_pregel());
        let context = RuntimeContext::new(()).with_stream_mode(StreamMode::Updates);
        let mut receiver = pregel
            .stream(Some(StateValue::String("start".to_string())), context)
            .unwrap();

        let item = receiver.recv().await.unwrap().unwrap();

        assert_eq!(item.step, 1);
        assert_eq!(item.mode, StreamMode::Updates);
        assert_eq!(
            item.data,
            StateValue::Object(HashMap::from([(
                "b".to_string(),
                StateValue::String("a".to_string())
            )]))
        );
    }

    #[test]
    fn invoke_runs_to_completion_and_returns_final_output() {
        let pregel = Arc::new(stream_pregel());

        let output = pregel
            .invoke(
                Some(StateValue::String("start".to_string())),
                RuntimeContext::default(),
            )
            .unwrap();

        assert_eq!(output, StateValue::String("a".to_string()));
    }

    #[test]
    fn invoke_with_updates_stream_mode_returns_stream_chunks() {
        let pregel = Arc::new(stream_pregel());
        let context = RuntimeContext::new(()).with_stream_mode(StreamMode::Updates);

        let output = pregel
            .invoke(Some(StateValue::String("start".to_string())), context)
            .unwrap();

        assert_eq!(
            output,
            StateValue::List(vec![StateValue::Object(HashMap::from([
                ("step".to_string(), StateValue::Number(1.0)),
                (
                    "mode".to_string(),
                    StateValue::String("updates".to_string())
                ),
                (
                    "data".to_string(),
                    StateValue::Object(HashMap::from([(
                        "b".to_string(),
                        StateValue::String("a".to_string())
                    )]))
                ),
            ]))])
        );
    }

    #[test]
    fn invoke_propagates_enter_error() {
        let pregel = Arc::new(stream_pregel());

        let error = pregel.invoke(None, RuntimeContext::default()).unwrap_err();

        assert!(
            matches!(error, GraphError::EmptyPregelInput(channels) if channels == vec!["input"])
        );
    }

    #[test]
    fn invoke_propagates_execute_error() {
        let pregel = Arc::new(
            Pregel::new(
                HashMap::from([(
                    "a".to_string(),
                    PregelNode::<StateValue, StateValue, ()>::new(
                        vec!["input".to_string()],
                        vec!["input".to_string()],
                        None,
                        Vec::new(),
                        Box::new(|_, _| Err(GraphError::InvalidPregelInput("bad".to_string()))),
                    ),
                )]),
                channels(&["input", "output"]),
                HashMap::new(),
                vec!["input".to_string()],
                vec!["output".to_string()],
            )
            .unwrap(),
        );

        let error = pregel
            .invoke(
                Some(StateValue::String("start".to_string())),
                RuntimeContext::default(),
            )
            .unwrap_err();

        assert!(matches!(
            error,
            GraphError::PregelTaskFailed { node, message }
                if node == "a" && message == "invalid Pregel input: bad"
        ));
    }

    #[test]
    fn invoke_propagates_tick_error() {
        let mut pregel = Pregel::new(
            HashMap::from([(
                "loop".to_string(),
                PregelNode::<StateValue, StateValue, ()>::new(
                    vec!["input".to_string()],
                    vec!["input".to_string()],
                    None,
                    vec![trigger_writer("input")],
                    Box::new(|_, _| Ok(NodeOutput::None)),
                ),
            )]),
            channels(&["input", "output"]),
            HashMap::new(),
            vec!["input".to_string()],
            vec!["output".to_string()],
        )
        .unwrap();
        pregel.recursion_limit = 1;

        let error = Arc::new(pregel)
            .invoke(
                Some(StateValue::String("start".to_string())),
                RuntimeContext::default(),
            )
            .unwrap_err();

        assert!(matches!(error, GraphError::PregelRecursionLimitReached(1)));
    }

    #[test]
    fn invoke_propagates_after_tick_error() {
        let pregel = Arc::new(
            Pregel::new(
                HashMap::from([
                    (
                        "a".to_string(),
                        PregelNode::<StateValue, StateValue, ()>::new(
                            vec!["input".to_string()],
                            vec!["input".to_string()],
                            None,
                            vec![passthrough_writer("output")],
                            Box::new(|_, _| Ok(NodeOutput::Update(StateValue::Number(1.0)))),
                        ),
                    ),
                    (
                        "b".to_string(),
                        PregelNode::<StateValue, StateValue, ()>::new(
                            vec!["input".to_string()],
                            vec!["input".to_string()],
                            None,
                            vec![passthrough_writer("output")],
                            Box::new(|_, _| Ok(NodeOutput::Update(StateValue::Number(2.0)))),
                        ),
                    ),
                ]),
                channels(&["input", "output"]),
                HashMap::new(),
                vec!["input".to_string()],
                vec!["output".to_string()],
            )
            .unwrap(),
        );

        let error = pregel
            .invoke(
                Some(StateValue::String("start".to_string())),
                RuntimeContext::default(),
            )
            .unwrap_err();

        assert!(matches!(
            error,
            GraphError::MultipleUpdatesWithoutReducer { count: 2 }
        ));
    }

    #[tokio::test]
    async fn stream_sends_enter_error_for_empty_input() {
        let pregel = Arc::new(stream_pregel());
        let mut receiver = pregel.stream(None, RuntimeContext::default()).unwrap();

        let error = receiver.recv().await.unwrap().unwrap_err();

        assert!(
            matches!(error, GraphError::EmptyPregelInput(channels) if channels == vec!["input"])
        );
        assert!(receiver.recv().await.is_none());
    }
}
