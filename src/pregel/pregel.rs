use std::collections::{HashMap, HashSet};

use crate::channel::DynChannel;
use crate::error::GraphError;
use crate::managed::ManagedValueSpec;
use crate::pregel::consts::{DEFAULT_NAME, DEFAULT_RECURSION_LIMIT};
use crate::pregel::node::PregelNode;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StreamMode {
    Values,
    Updates,
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channel::last_value::LastValue;
    use crate::graph::node::NodeOutput;

    struct TestManagedValue;

    impl ManagedValueSpec for TestManagedValue {}

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
}
