use std::collections::{HashMap, HashSet};

use crate::channel::{DynChannel, StateValue};
use crate::error::GraphError;
use crate::managed::ManagedValueSpec;
use crate::pregel::node::PregelNode;
use crate::pregel::pregel::{Pregel, PregelStreamItem, StreamMode};
use crate::pregel::task::PregelTaskManager;
use tokio::sync::mpsc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PregelLoopStatus {
    Input,
    Pending,
    Done,
    Draining,
    InterruptBefore,
    InterruptAfter,
    OutOfSteps,
}

pub(crate) struct PregelLoop<'a, StateT, UpdateT, ContextT> {
    pub(crate) nodes: &'a HashMap<String, PregelNode<StateT, UpdateT, ContextT>>,
    pub(crate) channels: HashMap<String, Box<DynChannel>>,
    pub(crate) managed: HashMap<String, Box<dyn ManagedValueSpec>>,
    pub(crate) input_channels: &'a [String],
    pub(crate) output_channels: &'a [String],
    pub(crate) stream_channels: Option<&'a [String]>,
    pub(crate) stream_mode: StreamMode,
    pub(crate) recursion_limit: usize,
    pub(crate) trigger_to_nodes: &'a HashMap<String, Vec<String>>,
    pub(crate) name: &'a str,
    pub(crate) input: Option<StateValue>,
    pub(crate) step: usize,
    pub(crate) stop: usize,
    pub(crate) status: PregelLoopStatus,
    pub(crate) task_manager: PregelTaskManager<'a, StateT, UpdateT, ContextT>,
    pub(crate) updated_channels: Option<HashSet<String>>,
    pub(crate) output: Option<StateValue>,
    pub(crate) stream_sender: mpsc::Sender<Result<PregelStreamItem, GraphError>>,
    pending_writes: Vec<(String, StateValue)>,
    task_updates: HashMap<String, StateValue>,
}

impl<'a, StateT, UpdateT, ContextT> PregelLoop<'a, StateT, UpdateT, ContextT>
where
    StateT: From<StateValue>,
    UpdateT: Into<StateValue>,
    ContextT: Default,
{
    pub(crate) fn new(
        pregel: &'a Pregel<StateT, UpdateT, ContextT>,
        input: Option<StateValue>,
        stream_sender: mpsc::Sender<Result<PregelStreamItem, GraphError>>,
    ) -> Result<Self, GraphError> {
        let channels = pregel.copy_channels()?;
        let managed = pregel.copy_managed();
        let recursion_limit = pregel.recursion_limit;
        let stop = recursion_limit + 1;

        Ok(Self {
            nodes: &pregel.nodes,
            channels,
            managed,
            input_channels: &pregel.input_channels,
            output_channels: &pregel.output_channels,
            stream_channels: pregel.stream_channels.as_deref(),
            stream_mode: pregel.stream_mode,
            recursion_limit,
            trigger_to_nodes: &pregel.trigger_to_nodes,
            name: &pregel.name,
            input,
            step: 0,
            stop,
            status: PregelLoopStatus::Input,
            task_manager: PregelTaskManager::new(),
            updated_channels: None,
            output: None,
            stream_sender,
            pending_writes: Vec::new(),
            task_updates: HashMap::new(),
        })
    }

    pub(crate) fn tick(&mut self) -> Result<bool, GraphError> {
        Ok(false)
    }

    pub(crate) fn execute(&mut self) -> Result<(), GraphError> {
        Ok(())
    }

    pub(crate) async fn after_tick(&mut self) -> Result<(), GraphError> {
        Ok(())
    }

    pub(crate) fn is_stream_closed(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::channel::last_value::LastValue;
    use crate::graph::node::NodeOutput;
    use crate::managed::ManagedValueSpec;
    use crate::pregel::node::PregelNode;

    struct TestManagedValue;

    impl ManagedValueSpec for TestManagedValue {
        fn copy_box(&self) -> Box<dyn ManagedValueSpec> {
            Box::new(TestManagedValue)
        }
    }

    fn channel() -> Box<DynChannel> {
        Box::new(LastValue::new())
    }

    fn valid_pregel() -> Pregel<StateValue, StateValue, ()> {
        Pregel::new(
            HashMap::from([(
                "a".to_string(),
                PregelNode::new(
                    vec!["input".to_string(), "managed".to_string()],
                    vec!["input".to_string()],
                    None,
                    Vec::new(),
                    Box::new(|_, _| Ok(NodeOutput::None)),
                ),
            )]),
            HashMap::from([
                ("input".to_string(), channel()),
                ("output".to_string(), channel()),
            ]),
            HashMap::from([(
                "managed".to_string(),
                Box::new(TestManagedValue) as Box<dyn ManagedValueSpec>,
            )]),
            vec!["input".to_string()],
            vec!["output".to_string()],
        )
        .unwrap()
    }

    #[test]
    fn initializes_loop_with_copied_channels() {
        let pregel = valid_pregel();
        let expected_stop = pregel.recursion_limit + 1;
        let (sender, _receiver) = mpsc::channel(1);
        let loop_state = PregelLoop::new(&pregel, Some(StateValue::Number(1.0)), sender).unwrap();

        assert_eq!(loop_state.input, Some(StateValue::Number(1.0)));
        assert_eq!(loop_state.step, 0);
        assert_eq!(loop_state.stop, expected_stop);
        assert_eq!(loop_state.status, PregelLoopStatus::Input);
        assert_eq!(loop_state.nodes.len(), 1);
        assert_eq!(loop_state.channels.len(), 2);
        assert_eq!(loop_state.managed.len(), 1);
        assert_eq!(loop_state.input_channels, ["input".to_string()]);
        assert_eq!(loop_state.output_channels, ["output".to_string()]);
        assert_eq!(loop_state.stream_channels, None);
        assert_eq!(loop_state.stream_mode, StreamMode::Values);
        assert_eq!(loop_state.recursion_limit, expected_stop - 1);
        assert_eq!(loop_state.trigger_to_nodes.len(), 1);
        assert_eq!(loop_state.name, "LangGraph");
        assert_eq!(loop_state.updated_channels, None);
        assert_eq!(loop_state.output, None);
    }
}
