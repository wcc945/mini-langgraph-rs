use std::collections::{HashMap, HashSet};

use crate::channel::{DynChannel, StateValue};
use crate::managed::ManagedValueSpec;
use crate::pregel::node::PregelNode;
use crate::pregel::pregel::{Pregel, StreamMode};
use crate::pregel::task::PregelTaskManager;

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
    pub(crate) channels: &'a mut HashMap<String, Box<DynChannel>>,
    pub(crate) managed: &'a mut HashMap<String, Box<dyn ManagedValueSpec>>,
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
    pub(crate) task_manager: PregelTaskManager<StateT, UpdateT, ContextT>,
    pub(crate) updated_channels: Option<HashSet<String>>,
    pub(crate) output: Option<StateValue>,
}

impl<'a, StateT, UpdateT, ContextT> PregelLoop<'a, StateT, UpdateT, ContextT> {
    pub(crate) fn new(
        pregel: &'a mut Pregel<StateT, UpdateT, ContextT>,
        input: Option<StateValue>,
    ) -> Self {
        let Pregel {
            nodes,
            channels,
            managed,
            input_channels,
            output_channels,
            stream_channels,
            stream_mode,
            recursion_limit,
            trigger_to_nodes,
            name,
        } = pregel;
        let recursion_limit = *recursion_limit;
        let stop = recursion_limit + 1;

        Self {
            nodes,
            channels,
            managed,
            input_channels,
            output_channels,
            stream_channels: stream_channels.as_deref(),
            stream_mode: *stream_mode,
            recursion_limit,
            trigger_to_nodes,
            name,
            input,
            step: 0,
            stop,
            status: PregelLoopStatus::Input,
            task_manager: PregelTaskManager::new(),
            updated_channels: None,
            output: None,
        }
    }

    pub(crate) fn tick(&mut self) {}

    pub(crate) fn execute(&mut self) {}

    pub(crate) fn after_tick(&mut self) {}
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::channel::last_value::LastValue;
    use crate::graph::node::NodeOutput;
    use crate::pregel::node::PregelNode;

    fn channel() -> Box<DynChannel> {
        Box::new(LastValue::new())
    }

    fn valid_pregel() -> Pregel<i64, i64, ()> {
        Pregel::new(
            HashMap::from([(
                "a".to_string(),
                PregelNode::new(
                    vec!["input".to_string()],
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
            HashMap::new(),
            vec!["input".to_string()],
            vec!["output".to_string()],
        )
        .unwrap()
    }

    #[test]
    fn initializes_sync_loop_skeleton() {
        let mut pregel = valid_pregel();
        let expected_stop = pregel.recursion_limit + 1;
        let mut loop_state = PregelLoop::new(&mut pregel, Some(StateValue::Number(1.0)));

        loop_state.tick();
        loop_state.execute();
        loop_state.after_tick();

        assert_eq!(loop_state.input, Some(StateValue::Number(1.0)));
        assert_eq!(loop_state.step, 0);
        assert_eq!(loop_state.stop, expected_stop);
        assert_eq!(loop_state.status, PregelLoopStatus::Input);
        assert_eq!(loop_state.nodes.len(), 1);
        assert_eq!(loop_state.channels.len(), 2);
        assert_eq!(loop_state.managed.len(), 0);
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
