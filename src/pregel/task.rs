use std::collections::HashMap;

use crate::channel::StateValue;
use crate::channel::channel_writer::ChannelWriter;
use crate::error::GraphError;
use crate::graph::node::NodeOutput;
use crate::pregel::node::PregelNodeBound;

pub(crate) struct PregelExecutableTask<StateT, UpdateT, ContextT> {
    pub(crate) name: String,
    pub(crate) input: StateT,
    pub(crate) bound: PregelNodeBound<StateT, UpdateT, ContextT>,
    pub(crate) writes: Vec<(String, StateValue)>,
    pub(crate) writers: Vec<ChannelWriter<StateT, ContextT>>,
    pub(crate) triggers: Vec<String>,
    pub(crate) id: String,
    pub(crate) path: Vec<String>,
}

pub(crate) struct PregelTaskManager<StateT, UpdateT, ContextT> {
    tasks: HashMap<String, PregelExecutableTask<StateT, UpdateT, ContextT>>,
}

impl<StateT, UpdateT, ContextT> PregelTaskManager<StateT, UpdateT, ContextT> {
    pub(crate) fn new() -> Self {
        Self {
            tasks: HashMap::new(),
        }
    }

    pub(crate) fn submit_task(&mut self, _task: PregelExecutableTask<StateT, UpdateT, ContextT>) {
        todo!()
    }

    pub(crate) fn prepare_tasks(&mut self) -> Vec<PregelExecutableTask<StateT, UpdateT, ContextT>> {
        todo!()
    }

    pub(crate) fn prepare_task(
        &mut self,
        _name: String,
        _input: StateT,
    ) -> PregelExecutableTask<StateT, UpdateT, ContextT> {
        todo!()
    }

    pub(crate) fn execute_task(
        &mut self,
        _task: PregelExecutableTask<StateT, UpdateT, ContextT>,
    ) -> Result<NodeOutput<UpdateT>, GraphError> {
        todo!()
    }
}
