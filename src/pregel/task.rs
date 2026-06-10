use std::collections::HashMap;

use crate::channel::StateValue;
use crate::channel::channel_writer::ChannelWriter;
use crate::error::GraphError;
use crate::pregel::node::PregelNodeBound;
use crate::runtime::RuntimeContext;

pub(crate) struct PregelExecutableTask<'a, StateT, UpdateT, ContextT> {
    pub(crate) name: String,
    pub(crate) input: StateT,
    pub(crate) bound: &'a PregelNodeBound<StateT, UpdateT, ContextT>,
    pub(crate) writes: Vec<(String, StateValue)>,
    pub(crate) writers: &'a [ChannelWriter<StateT, ContextT>],
    pub(crate) triggers: &'a [String],
    pub(crate) id: String,
    pub(crate) path: Vec<String>,
}

pub(crate) struct PregelTaskManager<'a, StateT, UpdateT, ContextT> {
    tasks: HashMap<String, PregelExecutableTask<'a, StateT, UpdateT, ContextT>>,
}

impl<'a, StateT, UpdateT, ContextT> PregelTaskManager<'a, StateT, UpdateT, ContextT> {
    pub(crate) fn new() -> Self {
        Self {
            tasks: HashMap::new(),
        }
    }

    pub(crate) fn submit_task(
        &mut self,
        task: PregelExecutableTask<'a, StateT, UpdateT, ContextT>,
    ) {
        let _ = task;
    }

    pub(crate) fn prepare_tasks(
        &mut self,
    ) -> Vec<PregelExecutableTask<'a, StateT, UpdateT, ContextT>> {
        Vec::new()
    }

    pub(crate) fn prepare_task(
        &mut self,
        name: String,
        input: StateT,
        bound: &'a PregelNodeBound<StateT, UpdateT, ContextT>,
        writers: &'a [ChannelWriter<StateT, ContextT>],
        triggers: &'a [String],
    ) -> PregelExecutableTask<'a, StateT, UpdateT, ContextT> {
        let _ = (name, input, bound, writers, triggers);
        todo!("prepare_task runtime logic is not implemented yet")
    }

    pub(crate) fn execute_task(
        &mut self,
        task: PregelExecutableTask<'a, StateT, UpdateT, ContextT>,
        context: &mut RuntimeContext<ContextT>,
    ) -> Result<PregelExecutableTask<'a, StateT, UpdateT, ContextT>, GraphError>
    where
        UpdateT: Into<StateValue>,
    {
        let _ = (task, context);
        todo!("execute_task runtime logic is not implemented yet")
    }
}
