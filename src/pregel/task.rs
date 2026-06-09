use crate::channel::StateValue;
use crate::graph::node::NodeOutput;

pub(crate) type TaskId = String;
pub(crate) type TaskPath = Vec<String>;
pub(crate) type TaskWrite = (String, StateValue);

pub(crate) struct PregelTask<StateT> {
    pub(crate) id: TaskId,
    pub(crate) name: String,
    pub(crate) path: TaskPath,
    pub(crate) input: StateT,
    pub(crate) triggers: Vec<String>,
}

pub(crate) struct PregelTaskWrites {
    pub(crate) name: String,
    pub(crate) path: TaskPath,
    pub(crate) writes: Vec<TaskWrite>,
    pub(crate) triggers: Vec<String>,
}

pub(crate) struct PregelTaskResult<UpdateT> {
    pub(crate) task_id: TaskId,
    pub(crate) name: String,
    pub(crate) output: NodeOutput<UpdateT>,
    pub(crate) writes: Vec<TaskWrite>,
}

pub(crate) struct PregelStep<StateT> {
    pub(crate) step: usize,
    pub(crate) tasks: Vec<PregelTask<StateT>>,
}

impl<StateT> PregelTask<StateT> {
    pub(crate) fn new(
        _id: TaskId,
        _name: String,
        _path: TaskPath,
        _input: StateT,
        _triggers: Vec<String>,
    ) -> Self {
        todo!()
    }

    pub(crate) fn writes(self, _writes: Vec<TaskWrite>) -> PregelTaskWrites {
        todo!()
    }
}

impl PregelTaskWrites {
    pub(crate) fn new(
        _name: String,
        _path: TaskPath,
        _writes: Vec<TaskWrite>,
        _triggers: Vec<String>,
    ) -> Self {
        todo!()
    }

    pub(crate) fn input(_writes: Vec<TaskWrite>) -> Self {
        todo!()
    }

    pub(crate) fn is_triggered(&self) -> bool {
        todo!()
    }
}

impl<UpdateT> PregelTaskResult<UpdateT> {
    pub(crate) fn new(
        _task_id: TaskId,
        _name: String,
        _output: NodeOutput<UpdateT>,
        _writes: Vec<TaskWrite>,
    ) -> Self {
        todo!()
    }

    pub(crate) fn into_writes(self, _path: TaskPath, _triggers: Vec<String>) -> PregelTaskWrites {
        todo!()
    }
}

impl<StateT> PregelStep<StateT> {
    pub(crate) fn new(_step: usize, _tasks: Vec<PregelTask<StateT>>) -> Self {
        todo!()
    }

    pub(crate) fn is_empty(&self) -> bool {
        todo!()
    }

    pub(crate) fn len(&self) -> usize {
        todo!()
    }
}
