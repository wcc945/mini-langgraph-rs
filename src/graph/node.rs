use crate::error::GraphError;
use crate::runtime::RuntimeContext;

struct Command<UpdateT> {
    temp: UpdateT,
}
enum NodeOutput<UpdateT> {
    Update(UpdateT),
    Command(Command<UpdateT>),
    Commands(Vec<Command<UpdateT>>),
    None,
}

type NodeFn<NodeInputT, UpdateT, ContextT> = Box<
    dyn Fn(&NodeInputT, &mut RuntimeContext<ContextT>) -> Result<NodeOutput<UpdateT>, GraphError>
        + Send
        + Sync
        + 'static,
>;

pub struct StateNodeSpec<NodeInputT, UpdateT, ContextT> {
    pub runnable: NodeFn<NodeInputT, UpdateT, ContextT>,
}
