use crate::error::GraphError;
use crate::runtime::RuntimeContext;

pub struct Command<UpdateT> {
    temp: UpdateT,
}
pub enum NodeOutput<UpdateT> {
    Update(UpdateT),
    Command(Command<UpdateT>),
    Commands(Vec<Command<UpdateT>>),
    None,
}

pub type NodeFn<StateT, UpdateT, ContextT> = Box<
    dyn Fn(&StateT, &RuntimeContext<ContextT>) -> Result<NodeOutput<UpdateT>, GraphError>
        + Send
        + Sync
        + 'static,
>;

pub struct StateNodeSpec<StateT, UpdateT, ContextT> {
    pub runnable: NodeFn<StateT, UpdateT, ContextT>,
}

impl<StateT, UpdateT, ContextT> StateNodeSpec<StateT, UpdateT, ContextT> {
    pub fn new(runnable: NodeFn<StateT, UpdateT, ContextT>) -> Self {
        Self { runnable }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_node_spec_stores_and_runs_node_function() {
        let spec = StateNodeSpec::new(Box::new(|state: &i32, context: &RuntimeContext<i32>| {
            Ok(NodeOutput::Update(*state + context.context))
        }));
        let context = RuntimeContext { context: 2 };

        let output = (spec.runnable)(&5, &context).unwrap();

        assert_eq!(context.context, 2);
        assert!(matches!(output, NodeOutput::Update(7)));
    }
}
