/// Run-scoped dependencies exposed to graph nodes.
///
/// This context is treated as a read-only view by the runtime. Mutable graph
/// state should be returned as node updates and applied through channels.
pub struct RuntimeContext<ContextT> {
    pub context: ContextT,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_context_stores_user_context() {
        let context = RuntimeContext { context: 42 };

        assert_eq!(context.context, 42);
    }

    #[test]
    fn runtime_context_is_read_through_shared_reference() {
        fn read(context: &RuntimeContext<i32>) -> i32 {
            context.context
        }

        let context = RuntimeContext { context: 7 };

        assert_eq!(read(&context), 7);
    }
}
