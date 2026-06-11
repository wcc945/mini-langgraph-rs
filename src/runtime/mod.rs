use crate::pregel::pregel::StreamMode;

/// Run-scoped dependencies exposed to graph nodes.
///
/// This context is treated as a read-only view by the runtime. Mutable graph
/// state should be returned as node updates and applied through channels.
pub struct RuntimeContext<ContextT> {
    pub context: ContextT,
    pub stream_mode: Option<StreamMode>,
}

impl<ContextT> RuntimeContext<ContextT> {
    pub fn new(context: ContextT) -> Self {
        Self {
            context,
            stream_mode: None,
        }
    }

    pub fn with_stream_mode(mut self, stream_mode: StreamMode) -> Self {
        self.stream_mode = Some(stream_mode);
        self
    }
}

impl<ContextT> Default for RuntimeContext<ContextT>
where
    ContextT: Default,
{
    fn default() -> Self {
        Self::new(ContextT::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_context_stores_user_context() {
        let context = RuntimeContext::new(42);

        assert_eq!(context.context, 42);
    }

    #[test]
    fn runtime_context_is_read_through_shared_reference() {
        fn read(context: &RuntimeContext<i32>) -> i32 {
            context.context
        }

        let context = RuntimeContext::new(7);

        assert_eq!(read(&context), 7);
    }
}
