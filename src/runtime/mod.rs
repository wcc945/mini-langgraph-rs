use crate::checkpoint::MemorySaver;
use crate::pregel::pregel::StreamMode;

/// Run-scoped dependencies exposed to graph nodes.
///
/// This context is treated as a read-only view by the runtime. Mutable graph
/// state should be returned as node updates and applied through channels.
pub struct RuntimeContext<ContextT> {
    pub context: ContextT,
    pub stream_mode: Option<StreamMode>,
    pub checkpointer: Option<MemorySaver>,
}

impl<ContextT> RuntimeContext<ContextT> {
    pub fn new(context: ContextT) -> Self {
        Self {
            context,
            stream_mode: None,
            checkpointer: None,
        }
    }

    pub fn with_stream_mode(mut self, stream_mode: StreamMode) -> Self {
        self.stream_mode = Some(stream_mode);
        self
    }

    pub fn with_checkpointer(mut self, checkpointer: MemorySaver) -> Self {
        self.checkpointer = Some(checkpointer);
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

    #[test]
    fn runtime_context_accepts_checkpointer() {
        let context = RuntimeContext::new(()).with_checkpointer(MemorySaver::new());

        assert!(context.checkpointer.is_some());
        assert!(context.stream_mode.is_none());
    }
    #[test]
    fn default_context_has_no_checkpointer() {
        let context = RuntimeContext::<()>::default();
        assert!(context.checkpointer.is_none());
    }

    #[test]
    fn with_checkpointer_and_stream_mode_chaining() {
        let context = RuntimeContext::new(42)
            .with_stream_mode(StreamMode::Updates)
            .with_checkpointer(MemorySaver::new());

        assert_eq!(context.context, 42);
        assert_eq!(context.stream_mode, Some(StreamMode::Updates));
        assert!(context.checkpointer.is_some());
    }

    #[test]
    fn new_context_has_neither_checkpointer_nor_stream_mode() {
        let context = RuntimeContext::new("data");

        assert_eq!(context.context, "data");
        assert!(context.checkpointer.is_none());
        assert!(context.stream_mode.is_none());
    }
}
