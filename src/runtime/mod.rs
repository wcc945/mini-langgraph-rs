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
}
