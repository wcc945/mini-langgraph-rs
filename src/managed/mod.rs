pub(crate) trait ManagedValueSpec: Send + Sync {}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestManagedValue;

    impl ManagedValueSpec for TestManagedValue {}

    fn assert_managed_value_spec<T: ManagedValueSpec>() {}

    #[test]
    fn managed_value_spec_accepts_send_sync_marker_implementor() {
        assert_managed_value_spec::<TestManagedValue>();
    }
}
