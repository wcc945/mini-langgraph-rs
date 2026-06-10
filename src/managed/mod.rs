pub(crate) trait ManagedValueSpec: Send + Sync {
    fn copy_box(&self) -> Box<dyn ManagedValueSpec>;
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestManagedValue;

    impl ManagedValueSpec for TestManagedValue {
        fn copy_box(&self) -> Box<dyn ManagedValueSpec> {
            Box::new(TestManagedValue)
        }
    }

    fn assert_managed_value_spec<T: ManagedValueSpec>() {}

    #[test]
    fn managed_value_spec_accepts_send_sync_marker_implementor() {
        assert_managed_value_spec::<TestManagedValue>();
    }

    #[test]
    fn managed_value_spec_can_copy_boxed_spec() {
        let spec = TestManagedValue;

        let _copy = spec.copy_box();
    }
}
