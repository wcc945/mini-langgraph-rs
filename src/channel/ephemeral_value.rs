use crate::channel::{BaseChannel, StateValue};
use crate::error::GraphError;

pub(crate) struct EphemeralValue {
    value: Option<StateValue>,
    guard: bool,
}

impl EphemeralValue {
    pub(crate) fn new(guard: bool) -> Self {
        Self { value: None, guard }
    }
}

impl BaseChannel for EphemeralValue {
    type Value = StateValue;
    type Update = StateValue;
    type Checkpoint = StateValue;

    fn value_type(&self) -> &'static str {
        "StateValue"
    }

    fn update_type(&self) -> &'static str {
        "StateValue"
    }

    fn checkpoint(&self) -> Result<Option<Self::Checkpoint>, GraphError> {
        Ok(self.value.clone())
    }

    fn from_checkpoint(&self, checkpoint: Option<Self::Checkpoint>) -> Result<Self, GraphError> {
        Ok(Self {
            value: checkpoint,
            guard: self.guard,
        })
    }

    fn copy_box(&self) -> Result<Box<crate::channel::DynChannel>, GraphError> {
        Ok(Box::new(self.copy()?))
    }

    fn get(&self) -> Result<Self::Value, GraphError> {
        self.value.clone().ok_or(GraphError::EmptyChannel)
    }

    fn is_available(&self) -> bool {
        self.value.is_some()
    }

    fn update(&mut self, values: Vec<Self::Update>) -> Result<bool, GraphError> {
        if values.is_empty() {
            let changed = self.value.is_some();
            self.value = None;
            return Ok(changed);
        }

        if self.guard && values.len() != 1 {
            return Err(GraphError::MultipleUpdatesWithoutReducer {
                count: values.len(),
            });
        }

        self.value = values.into_iter().last();
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn update_makes_value_available_until_empty_update_clears_it() {
        let mut channel = EphemeralValue::new(true);

        assert!(
            channel
                .update(vec![StateValue::String("tick".to_string())])
                .unwrap()
        );
        assert_eq!(
            channel.get().unwrap(),
            StateValue::String("tick".to_string())
        );
        assert!(channel.update(vec![]).unwrap());
        assert!(matches!(channel.get(), Err(GraphError::EmptyChannel)));
    }

    #[test]
    fn empty_update_without_value_reports_no_change() {
        let mut channel = EphemeralValue::new(true);

        assert!(!channel.update(vec![]).unwrap());
    }

    #[test]
    fn guarded_channel_rejects_multiple_updates() {
        let mut channel = EphemeralValue::new(true);

        let error = channel
            .update(vec![StateValue::Number(1.0), StateValue::Number(2.0)])
            .unwrap_err();

        assert!(matches!(
            error,
            GraphError::MultipleUpdatesWithoutReducer { count: 2 }
        ));
    }

    #[test]
    fn unguarded_channel_keeps_last_update() {
        let mut channel = EphemeralValue::new(false);

        channel
            .update(vec![StateValue::Number(1.0), StateValue::Number(2.0)])
            .unwrap();

        assert_eq!(channel.get().unwrap(), StateValue::Number(2.0));
    }

    #[test]
    fn copy_preserves_guard_and_value() {
        let mut channel = EphemeralValue::new(false);
        channel.update(vec![StateValue::Bool(true)]).unwrap();

        let mut copied = channel.copy().unwrap();
        copied
            .update(vec![StateValue::Bool(false), StateValue::Bool(true)])
            .unwrap();

        assert_eq!(copied.get().unwrap(), StateValue::Bool(true));
    }
}
