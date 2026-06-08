use crate::channel::{BaseChannel, StateValue};
use crate::error::GraphError;

pub(crate) struct LastValue {
    value: Option<StateValue>,
}

impl LastValue {
    pub(crate) fn new() -> Self {
        Self { value: None }
    }
}

impl BaseChannel for LastValue {
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
        Ok(Self { value: checkpoint })
    }

    fn get(&self) -> Result<Self::Value, GraphError> {
        self.value.clone().ok_or(GraphError::EmptyChannel)
    }

    fn is_available(&self) -> bool {
        self.value.is_some()
    }

    fn update(&mut self, values: Vec<Self::Update>) -> Result<bool, GraphError> {
        match values.len() {
            0 => Ok(false),
            1 => {
                self.value = values.into_iter().next();
                Ok(true)
            }
            count => Err(GraphError::MultipleUpdatesWithoutReducer { count }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_rejects_empty_channel() {
        let channel = LastValue::new();

        assert!(matches!(channel.get(), Err(GraphError::EmptyChannel)));
        assert!(!channel.is_available());
    }

    #[test]
    fn empty_update_does_not_change_channel() {
        let mut channel = LastValue::new();

        assert!(!channel.update(vec![]).unwrap());
        assert!(matches!(channel.get(), Err(GraphError::EmptyChannel)));
    }

    #[test]
    fn single_update_sets_value_and_can_be_copied() {
        let mut channel = LastValue::new();

        assert!(channel.update(vec![StateValue::Number(1.0)]).unwrap());
        let copied = channel.copy().unwrap();

        assert_eq!(channel.get().unwrap(), StateValue::Number(1.0));
        assert_eq!(copied.get().unwrap(), StateValue::Number(1.0));
    }

    #[test]
    fn multiple_updates_without_reducer_are_rejected() {
        let mut channel = LastValue::new();

        let error = channel
            .update(vec![StateValue::Number(1.0), StateValue::Number(2.0)])
            .unwrap_err();

        assert!(matches!(
            error,
            GraphError::MultipleUpdatesWithoutReducer { count: 2 }
        ));
    }
}
