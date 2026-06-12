use std::sync::Arc;

use crate::channel::{BaseChannel, StateValue};
use crate::error::GraphError;

type StateValueReducer =
    dyn Fn(StateValue, StateValue) -> Result<StateValue, GraphError> + Send + Sync + 'static;

pub struct BinaryOperatorAggregate {
    value: Option<StateValue>,
    reducer: Arc<StateValueReducer>,
}

impl BinaryOperatorAggregate {
    pub fn new(
        reducer: impl Fn(StateValue, StateValue) -> Result<StateValue, GraphError>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self {
            value: None,
            reducer: Arc::new(reducer),
        }
    }
}

impl BaseChannel for BinaryOperatorAggregate {
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
            reducer: Arc::clone(&self.reducer),
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
            return Ok(false);
        }

        let mut values = values.into_iter();
        if self.value.is_none() {
            self.value = values.next();
        }

        for value in values {
            let current = self.value.take().ok_or_else(|| {
                GraphError::InvalidChannelUpdate(
                    "BinaryOperatorAggregate lost its current value".to_string(),
                )
            })?;
            self.value = Some((self.reducer)(current, value)?);
        }

        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn add_numbers(left: StateValue, right: StateValue) -> Result<StateValue, GraphError> {
        match (left, right) {
            (StateValue::Number(left), StateValue::Number(right)) => {
                Ok(StateValue::Number(left + right))
            }
            (left, right) => Err(GraphError::InvalidChannelUpdate(format!(
                "expected numbers, got {left:?} and {right:?}"
            ))),
        }
    }

    #[test]
    fn reducer_combines_multiple_updates_in_order() {
        let mut channel = BinaryOperatorAggregate::new(add_numbers);

        assert!(
            channel
                .update(vec![
                    StateValue::Number(1.0),
                    StateValue::Number(2.0),
                    StateValue::Number(3.0),
                ])
                .unwrap()
        );

        assert_eq!(channel.get().unwrap(), StateValue::Number(6.0));
    }

    #[test]
    fn empty_update_reports_no_change() {
        let mut channel = BinaryOperatorAggregate::new(add_numbers);

        assert!(!channel.update(vec![]).unwrap());
        assert!(matches!(channel.get(), Err(GraphError::EmptyChannel)));
    }

    #[test]
    fn later_updates_reduce_against_existing_value() {
        let mut channel = BinaryOperatorAggregate::new(add_numbers);

        channel.update(vec![StateValue::Number(1.0)]).unwrap();
        channel.update(vec![StateValue::Number(2.0)]).unwrap();

        assert_eq!(channel.get().unwrap(), StateValue::Number(3.0));
    }

    #[test]
    fn copy_preserves_checkpoint_and_reducer() {
        let mut channel = BinaryOperatorAggregate::new(add_numbers);
        channel.update(vec![StateValue::Number(1.0)]).unwrap();

        let mut copied = channel.copy().unwrap();
        copied.update(vec![StateValue::Number(4.0)]).unwrap();

        assert_eq!(copied.get().unwrap(), StateValue::Number(5.0));
    }

    #[test]
    fn reducer_errors_are_propagated() {
        let mut channel = BinaryOperatorAggregate::new(add_numbers);

        let error = channel
            .update(vec![
                StateValue::Number(1.0),
                StateValue::String("bad".to_string()),
            ])
            .unwrap_err();

        assert!(matches!(error, GraphError::InvalidChannelUpdate(_)));
    }
}
