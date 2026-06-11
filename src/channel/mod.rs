use std::collections::HashMap;

use crate::error::GraphError;

#[derive(Debug, Clone, PartialEq)]
pub enum StateValue {
    Null,
    Bool(bool),
    Number(f64),
    String(String),
    List(Vec<StateValue>),
    Object(HashMap<String, StateValue>),
}

pub(crate) type DynChannel =
    dyn BaseChannel<Value = StateValue, Update = StateValue, Checkpoint = StateValue>;

pub(crate) mod binop;
pub(crate) mod channel_writer;
pub(crate) mod ephemeral_value;
pub(crate) mod last_value;
pub(crate) mod named_barrier_value;

pub(crate) trait BaseChannel: Send + Sync {
    type Value;
    type Update;
    type Checkpoint;

    fn value_type(&self) -> &'static str;

    fn update_type(&self) -> &'static str;

    fn copy(&self) -> Result<Self, GraphError>
    where
        Self: Sized,
    {
        self.from_checkpoint(self.checkpoint()?)
    }

    fn copy_box(&self) -> Result<Box<DynChannel>, GraphError>;

    fn checkpoint(&self) -> Result<Option<Self::Checkpoint>, GraphError>;

    fn from_checkpoint(&self, checkpoint: Option<Self::Checkpoint>) -> Result<Self, GraphError>
    where
        Self: Sized;

    fn get(&self) -> Result<Self::Value, GraphError>;

    fn is_available(&self) -> bool {
        self.get().is_ok()
    }

    fn update(&mut self, values: Vec<Self::Update>) -> Result<bool, GraphError>;

    fn consume(&mut self) -> Result<bool, GraphError> {
        Ok(false)
    }

    fn finish(&mut self) -> Result<bool, GraphError> {
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone)]
    struct LastValueChannel {
        value: Option<StateValue>,
    }

    impl BaseChannel for LastValueChannel {
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

        fn from_checkpoint(
            &self,
            checkpoint: Option<Self::Checkpoint>,
        ) -> Result<Self, GraphError> {
            Ok(Self { value: checkpoint })
        }

        fn copy_box(&self) -> Result<Box<DynChannel>, GraphError> {
            Ok(Box::new(self.copy()?))
        }

        fn get(&self) -> Result<Self::Value, GraphError> {
            self.value.clone().ok_or(GraphError::EmptyChannel)
        }

        fn update(&mut self, values: Vec<Self::Update>) -> Result<bool, GraphError> {
            let Some(value) = values.into_iter().last() else {
                return Ok(false);
            };

            self.value = Some(value);
            Ok(true)
        }
    }

    #[test]
    fn channel_reports_availability_from_get_result() {
        let empty = LastValueChannel { value: None };
        let filled = LastValueChannel {
            value: Some(StateValue::Number(1.0)),
        };

        assert!(!empty.is_available());
        assert!(filled.is_available());
    }

    #[test]
    fn copy_rebuilds_channel_from_checkpoint() {
        let channel = LastValueChannel {
            value: Some(StateValue::String("saved".to_string())),
        };

        let copied = channel.copy().unwrap();

        assert_eq!(
            copied.get().unwrap(),
            StateValue::String("saved".to_string())
        );
    }

    #[test]
    fn update_uses_last_value_and_reports_change() {
        let mut channel = LastValueChannel { value: None };

        let changed = channel
            .update(vec![StateValue::Number(1.0), StateValue::Number(2.0)])
            .unwrap();

        assert!(changed);
        assert_eq!(channel.get().unwrap(), StateValue::Number(2.0));
    }

    #[test]
    fn default_consume_and_finish_do_not_change_channel() {
        let mut channel = LastValueChannel {
            value: Some(StateValue::Bool(true)),
        };

        assert!(!channel.consume().unwrap());
        assert!(!channel.finish().unwrap());
        assert_eq!(channel.get().unwrap(), StateValue::Bool(true));
    }

    #[test]
    fn state_value_supports_nested_object_equality() {
        let value = StateValue::Object(HashMap::from([(
            "items".to_string(),
            StateValue::List(vec![StateValue::Null]),
        )]));

        assert_eq!(value.clone(), value);
    }
}
