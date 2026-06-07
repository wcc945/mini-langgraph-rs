use std::collections::HashMap;

use crate::error::GraphError;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum StateValue {
    Null,
    Bool(bool),
    Number(f64),
    String(String),
    List(Vec<StateValue>),
    Object(HashMap<String, StateValue>),
}

pub(crate) type DynChannel =
    dyn BaseChannel<Value = StateValue, Update = StateValue, Checkpoint = StateValue>;

pub(crate) trait BaseChannel {
    type Value;
    type Update;
    type Checkpoint;

    fn value_type(&self) -> &'static str;

    fn update_type(&self) -> &'static str;

    fn copy(&self) -> Result<Self, GraphError>
    where
        Self: Sized,
    {
        Self::from_checkpoint(self.checkpoint()?)
    }

    fn checkpoint(&self) -> Result<Option<Self::Checkpoint>, GraphError>;

    fn from_checkpoint(checkpoint: Option<Self::Checkpoint>) -> Result<Self, GraphError>
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
