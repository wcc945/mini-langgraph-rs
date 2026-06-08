use std::collections::HashSet;

use crate::channel::{BaseChannel, StateValue};
use crate::error::GraphError;

pub(crate) struct NamedBarrierValue {
    names: HashSet<String>,
    seen: HashSet<String>,
}

impl NamedBarrierValue {
    pub(crate) fn new(names: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            names: names.into_iter().map(Into::into).collect(),
            seen: HashSet::new(),
        }
    }

    fn checkpoint_from_seen(&self) -> StateValue {
        let mut seen: Vec<_> = self.seen.iter().cloned().collect();
        seen.sort();

        StateValue::List(seen.into_iter().map(StateValue::String).collect())
    }

    fn seen_from_checkpoint(checkpoint: Option<StateValue>) -> Result<HashSet<String>, GraphError> {
        let Some(StateValue::List(values)) = checkpoint else {
            return Ok(HashSet::new());
        };

        values
            .into_iter()
            .map(|value| match value {
                StateValue::String(name) => Ok(name),
                other => Err(GraphError::InvalidChannelUpdate(format!(
                    "barrier checkpoint contains non-string value {other:?}"
                ))),
            })
            .collect()
    }
}

impl BaseChannel for NamedBarrierValue {
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
        Ok(Some(self.checkpoint_from_seen()))
    }

    fn from_checkpoint(&self, checkpoint: Option<Self::Checkpoint>) -> Result<Self, GraphError> {
        let seen = Self::seen_from_checkpoint(checkpoint)?;

        for name in &seen {
            if !self.names.contains(name) {
                return Err(GraphError::InvalidBarrierValue(name.clone()));
            }
        }

        Ok(Self {
            names: self.names.clone(),
            seen,
        })
    }

    fn get(&self) -> Result<Self::Value, GraphError> {
        if self.is_available() {
            Ok(StateValue::Null)
        } else {
            Err(GraphError::EmptyChannel)
        }
    }

    fn is_available(&self) -> bool {
        self.seen == self.names
    }

    fn update(&mut self, values: Vec<Self::Update>) -> Result<bool, GraphError> {
        let mut updated = false;

        for value in values {
            let StateValue::String(name) = value else {
                return Err(GraphError::InvalidBarrierValue(format!("{value:?}")));
            };

            if !self.names.contains(&name) {
                return Err(GraphError::InvalidBarrierValue(name));
            }

            updated |= self.seen.insert(name);
        }

        Ok(updated)
    }

    fn consume(&mut self) -> Result<bool, GraphError> {
        if self.is_available() {
            self.seen.clear();
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn barrier_is_unavailable_until_all_names_are_seen() {
        let mut channel = NamedBarrierValue::new(["a", "b"]);

        assert!(!channel.is_available());
        assert!(matches!(channel.get(), Err(GraphError::EmptyChannel)));

        assert!(
            channel
                .update(vec![StateValue::String("a".to_string())])
                .unwrap()
        );
        assert!(!channel.is_available());

        assert!(
            channel
                .update(vec![StateValue::String("b".to_string())])
                .unwrap()
        );
        assert!(channel.is_available());
        assert_eq!(channel.get().unwrap(), StateValue::Null);
    }

    #[test]
    fn duplicate_seen_name_does_not_report_change() {
        let mut channel = NamedBarrierValue::new(["a"]);

        assert!(
            channel
                .update(vec![StateValue::String("a".to_string())])
                .unwrap()
        );
        assert!(
            !channel
                .update(vec![StateValue::String("a".to_string())])
                .unwrap()
        );
    }

    #[test]
    fn consume_clears_completed_barrier() {
        let mut channel = NamedBarrierValue::new(["a"]);
        channel
            .update(vec![StateValue::String("a".to_string())])
            .unwrap();

        assert!(channel.consume().unwrap());
        assert!(!channel.is_available());
    }

    #[test]
    fn invalid_name_is_rejected() {
        let mut channel = NamedBarrierValue::new(["a"]);

        let error = channel
            .update(vec![StateValue::String("b".to_string())])
            .unwrap_err();

        assert!(matches!(error, GraphError::InvalidBarrierValue(value) if value == "b"));
    }

    #[test]
    fn non_string_update_is_rejected() {
        let mut channel = NamedBarrierValue::new(["a"]);

        let error = channel.update(vec![StateValue::Number(1.0)]).unwrap_err();

        assert!(matches!(error, GraphError::InvalidBarrierValue(_)));
    }

    #[test]
    fn copy_preserves_seen_names() {
        let mut channel = NamedBarrierValue::new(["a", "b"]);
        channel
            .update(vec![StateValue::String("a".to_string())])
            .unwrap();

        let mut copied = channel.copy().unwrap();
        copied
            .update(vec![StateValue::String("b".to_string())])
            .unwrap();

        assert!(copied.is_available());
    }
}
