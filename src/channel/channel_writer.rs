use std::collections::HashMap;

use crate::channel::StateValue;
use crate::error::GraphError;

pub(crate) struct ChannelWriter {
    entries: Vec<ChannelWriterEntry>,
}

pub(crate) enum ChannelWriterEntry {
    Channel(ChannelWriteEntry),
    Tuple(ChannelWriteTupleEntry),
}

pub(crate) struct ChannelWriteEntry {
    pub channel: String,
    pub value: ChannelWriteValue,
    pub skip_none: bool,
    pub mapper: Option<ChannelMapper>,
}

pub(crate) struct ChannelWriteTupleEntry {
    pub value: ChannelWriteValue,
    pub mapper: ChannelTupleMapper,
}

pub(crate) enum ChannelWriteValue {
    Value(StateValue),
    Passthrough,
    SkipWrite,
}

pub(crate) type ChannelMapper =
    Box<dyn Fn(StateValue) -> Result<ChannelWriteValue, GraphError> + Send + Sync>;

pub(crate) type ChannelTupleMapper =
    Box<dyn Fn(StateValue) -> Result<Vec<(String, StateValue)>, GraphError> + Send + Sync>;

impl ChannelWriter {
    pub(crate) fn new(entries: Vec<ChannelWriterEntry>) -> Self {
        Self { entries }
    }

    pub(crate) fn state_value(value: impl Into<StateValue>) -> StateValue {
        value.into()
    }

    pub(crate) fn assemble(
        &self,
        output: StateValue,
        allow_passthrough: bool,
    ) -> Result<Vec<(String, StateValue)>, GraphError> {
        let mut writes = Vec::new();

        for entry in &self.entries {
            match entry {
                ChannelWriterEntry::Channel(entry) => {
                    if let Some(write) =
                        Self::assemble_channel_entry(entry, &output, allow_passthrough)?
                    {
                        writes.push(write);
                    }
                }
                ChannelWriterEntry::Tuple(entry) => {
                    writes.extend(Self::assemble_tuple_entry(
                        entry,
                        &output,
                        allow_passthrough,
                    )?);
                }
            }
        }

        Ok(writes)
    }

    fn assemble_channel_entry(
        entry: &ChannelWriteEntry,
        output: &StateValue,
        allow_passthrough: bool,
    ) -> Result<Option<(String, StateValue)>, GraphError> {
        let Some(value) = Self::entry_value(&entry.value, output, allow_passthrough)? else {
            return Ok(None);
        };

        let value = match &entry.mapper {
            Some(mapper) => mapper(value)?,
            None => ChannelWriteValue::Value(value),
        };

        let ChannelWriteValue::Value(value) = value else {
            return Ok(None);
        };

        if entry.skip_none && value == StateValue::Null {
            return Ok(None);
        }

        Ok(Some((entry.channel.clone(), value)))
    }

    fn assemble_tuple_entry(
        entry: &ChannelWriteTupleEntry,
        output: &StateValue,
        allow_passthrough: bool,
    ) -> Result<Vec<(String, StateValue)>, GraphError> {
        let Some(value) = Self::entry_value(&entry.value, output, allow_passthrough)? else {
            return Ok(Vec::new());
        };

        (entry.mapper)(value)
    }

    fn entry_value(
        value: &ChannelWriteValue,
        output: &StateValue,
        allow_passthrough: bool,
    ) -> Result<Option<StateValue>, GraphError> {
        match value {
            ChannelWriteValue::Value(value) => Ok(Some(value.clone())),
            ChannelWriteValue::Passthrough if allow_passthrough => Ok(Some(output.clone())),
            ChannelWriteValue::Passthrough => Err(GraphError::PassthroughNotAllowed),
            ChannelWriteValue::SkipWrite => Ok(None),
        }
    }
}

impl From<bool> for StateValue {
    fn from(value: bool) -> Self {
        StateValue::Bool(value)
    }
}

impl From<f64> for StateValue {
    fn from(value: f64) -> Self {
        StateValue::Number(value)
    }
}

impl From<i64> for StateValue {
    fn from(value: i64) -> Self {
        StateValue::Number(value as f64)
    }
}

impl From<u64> for StateValue {
    fn from(value: u64) -> Self {
        StateValue::Number(value as f64)
    }
}

impl From<String> for StateValue {
    fn from(value: String) -> Self {
        StateValue::String(value)
    }
}

impl From<&str> for StateValue {
    fn from(value: &str) -> Self {
        StateValue::String(value.to_string())
    }
}

impl<T> From<Vec<T>> for StateValue
where
    T: Into<StateValue>,
{
    fn from(values: Vec<T>) -> Self {
        StateValue::List(values.into_iter().map(Into::into).collect())
    }
}

impl<T> From<HashMap<String, T>> for StateValue
where
    T: Into<StateValue>,
{
    fn from(values: HashMap<String, T>) -> Self {
        StateValue::Object(
            values
                .into_iter()
                .map(|(key, value)| (key, value.into()))
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(channel: &str, value: ChannelWriteValue) -> ChannelWriterEntry {
        ChannelWriterEntry::Channel(ChannelWriteEntry {
            channel: channel.to_string(),
            value,
            skip_none: false,
            mapper: None,
        })
    }

    #[test]
    fn state_value_converts_common_rust_values() {
        assert_eq!(ChannelWriter::state_value(true), StateValue::Bool(true));
        assert_eq!(ChannelWriter::state_value(2_i64), StateValue::Number(2.0));
        assert_eq!(ChannelWriter::state_value(3_u64), StateValue::Number(3.0));
        assert_eq!(ChannelWriter::state_value(4.5_f64), StateValue::Number(4.5));
        assert_eq!(
            ChannelWriter::state_value("hello"),
            StateValue::String("hello".to_string())
        );
        assert_eq!(
            ChannelWriter::state_value(vec![1_i64, 2_i64]),
            StateValue::List(vec![StateValue::Number(1.0), StateValue::Number(2.0)])
        );
        assert_eq!(
            ChannelWriter::state_value(HashMap::from([("count".to_string(), 1_i64)])),
            StateValue::Object(HashMap::from([(
                "count".to_string(),
                StateValue::Number(1.0)
            )]))
        );
    }

    #[test]
    fn assemble_writes_fixed_value_to_channel() {
        let writer = ChannelWriter::new(vec![entry(
            "answer",
            ChannelWriteValue::Value(StateValue::Number(42.0)),
        )]);

        let writes = writer.assemble(StateValue::Null, false).unwrap();

        assert_eq!(
            writes,
            vec![("answer".to_string(), StateValue::Number(42.0))]
        );
    }

    #[test]
    fn assemble_uses_passthrough_output() {
        let writer = ChannelWriter::new(vec![entry("output", ChannelWriteValue::Passthrough)]);

        let writes = writer
            .assemble(StateValue::String("hello".to_string()), true)
            .unwrap();

        assert_eq!(
            writes,
            vec![(
                "output".to_string(),
                StateValue::String("hello".to_string())
            )]
        );
    }

    #[test]
    fn assemble_rejects_passthrough_when_not_allowed() {
        let writer = ChannelWriter::new(vec![entry("input", ChannelWriteValue::Passthrough)]);

        let error = writer.assemble(StateValue::Null, false).unwrap_err();

        assert!(matches!(error, GraphError::PassthroughNotAllowed));
    }

    #[test]
    fn assemble_mapper_can_transform_value() {
        let writer = ChannelWriter::new(vec![ChannelWriterEntry::Channel(ChannelWriteEntry {
            channel: "mapped".to_string(),
            value: ChannelWriteValue::Value(StateValue::Number(1.0)),
            skip_none: false,
            mapper: Some(Box::new(|value| match value {
                StateValue::Number(value) => {
                    Ok(ChannelWriteValue::Value(StateValue::Number(value + 1.0)))
                }
                other => Err(GraphError::InvalidChannelUpdate(format!(
                    "expected number, got {other:?}"
                ))),
            })),
        })]);

        let writes = writer.assemble(StateValue::Null, false).unwrap();

        assert_eq!(
            writes,
            vec![("mapped".to_string(), StateValue::Number(2.0))]
        );
    }

    #[test]
    fn assemble_mapper_can_skip_write() {
        let writer = ChannelWriter::new(vec![ChannelWriterEntry::Channel(ChannelWriteEntry {
            channel: "mapped".to_string(),
            value: ChannelWriteValue::Value(StateValue::Number(1.0)),
            skip_none: false,
            mapper: Some(Box::new(|_| Ok(ChannelWriteValue::SkipWrite))),
        })]);

        let writes = writer.assemble(StateValue::Null, false).unwrap();

        assert!(writes.is_empty());
    }

    #[test]
    fn assemble_skip_none_drops_null_value() {
        let writer = ChannelWriter::new(vec![ChannelWriterEntry::Channel(ChannelWriteEntry {
            channel: "optional".to_string(),
            value: ChannelWriteValue::Value(StateValue::Null),
            skip_none: true,
            mapper: None,
        })]);

        let writes = writer.assemble(StateValue::Null, false).unwrap();

        assert!(writes.is_empty());
    }

    #[test]
    fn assemble_tuple_entry_expands_value_to_channel_writes() {
        let writer = ChannelWriter::new(vec![ChannelWriterEntry::Tuple(ChannelWriteTupleEntry {
            value: ChannelWriteValue::Passthrough,
            mapper: Box::new(|value| match value {
                StateValue::Object(values) => Ok(values.into_iter().collect()),
                other => Err(GraphError::InvalidChannelUpdate(format!(
                    "expected object, got {other:?}"
                ))),
            }),
        })]);
        let output = StateValue::Object(std::collections::HashMap::from([
            ("left".to_string(), StateValue::Number(1.0)),
            ("right".to_string(), StateValue::Number(2.0)),
        ]));

        let mut writes = writer.assemble(output, true).unwrap();
        writes.sort_by(|left, right| left.0.cmp(&right.0));

        assert_eq!(
            writes,
            vec![
                ("left".to_string(), StateValue::Number(1.0)),
                ("right".to_string(), StateValue::Number(2.0)),
            ]
        );
    }
}
