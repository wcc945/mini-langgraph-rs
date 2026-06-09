use crate::channel::StateValue;
use crate::channel::channel_writer::ChannelWriter;
use crate::error::GraphError;
use crate::graph::node::NodeOutput;
use crate::runtime::RuntimeContext;

pub(crate) type PregelNodeMapper<StateT> =
    Box<dyn Fn(StateValue) -> Result<StateT, GraphError> + Send + Sync + 'static>;

pub(crate) type PregelNodeBound<StateT, UpdateT, ContextT> = Box<
    dyn Fn(&StateT, &mut RuntimeContext<ContextT>) -> Result<NodeOutput<UpdateT>, GraphError>
        + Send
        + Sync
        + 'static,
>;

pub(crate) struct PregelNode<StateT, UpdateT, ContextT> {
    pub(crate) channels: Vec<String>,
    pub(crate) triggers: Vec<String>,
    pub(crate) mapper: Option<PregelNodeMapper<StateT>>,
    pub(crate) writers: Vec<ChannelWriter<StateT, ContextT>>,
    pub(crate) bound: PregelNodeBound<StateT, UpdateT, ContextT>,
}

impl<StateT, UpdateT, ContextT> PregelNode<StateT, UpdateT, ContextT> {
    pub(crate) fn new(
        channels: Vec<String>,
        triggers: Vec<String>,
        mapper: Option<PregelNodeMapper<StateT>>,
        writers: Vec<ChannelWriter<StateT, ContextT>>,
        bound: PregelNodeBound<StateT, UpdateT, ContextT>,
    ) -> Self {
        Self {
            channels,
            triggers,
            mapper,
            writers,
            bound,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channel::channel_writer::{
        ChannelWriteEntry, ChannelWriteValue, ChannelWriterEntry,
    };

    fn writer(channel: &str) -> ChannelWriter<i64, i64> {
        ChannelWriter::new(vec![ChannelWriterEntry::Channel(ChannelWriteEntry {
            channel: channel.to_string(),
            value: ChannelWriteValue::Passthrough,
            skip_none: false,
            mapper: None,
        })])
    }

    #[test]
    fn stores_channels_triggers_mapper_writers_and_bound() {
        let node: PregelNode<i64, i64, i64> = PregelNode::new(
            vec!["input".to_string()],
            vec!["trigger".to_string()],
            Some(Box::new(|_| Ok(2))),
            vec![writer("output")],
            Box::new(|input, context| {
                context.context += *input;
                Ok(NodeOutput::Update(input + context.context))
            }),
        );
        let mut context = RuntimeContext { context: 3 };
        let mapped = node.mapper.as_ref().unwrap()(StateValue::Null).unwrap();
        let output = (node.bound)(&mapped, &mut context).unwrap();
        let writes = node.writers[0]
            .assemble(StateValue::Number(1.0), true, &mapped, &mut context)
            .unwrap();

        assert_eq!(node.channels, vec!["input".to_string()]);
        assert_eq!(node.triggers, vec!["trigger".to_string()]);
        assert_eq!(node.writers.len(), 1);
        assert_eq!(
            writes,
            vec![("output".to_string(), StateValue::Number(1.0))]
        );
        assert_eq!(context.context, 5);
        assert!(matches!(output, NodeOutput::Update(7)));
    }

    #[test]
    fn mapper_can_transform_channel_value_before_bound() {
        let node: PregelNode<i64, i64, ()> = PregelNode::new(
            vec!["raw".to_string()],
            vec!["raw".to_string()],
            Some(Box::new(|value| match value {
                StateValue::Number(value) => Ok(value as i64 + 1),
                other => Err(GraphError::InvalidChannelUpdate(format!(
                    "expected number, got {other:?}"
                ))),
            })),
            Vec::new(),
            Box::new(|input, _| Ok(NodeOutput::Update(input * 2))),
        );
        let mut context = RuntimeContext { context: () };

        let mapped = node.mapper.as_ref().unwrap()(StateValue::Number(2.0)).unwrap();
        let output = (node.bound)(&mapped, &mut context).unwrap();

        assert!(matches!(output, NodeOutput::Update(6)));
    }

    #[test]
    fn channels_are_plain_vectors() {
        let node: PregelNode<i64, i64, ()> = PregelNode::new(
            vec!["left".to_string(), "right".to_string()],
            Vec::new(),
            None,
            Vec::new(),
            Box::new(|_, _| Ok(NodeOutput::None)),
        );

        assert_eq!(node.channels, vec!["left".to_string(), "right".to_string()]);
    }
}
