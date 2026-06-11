#[derive(Debug, thiserror::Error)]
pub enum GraphError {
    #[error("channel is empty")]
    EmptyChannel,

    #[error("can receive only one value per step, got {count}")]
    MultipleUpdatesWithoutReducer { count: usize },

    #[error("invalid channel update: {0}")]
    InvalidChannelUpdate(String),

    #[error("passthrough write is not allowed")]
    PassthroughNotAllowed,

    #[error("invalid barrier value: {0}")]
    InvalidBarrierValue(String),

    #[error("invalid branch target `{0}`")]
    InvalidBranchTarget(String),

    #[error("node `{0}` already exists")]
    DuplicateNode(String),

    #[error("node name `{0}` is reserved")]
    ReservedNodeName(String),

    #[error("node name `{node}` contains reserved character `{character}`")]
    ReservedNodeCharacter { node: String, character: String },

    #[error("edge starts with empty vector")]
    EmptyEdgeStarts,

    #[error("sequence requires at least one node")]
    EmptySequence,

    #[error("START cannot be an end node")]
    StartCannotBeEnd,

    #[error("END cannot be a start node")]
    EndCannotBeStart,

    #[error("START cannot be used in a waiting edge")]
    StartCannotBeWaitingEdgeStart,

    #[error("node `{0}` does not exist")]
    UnknownNode(String),

    #[error("branch `{branch}` already exists for node `{node}`")]
    DuplicateBranch { node: String, branch: String },

    #[error("graph must have an entrypoint: add at least one edge from START to another node")]
    MissingEntrypoint,

    #[error("found edge starting at unknown node `{0}`")]
    UnknownEdgeSource(String),

    #[error("found edge ending at unknown node `{0}`")]
    UnknownEdgeTarget(String),

    #[error("at `{node}` node, `{branch}` branch found unknown target `{target}`")]
    UnknownBranchTarget {
        node: String,
        branch: String,
        target: String,
    },

    #[error("Pregel node `{node}` reads unknown channel `{channel}`")]
    UnknownPregelReadChannel { node: String, channel: String },

    #[error("Pregel node `{node}` subscribes to unknown trigger channel `{channel}`")]
    UnknownPregelTriggerChannel { node: String, channel: String },

    #[error("Pregel input channel `{0}` does not exist")]
    UnknownPregelInputChannel(String),

    #[error("no Pregel input channel is subscribed to by any node: {0:?}")]
    PregelInputChannelNotSubscribed(Vec<String>),

    #[error("Pregel output channel `{0}` does not exist")]
    UnknownPregelOutputChannel(String),

    #[error("Pregel stream channel `{0}` does not exist")]
    UnknownPregelStreamChannel(String),

    #[error("Pregel recursion limit must be greater than 0, got {0}")]
    InvalidPregelRecursionLimit(usize),

    #[error("Pregel recursion limit of {0} was reached")]
    PregelRecursionLimitReached(usize),

    #[error("Pregel task `{node}` failed: {message}")]
    PregelTaskFailed { node: String, message: String },

    #[error("Pregel node output command is not supported yet")]
    UnsupportedPregelCommand,

    #[error("Pregel stream failed to copy channel `{channel}`: {message}")]
    PregelChannelCopyFailed { channel: String, message: String },

    #[error("Pregel stream runtime failed: {0}")]
    PregelStreamRuntimeFailed(String),

    #[error("Pregel received no input for input channels: {0:?}")]
    EmptyPregelInput(Vec<String>),

    #[error("invalid Pregel input: {0}")]
    InvalidPregelInput(String),

    #[error("compile does not support conditional branches yet")]
    UnsupportedCompiledBranches,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn graph_error_formats_simple_variants() {
        assert_eq!(
            GraphError::DuplicateNode("a".to_string()).to_string(),
            "node `a` already exists"
        );
        assert_eq!(
            GraphError::MissingEntrypoint.to_string(),
            "graph must have an entrypoint: add at least one edge from START to another node"
        );
    }

    #[test]
    fn graph_error_formats_struct_variants() {
        let error = GraphError::UnknownBranchTarget {
            node: "router".to_string(),
            branch: "route".to_string(),
            target: "missing".to_string(),
        };

        assert_eq!(
            error.to_string(),
            "at `router` node, `route` branch found unknown target `missing`"
        );
    }
}
