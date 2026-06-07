#[derive(Debug, thiserror::Error)]
pub(crate) enum GraphError {
    #[error("channel is empty")]
    EmptyChannel,

    #[error("invalid branch target `{0}`")]
    InvalidBranchTarget(String),
}
