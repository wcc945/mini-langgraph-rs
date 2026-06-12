use std::collections::HashMap;

use crate::error::GraphError;

use super::{Checkpoint, CheckpointConfig, CheckpointMetadata, CheckpointTuple, PendingWrite};

/// Trait for checkpoint storage backends.
///
/// Only synchronous methods are provided because the in-memory
/// implementation has no IO. An async variant can be added later
/// when durable backends (sqlite, postgres) are introduced.
pub trait CheckpointSaver {
    /// Retrieve the checkpoint tuple for the given config.
    ///
    /// If config.checkpoint_id is None, returns the latest
    /// checkpoint for the thread and namespace.
    fn get_tuple(&self, config: &CheckpointConfig) -> Result<Option<CheckpointTuple>, GraphError>;

    /// Store a checkpoint and return the updated config.
    fn put(
        &mut self,
        config: &CheckpointConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: HashMap<String, u64>,
    ) -> Result<CheckpointConfig, GraphError>;

    /// Store pending writes for a checkpoint.
    ///
    /// Existing writes for the same `task_id` are replaced (overwrite
    /// semantics), matching the Python source project behavior.
    fn put_writes(
        &mut self,
        config: &CheckpointConfig,
        writes: Vec<PendingWrite>,
        task_id: &str,
    ) -> Result<(), GraphError>;

    /// Delete all checkpoints and writes for a thread.
    fn delete_thread(&mut self, thread_id: &str) -> Result<(), GraphError>;

    /// Compute the next version for a channel.
    fn get_next_version(current: Option<u64>) -> u64 {
        current.map_or(1, |v| v + 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::memory::MemorySaver;

    #[test]
    fn get_next_version_returns_1_for_none() {
        assert_eq!(MemorySaver::get_next_version(None), 1);
    }

    #[test]
    fn get_next_version_increments_existing() {
        assert_eq!(MemorySaver::get_next_version(Some(5)), 6);
        assert_eq!(MemorySaver::get_next_version(Some(1)), 2);
    }
}
