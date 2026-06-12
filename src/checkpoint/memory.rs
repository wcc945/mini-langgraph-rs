use std::collections::{BTreeMap, HashMap};

use crate::error::GraphError;

use super::{
    Checkpoint, CheckpointConfig, CheckpointMetadata, CheckpointTuple, PendingWrite,
    saver::CheckpointSaver,
};

/// In-memory checkpoint storage.
///
/// Stores checkpoints and pending writes purely in process memory.
/// Suitable for testing and single-run scenarios. For production
/// durability, use a persistent backend instead.
type NamespaceStorage =
    HashMap<String, BTreeMap<String, (Checkpoint, CheckpointMetadata, Option<String>)>>;

type WriteKey = (String, String, String);

#[derive(Clone)]
pub struct MemorySaver {
    /// thread_id -> namespace -> checkpoint_id -> (checkpoint, metadata, parent_id)
    storage: HashMap<String, NamespaceStorage>,
    /// (thread_id, namespace, checkpoint_id) -> pending writes
    writes: HashMap<WriteKey, Vec<PendingWrite>>,
}

impl MemorySaver {
    pub fn new() -> Self {
        Self {
            storage: HashMap::new(),
            writes: HashMap::new(),
        }
    }
}

impl Default for MemorySaver {
    fn default() -> Self {
        Self::new()
    }
}

type StorageEntry = (Checkpoint, CheckpointMetadata, Option<String>);

fn find_entry<'a>(
    storage: &'a HashMap<String, HashMap<String, BTreeMap<String, StorageEntry>>>,
    config: &CheckpointConfig,
) -> Option<(&'a String, &'a StorageEntry)> {
    let ns_map = storage.get(&config.thread_id)?;
    let cp_map = ns_map.get(&config.checkpoint_ns)?;
    match &config.checkpoint_id {
        Some(id) => cp_map.get_key_value(id),
        None => cp_map.last_key_value(),
    }
}

impl CheckpointSaver for MemorySaver {
    fn get_tuple(&self, config: &CheckpointConfig) -> Result<Option<CheckpointTuple>, GraphError> {
        let (checkpoint_id, (checkpoint, metadata, parent_id)) =
            match find_entry(&self.storage, config) {
                Some(pair) => pair,
                None => return Ok(None),
            };

        let key = (
            config.thread_id.clone(),
            config.checkpoint_ns.clone(),
            checkpoint_id.clone(),
        );
        let pending_writes = self.writes.get(&key).cloned().unwrap_or_default();

        Ok(Some(CheckpointTuple {
            config: CheckpointConfig {
                thread_id: config.thread_id.clone(),
                checkpoint_ns: config.checkpoint_ns.clone(),
                checkpoint_id: Some(checkpoint_id.clone()),
            },
            checkpoint: checkpoint.clone(),
            metadata: metadata.clone(),
            parent_config: parent_id.as_ref().map(|pid| CheckpointConfig {
                thread_id: config.thread_id.clone(),
                checkpoint_ns: config.checkpoint_ns.clone(),
                checkpoint_id: Some(pid.clone()),
            }),
            pending_writes,
        }))
    }

    fn put(
        &mut self,
        config: &CheckpointConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        _new_versions: HashMap<String, u64>,
    ) -> Result<CheckpointConfig, GraphError> {
        let parent_id = config.checkpoint_id.clone();
        let checkpoint_id = checkpoint.id.clone();

        self.storage
            .entry(config.thread_id.clone())
            .or_default()
            .entry(config.checkpoint_ns.clone())
            .or_default()
            .insert(checkpoint_id.clone(), (checkpoint, metadata, parent_id));

        Ok(CheckpointConfig {
            thread_id: config.thread_id.clone(),
            checkpoint_ns: config.checkpoint_ns.clone(),
            checkpoint_id: Some(checkpoint_id),
        })
    }

    fn put_writes(
        &mut self,
        config: &CheckpointConfig,
        writes: Vec<PendingWrite>,
        task_id: &str,
    ) -> Result<(), GraphError> {
        let checkpoint_id = config.checkpoint_id.as_ref().ok_or_else(|| {
            GraphError::InvalidPregelInput("checkpoint_id is required for put_writes".to_string())
        })?;

        let key = (
            config.thread_id.clone(),
            config.checkpoint_ns.clone(),
            checkpoint_id.clone(),
        );

        // Overwrite semantics: remove existing writes for the same task_id first.
        let existing = self.writes.entry(key).or_default();
        existing.retain(|w| w.task_id != task_id);
        existing.extend(writes);

        Ok(())
    }

    fn delete_thread(&mut self, thread_id: &str) -> Result<(), GraphError> {
        self.storage.remove(thread_id);

        let keys_to_remove: Vec<_> = self
            .writes
            .keys()
            .filter(|(tid, _, _)| tid == thread_id)
            .cloned()
            .collect();
        for key in keys_to_remove {
            self.writes.remove(&key);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channel::StateValue;
    use crate::checkpoint::CheckpointSource;

    fn config(thread_id: &str, checkpoint_id: Option<&str>) -> CheckpointConfig {
        CheckpointConfig {
            thread_id: thread_id.to_string(),
            checkpoint_ns: String::new(),
            checkpoint_id: checkpoint_id.map(str::to_string),
        }
    }

    fn test_checkpoint(id: &str) -> Checkpoint {
        Checkpoint {
            id: id.to_string(),
            ts: "2025-01-01T00:00:00Z".to_string(),
            channel_values: HashMap::from([(
                "msg".to_string(),
                StateValue::String("hello".to_string()),
            )]),
            channel_versions: HashMap::from([("msg".to_string(), 1)]),
            versions_seen: HashMap::new(),
            updated_channels: Some(vec!["msg".to_string()]),
        }
    }

    fn test_metadata(source: CheckpointSource, step: i64) -> CheckpointMetadata {
        CheckpointMetadata {
            source,
            step,
            parents: HashMap::new(),
        }
    }

    #[test]
    fn put_and_get_tuple_roundtrip() {
        let mut saver = MemorySaver::new();
        let cfg = config("t1", None);
        let cp = test_checkpoint("cp1");
        let meta = test_metadata(CheckpointSource::Input, -1);

        let put_cfg = saver
            .put(&cfg, cp.clone(), meta.clone(), HashMap::new())
            .unwrap();
        assert!(put_cfg.checkpoint_id.is_some());

        let tuple = saver.get_tuple(&put_cfg).unwrap().unwrap();
        assert_eq!(tuple.checkpoint.id, "cp1");
        assert_eq!(tuple.metadata.step, -1);
    }

    #[test]
    fn get_tuple_returns_latest_when_no_checkpoint_id() {
        let mut saver = MemorySaver::new();
        let cfg = config("t1", None);

        let cp1 = test_checkpoint("0000000000000001");
        let meta1 = test_metadata(CheckpointSource::Input, -1);
        saver.put(&cfg, cp1, meta1, HashMap::new()).unwrap();

        let cp2 = test_checkpoint("0000000000000002");
        let meta2 = test_metadata(CheckpointSource::Loop, 0);
        saver.put(&cfg, cp2, meta2, HashMap::new()).unwrap();

        let result = saver.get_tuple(&cfg).unwrap().unwrap();
        assert_eq!(result.checkpoint.id, "0000000000000002");
    }

    #[test]
    fn get_tuple_returns_none_for_unknown_thread() {
        let saver = MemorySaver::new();
        let cfg = config("missing", None);
        assert!(saver.get_tuple(&cfg).unwrap().is_none());
    }

    #[test]
    fn put_writes_overwrites_same_task_id() {
        let mut saver = MemorySaver::new();
        let cfg = config("t1", None);
        let cp = test_checkpoint("cp1");
        let meta = test_metadata(CheckpointSource::Input, -1);
        let put_cfg = saver.put(&cfg, cp, meta, HashMap::new()).unwrap();

        saver
            .put_writes(
                &put_cfg,
                vec![PendingWrite {
                    task_id: "task1".to_string(),
                    channel: "msg".to_string(),
                    value: StateValue::String("old".to_string()),
                }],
                "task1",
            )
            .unwrap();

        saver
            .put_writes(
                &put_cfg,
                vec![PendingWrite {
                    task_id: "task1".to_string(),
                    channel: "msg".to_string(),
                    value: StateValue::String("new".to_string()),
                }],
                "task1",
            )
            .unwrap();

        let tuple = saver.get_tuple(&put_cfg).unwrap().unwrap();
        assert_eq!(tuple.pending_writes.len(), 1);
        assert_eq!(
            tuple.pending_writes[0].value,
            StateValue::String("new".to_string())
        );
    }

    #[test]
    fn put_writes_preserves_different_task_ids() {
        let mut saver = MemorySaver::new();
        let cfg = config("t1", None);
        let cp = test_checkpoint("cp1");
        let meta = test_metadata(CheckpointSource::Input, -1);
        let put_cfg = saver.put(&cfg, cp, meta, HashMap::new()).unwrap();

        saver
            .put_writes(
                &put_cfg,
                vec![PendingWrite {
                    task_id: "task1".to_string(),
                    channel: "msg".to_string(),
                    value: StateValue::String("a".to_string()),
                }],
                "task1",
            )
            .unwrap();

        saver
            .put_writes(
                &put_cfg,
                vec![PendingWrite {
                    task_id: "task2".to_string(),
                    channel: "msg".to_string(),
                    value: StateValue::String("b".to_string()),
                }],
                "task2",
            )
            .unwrap();

        let tuple = saver.get_tuple(&put_cfg).unwrap().unwrap();
        assert_eq!(tuple.pending_writes.len(), 2);
    }

    #[test]
    fn delete_thread_removes_all_data() {
        let mut saver = MemorySaver::new();
        let cfg = config("t1", None);
        let cp = test_checkpoint("cp1");
        let meta = test_metadata(CheckpointSource::Input, -1);
        saver.put(&cfg, cp, meta, HashMap::new()).unwrap();

        saver.delete_thread("t1").unwrap();

        let result = saver.get_tuple(&cfg).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn delete_thread_does_not_affect_other_threads() {
        let mut saver = MemorySaver::new();
        let cfg1 = config("t1", None);
        let cfg2 = config("t2", None);

        saver
            .put(
                &cfg1,
                test_checkpoint("cp1"),
                test_metadata(CheckpointSource::Input, -1),
                HashMap::new(),
            )
            .unwrap();
        saver
            .put(
                &cfg2,
                test_checkpoint("cp2"),
                test_metadata(CheckpointSource::Input, -1),
                HashMap::new(),
            )
            .unwrap();

        saver.delete_thread("t1").unwrap();

        assert!(saver.get_tuple(&cfg1).unwrap().is_none());
        assert!(saver.get_tuple(&cfg2).unwrap().is_some());
    }

    #[test]
    fn get_tuple_with_specific_checkpoint_id() {
        let mut saver = MemorySaver::new();
        let cfg = config("t1", None);

        let cp1 = test_checkpoint("0000000000000001");
        let cp2 = test_checkpoint("0000000000000002");

        saver
            .put(
                &cfg,
                cp1,
                test_metadata(CheckpointSource::Input, -1),
                HashMap::new(),
            )
            .unwrap();
        saver
            .put(
                &cfg,
                cp2,
                test_metadata(CheckpointSource::Loop, 0),
                HashMap::new(),
            )
            .unwrap();

        let specific_cfg = config("t1", Some("0000000000000001"));
        let result = saver.get_tuple(&specific_cfg).unwrap().unwrap();
        assert_eq!(result.checkpoint.id, "0000000000000001");
    }

    #[test]
    fn multiple_namespaces_are_isolated() {
        let mut saver = MemorySaver::new();
        let cfg_ns1 = CheckpointConfig {
            thread_id: "t1".to_string(),
            checkpoint_ns: "ns1".to_string(),
            checkpoint_id: None,
        };
        let cfg_ns2 = CheckpointConfig {
            thread_id: "t1".to_string(),
            checkpoint_ns: "ns2".to_string(),
            checkpoint_id: None,
        };

        saver
            .put(
                &cfg_ns1,
                test_checkpoint("cp1"),
                test_metadata(CheckpointSource::Input, -1),
                HashMap::new(),
            )
            .unwrap();
        saver
            .put(
                &cfg_ns2,
                test_checkpoint("cp2"),
                test_metadata(CheckpointSource::Loop, 0),
                HashMap::new(),
            )
            .unwrap();

        assert_eq!(
            saver.get_tuple(&cfg_ns1).unwrap().unwrap().checkpoint.id,
            "cp1"
        );
        assert_eq!(
            saver.get_tuple(&cfg_ns2).unwrap().unwrap().checkpoint.id,
            "cp2"
        );
    }

    #[test]
    fn delete_thread_also_removes_writes() {
        let mut saver = MemorySaver::new();
        let cfg = config("t1", None);
        saver
            .put(
                &cfg,
                test_checkpoint("cp1"),
                test_metadata(CheckpointSource::Input, -1),
                HashMap::new(),
            )
            .unwrap();
        let put_cfg = saver
            .put(
                &cfg,
                test_checkpoint("cp2"),
                test_metadata(CheckpointSource::Loop, 0),
                HashMap::new(),
            )
            .unwrap();
        saver
            .put_writes(
                &put_cfg,
                vec![PendingWrite {
                    task_id: "task1".to_string(),
                    channel: "msg".to_string(),
                    value: StateValue::String("x".to_string()),
                }],
                "task1",
            )
            .unwrap();

        saver.delete_thread("t1").unwrap();

        assert!(saver.get_tuple(&cfg).unwrap().is_none());
    }

    #[test]
    fn get_tuple_returns_none_for_unknown_namespace() {
        let mut saver = MemorySaver::new();
        let cfg = config("t1", None);
        saver
            .put(
                &cfg,
                test_checkpoint("cp1"),
                test_metadata(CheckpointSource::Input, -1),
                HashMap::new(),
            )
            .unwrap();

        let other_ns = CheckpointConfig {
            thread_id: "t1".to_string(),
            checkpoint_ns: "other".to_string(),
            checkpoint_id: None,
        };
        assert!(saver.get_tuple(&other_ns).unwrap().is_none());
    }

    #[test]
    fn put_with_same_checkpoint_id_updates_existing() {
        let mut saver = MemorySaver::new();
        let cfg = CheckpointConfig {
            thread_id: "t1".to_string(),
            checkpoint_ns: String::new(),
            checkpoint_id: Some("cp1".to_string()),
        };
        saver
            .put(
                &cfg,
                test_checkpoint("cp1"),
                test_metadata(CheckpointSource::Input, -1),
                HashMap::new(),
            )
            .unwrap();

        let mut cp2 = test_checkpoint("cp1");
        cp2.channel_versions.insert("new_ch".to_string(), 5);
        saver
            .put(
                &cfg,
                cp2.clone(),
                test_metadata(CheckpointSource::Loop, 0),
                HashMap::new(),
            )
            .unwrap();

        let result = saver.get_tuple(&cfg).unwrap().unwrap();
        assert_eq!(
            result.checkpoint.channel_versions.get("new_ch"),
            Some(&5u64)
        );
    }
    #[test]
    fn put_writes_without_checkpoint_id_returns_error() {
        let mut saver = MemorySaver::new();
        let config = CheckpointConfig {
            thread_id: "t1".to_string(),
            checkpoint_ns: String::new(),
            checkpoint_id: None,
        };
        let result = saver.put_writes(
            &config,
            vec![PendingWrite {
                task_id: "task1".to_string(),
                channel: "msg".to_string(),
                value: StateValue::Null,
            }],
            "task1",
        );
        assert!(result.is_err());
    }
}
