use std::collections::{HashMap, HashSet};

use crate::channel::StateValue;
use crate::error::GraphError;

pub mod memory;
pub mod saver;

pub use memory::MemorySaver;
pub use saver::CheckpointSaver;

/// Checkpoint format version.
pub const CHECKPOINT_VERSION: u32 = 1;

/// The source that created a checkpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointSource {
    Input,
    Loop,
    Update,
    Fork,
}

/// Metadata associated with a checkpoint.
#[derive(Debug, Clone)]
pub struct CheckpointMetadata {
    pub source: CheckpointSource,
    pub step: i64,
    pub parents: HashMap<String, String>,
}

/// Configuration key used to locate a checkpoint.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CheckpointConfig {
    pub thread_id: String,
    pub checkpoint_ns: String,
    pub checkpoint_id: Option<String>,
}

/// State snapshot at a step boundary.
#[derive(Debug, Clone)]
pub struct Checkpoint {
    pub id: String,
    pub ts: String,
    pub channel_values: HashMap<String, StateValue>,
    pub channel_versions: HashMap<String, u64>,
    pub versions_seen: HashMap<String, HashMap<String, u64>>,
    pub updated_channels: Option<Vec<String>>,
}

/// A write that happened after a checkpoint but before the next one.
#[derive(Debug, Clone)]
pub struct PendingWrite {
    pub task_id: String,
    pub channel: String,
    pub value: StateValue,
}

/// Full result returned by get_tuple.
#[derive(Debug, Clone)]
pub struct CheckpointTuple {
    pub config: CheckpointConfig,
    pub checkpoint: Checkpoint,
    pub metadata: CheckpointMetadata,
    pub parent_config: Option<CheckpointConfig>,
    pub pending_writes: Vec<PendingWrite>,
}

/// Create an empty checkpoint with no channel data.
pub fn empty_checkpoint() -> Checkpoint {
    Checkpoint {
        id: format!("{:016x}", 0u64),
        ts: now_rfc3339(),
        channel_values: HashMap::new(),
        channel_versions: HashMap::new(),
        versions_seen: HashMap::new(),
        updated_channels: None,
    }
}

/// Create a new checkpoint from live channel state.
///
/// Reads checkpoint() from each channel, increments versions
/// for channels listed in updated_channels, and carries forward
/// versions_seen from the previous checkpoint.
pub(crate) fn create_checkpoint(
    prev: &Checkpoint,
    channels: &HashMap<String, Box<crate::channel::DynChannel>>,
    step: usize,
    channel_versions: &HashMap<String, u64>,
    updated_channels: Option<&HashSet<String>>,
) -> Result<Checkpoint, GraphError> {
    let mut channel_values = HashMap::new();

    for (name, channel) in channels {
        match channel.checkpoint() {
            Ok(Some(value)) => {
                channel_values.insert(name.clone(), value);
            }
            Ok(None) => {}
            Err(e) => return Err(e),
        }
    }

    Ok(Checkpoint {
        id: format!("{:016x}", (step as u64) + 1),
        ts: now_rfc3339(),
        channel_values,
        channel_versions: channel_versions.clone(),
        versions_seen: prev.versions_seen.clone(),
        updated_channels: updated_channels.map(|s| {
            let mut v: Vec<String> = s.iter().cloned().collect();
            v.sort();
            v
        }),
    })
}

/// Deep-copy a checkpoint.
pub fn copy_checkpoint(src: &Checkpoint) -> Checkpoint {
    Checkpoint {
        id: src.id.clone(),
        ts: src.ts.clone(),
        channel_values: src.channel_values.clone(),
        channel_versions: src.channel_versions.clone(),
        versions_seen: src.versions_seen.clone(),
        updated_channels: src.updated_channels.clone(),
    }
}

fn now_rfc3339() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = d.as_secs() as i64;
    let nanos = d.subsec_nanos();
    let (y, mo, d, h, mi, s) = unix_to_calendar(secs);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:09}Z",
        y, mo, d, h, mi, s, nanos,
    )
}

/// Convert Unix timestamp (seconds since epoch) to (year, month, day, hour, min, sec).
/// Uses the Howard Hinnant algorithm with proper leap-year handling.
fn unix_to_calendar(secs: i64) -> (i64, u32, u32, u32, u32, u32) {
    let days = secs.div_euclid(86400);
    let tod = secs.rem_euclid(86400);
    let hour = (tod / 3600) as u32;
    let min = ((tod % 3600) / 60) as u32;
    let sec = (tod % 60) as u32;

    // days since civil epoch 0000-03-01
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { y + 1 } else { y };

    (year, month, day, hour, min, sec)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channel::BaseChannel;
    use crate::channel::last_value::LastValue;

    #[test]
    fn empty_checkpoint_has_no_data() {
        let cp = empty_checkpoint();
        assert_eq!(cp.id, format!("{:016x}", 0u64));
        assert!(!cp.ts.is_empty());
        assert!(cp.channel_values.is_empty());
        assert!(cp.channel_versions.is_empty());
        assert!(cp.versions_seen.is_empty());
        assert!(cp.updated_channels.is_none());
    }

    #[test]
    fn copy_checkpoint_produces_identical_snapshot() {
        let mut cp = empty_checkpoint();
        cp.channel_values
            .insert("msg".to_string(), StateValue::String("hello".to_string()));
        cp.channel_versions.insert("msg".to_string(), 3);

        let copy = copy_checkpoint(&cp);
        assert_eq!(copy.id, cp.id);
        assert_eq!(copy.channel_values, cp.channel_values);
        assert_eq!(copy.channel_versions, cp.channel_versions);

        // Mutating the copy does not affect the original.
        let _ = cp
            .channel_values
            .insert("other".to_string(), StateValue::Null);
        assert!(!copy.channel_values.contains_key("other"));
    }

    #[test]
    fn create_checkpoint_reads_channels_and_increments_versions() {
        let mut channels: HashMap<String, Box<crate::channel::DynChannel>> = HashMap::new();
        let mut channel = LastValue::new();
        channel
            .update(vec![StateValue::String("data".to_string())])
            .unwrap();
        channels.insert("msg".to_string(), Box::new(channel));
        channels.insert("empty".to_string(), Box::new(LastValue::new()));

        let prev = empty_checkpoint();
        let updated: HashSet<String> = HashSet::from(["msg".to_string()]);

        let mut channel_versions = HashMap::new();
        channel_versions.insert("msg".to_string(), 1u64);
        let cp = create_checkpoint(&prev, &channels, 1, &channel_versions, Some(&updated)).unwrap();

        assert_eq!(
            cp.channel_values.get("msg"),
            Some(&StateValue::String("data".to_string()))
        );
        assert!(!cp.channel_values.contains_key("empty"));
        assert_eq!(cp.channel_versions.get("msg"), Some(&1u64));
        assert_eq!(cp.updated_channels, Some(vec!["msg".to_string()]));
    }

    #[test]
    fn checkpoint_source_display_values() {
        assert_ne!(CheckpointSource::Input, CheckpointSource::Loop);
        assert_ne!(CheckpointSource::Loop, CheckpointSource::Update);
        assert_ne!(CheckpointSource::Update, CheckpointSource::Fork);
    }

    #[test]
    fn create_checkpoint_without_updates_preserves_empty_versions() {
        let channels: HashMap<String, Box<crate::channel::DynChannel>> = HashMap::new();
        let prev = empty_checkpoint();
        let channel_versions: HashMap<String, u64> = HashMap::new();

        let cp = create_checkpoint(&prev, &channels, 0, &channel_versions, None).unwrap();

        assert!(cp.channel_versions.is_empty());
        assert!(cp.channel_values.is_empty());
        assert!(cp.updated_channels.is_none());
    }

    #[test]
    fn create_checkpoint_freeses_versions_seen_from_prev() {
        let mut prev = empty_checkpoint();
        prev.versions_seen.insert(
            "node1".to_string(),
            HashMap::from([("chan".to_string(), 3u64)]),
        );

        let channels: HashMap<String, Box<crate::channel::DynChannel>> = HashMap::new();
        let channel_versions: HashMap<String, u64> = HashMap::new();
        let cp = create_checkpoint(&prev, &channels, 0, &channel_versions, None).unwrap();

        assert_eq!(
            cp.versions_seen.get("node1").unwrap().get("chan"),
            Some(&3u64)
        );
    }

    #[test]
    fn copy_checkpoint_preserves_updated_channels() {
        let mut cp = empty_checkpoint();
        cp.updated_channels = Some(vec!["a".to_string(), "b".to_string()]);

        let copy = copy_checkpoint(&cp);
        assert_eq!(
            copy.updated_channels,
            Some(vec!["a".to_string(), "b".to_string()])
        );
    }

    #[test]
    fn checkpoint_config_defaults() {
        let config = CheckpointConfig {
            thread_id: "t1".to_string(),
            checkpoint_ns: String::new(),
            checkpoint_id: None,
        };
        assert_eq!(config.checkpoint_ns, "");
        assert!(config.checkpoint_id.is_none());
    }

    #[test]
    fn checkpoint_metadata_parents_default_empty() {
        let meta = CheckpointMetadata {
            source: CheckpointSource::Loop,
            step: 5,
            parents: HashMap::new(),
        };
        assert_eq!(meta.step, 5);
        assert!(meta.parents.is_empty());
    }
}
