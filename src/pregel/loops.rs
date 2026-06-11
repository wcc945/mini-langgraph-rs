use std::collections::{BTreeMap, HashMap, HashSet};

use crate::channel::{DynChannel, StateValue};
use crate::error::GraphError;
use crate::managed::ManagedValueSpec;
use crate::pregel::node::PregelNode;
use crate::pregel::pregel::{Pregel, PregelStreamItem, StreamMode};
use crate::pregel::task::{PregelTaskManager, PregelTaskWrites};
use crate::runtime::RuntimeContext;
use tokio::sync::mpsc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PregelLoopStatus {
    Input,
    Pending,
    Done,
    Draining,
    InterruptBefore,
    InterruptAfter,
    OutOfSteps,
}

pub(crate) struct PregelLoop<'a, StateT, UpdateT, ContextT> {
    pub(crate) nodes: &'a HashMap<String, PregelNode<StateT, UpdateT, ContextT>>,
    pub(crate) channels: HashMap<String, Box<DynChannel>>,
    pub(crate) managed: HashMap<String, Box<dyn ManagedValueSpec>>,
    pub(crate) input_channels: &'a [String],
    pub(crate) output_channels: &'a [String],
    pub(crate) stream_channels: Option<&'a [String]>,
    pub(crate) stream_mode: StreamMode,
    pub(crate) recursion_limit: usize,
    pub(crate) trigger_to_nodes: &'a HashMap<String, Vec<String>>,
    pub(crate) name: &'a str,
    pub(crate) input: Option<StateValue>,
    pub(crate) step: usize,
    pub(crate) stop: usize,
    pub(crate) status: PregelLoopStatus,
    pub(crate) task_manager: PregelTaskManager<'a, StateT, UpdateT, ContextT>,
    pub(crate) updated_channels: Option<HashSet<String>>,
    pub(crate) output: Option<StateValue>,
    pub(crate) stream_sender: mpsc::Sender<Result<PregelStreamItem, GraphError>>,
    pending_writes: Vec<PregelTaskWrites>,
    runtime_context: RuntimeContext<ContextT>,
}

impl<'a, StateT, UpdateT, ContextT> PregelLoop<'a, StateT, UpdateT, ContextT>
where
    StateT: From<StateValue>,
    UpdateT: Into<StateValue>,
    ContextT: Default,
{
    pub(crate) fn new(
        pregel: &'a Pregel<StateT, UpdateT, ContextT>,
        input: Option<StateValue>,
        stream_sender: mpsc::Sender<Result<PregelStreamItem, GraphError>>,
    ) -> Result<Self, GraphError> {
        let channels = pregel.copy_channels()?;
        let managed = pregel.copy_managed();
        let recursion_limit = pregel.recursion_limit;
        let stop = recursion_limit + 1;

        Ok(Self {
            nodes: &pregel.nodes,
            channels,
            managed,
            input_channels: &pregel.input_channels,
            output_channels: &pregel.output_channels,
            stream_channels: pregel.stream_channels.as_deref(),
            stream_mode: pregel.stream_mode,
            recursion_limit,
            trigger_to_nodes: &pregel.trigger_to_nodes,
            name: &pregel.name,
            input,
            step: 0,
            stop,
            status: PregelLoopStatus::Input,
            task_manager: PregelTaskManager::new(),
            updated_channels: None,
            output: None,
            stream_sender,
            pending_writes: Vec::new(),
            runtime_context: RuntimeContext {
                context: ContextT::default(),
            },
        })
    }

    pub(crate) fn enter(&mut self) -> Result<(), GraphError> {
        let input_channels = self.input_channels.to_vec();
        self.updated_channels = self.first(&input_channels)?;
        self.status = PregelLoopStatus::Pending;

        Ok(())
    }

    pub(crate) fn first(
        &mut self,
        input_channels: &[String],
    ) -> Result<Option<HashSet<String>>, GraphError> {
        let Some(input) = self.input.clone() else {
            return Err(GraphError::EmptyPregelInput(input_channels.to_vec()));
        };

        let input_writes = self.map_input(input_channels, input)?;
        if input_writes.is_empty() {
            return Err(GraphError::EmptyPregelInput(input_channels.to_vec()));
        }

        let updated_channels = self.apply_writes(&[PregelTaskWrites {
            name: "input".to_string(),
            writes: input_writes,
            triggers: Vec::new(),
            path: vec!["input".to_string()],
        }])?;

        Ok(Some(updated_channels))
    }

    fn map_input(
        &self,
        input_channels: &[String],
        input: StateValue,
    ) -> Result<Vec<(String, StateValue)>, GraphError> {
        if input_channels.len() == 1 {
            return Ok(vec![(input_channels[0].clone(), input)]);
        }

        let StateValue::Object(values) = input else {
            return Err(GraphError::InvalidPregelInput(format!(
                "expected object input for multiple input channels, got {input:?}"
            )));
        };

        Ok(values
            .into_iter()
            .filter(|(channel, _)| input_channels.contains(channel))
            .collect())
    }

    pub(crate) fn apply_writes(
        &mut self,
        tasks: &[PregelTaskWrites],
    ) -> Result<HashSet<String>, GraphError> {
        let mut sorted_tasks = tasks.iter().collect::<Vec<_>>();
        sorted_tasks.sort_by(|left, right| left.path.iter().take(3).cmp(right.path.iter().take(3)));

        let bump_step = sorted_tasks.iter().any(|task| !task.triggers.is_empty());

        if bump_step {
            let mut consumed_channels = sorted_tasks
                .iter()
                .flat_map(|task| task.triggers.iter())
                .filter(|channel| self.channels.contains_key(*channel))
                .cloned()
                .collect::<Vec<_>>();
            consumed_channels.sort();
            consumed_channels.dedup();

            for channel in consumed_channels {
                if let Some(channel_state) = self.channels.get_mut(&channel) {
                    channel_state.consume()?;
                }
            }
        }

        let mut pending_writes_by_channel = BTreeMap::<String, Vec<StateValue>>::new();
        for task in sorted_tasks {
            let _ = &task.name;
            for (channel, value) in &task.writes {
                if self.channels.contains_key(channel) {
                    pending_writes_by_channel
                        .entry(channel.clone())
                        .or_default()
                        .push(value.clone());
                }
            }
        }
        let mut updated_channels = HashSet::new();
        for (channel, values) in pending_writes_by_channel {
            let Some(channel_state) = self.channels.get_mut(&channel) else {
                continue;
            };

            if channel_state.update(values)? && channel_state.is_available() {
                updated_channels.insert(channel);
            }
        }

        if bump_step {
            let mut channel_names = self.channels.keys().cloned().collect::<Vec<_>>();
            channel_names.sort();

            for channel in &channel_names {
                if updated_channels.contains(channel) {
                    continue;
                }

                let Some(channel_state) = self.channels.get_mut(channel) else {
                    continue;
                };

                if channel_state.is_available()
                    && channel_state.update(Vec::new())?
                    && channel_state.is_available()
                {
                    updated_channels.insert(channel.clone());
                }
            }

            if !updated_channels
                .iter()
                .any(|channel| self.trigger_to_nodes.contains_key(channel))
            {
                for channel in channel_names {
                    let Some(channel_state) = self.channels.get_mut(&channel) else {
                        continue;
                    };

                    if channel_state.finish()? && channel_state.is_available() {
                        updated_channels.insert(channel);
                    }
                }
            }
        }

        Ok(updated_channels)
    }

    fn read_stream_channels(&self) -> Result<Option<StateValue>, GraphError> {
        let stream_channels = self.stream_channels.unwrap_or(self.output_channels);

        if stream_channels.len() == 1 {
            let Some(channel) = self.channels.get(&stream_channels[0]) else {
                return Ok(None);
            };

            if !channel.is_available() {
                return Ok(None);
            }

            return channel.get().map(Some);
        }

        let mut values = HashMap::new();
        for channel_name in stream_channels {
            let Some(channel) = self.channels.get(channel_name) else {
                continue;
            };

            if channel.is_available() {
                values.insert(channel_name.clone(), channel.get()?);
            }
        }

        if values.is_empty() {
            Ok(None)
        } else {
            Ok(Some(StateValue::Object(values)))
        }
    }

    fn map_output_updates(&self, tasks: &[PregelTaskWrites]) -> Option<StateValue> {
        let stream_channels = self
            .stream_channels
            .unwrap_or(self.output_channels)
            .iter()
            .map(String::as_str)
            .collect::<HashSet<_>>();
        let stream_channel_count = stream_channels.len();
        let mut grouped = BTreeMap::<String, Vec<StateValue>>::new();

        for task in tasks {
            let writes = task
                .writes
                .iter()
                .filter(|(channel, _)| stream_channels.contains(channel.as_str()))
                .collect::<Vec<_>>();

            if writes.is_empty() {
                continue;
            }

            let updates = if stream_channel_count == 1 {
                writes
                    .into_iter()
                    .map(|(_, value)| value.clone())
                    .collect::<Vec<_>>()
            } else {
                let mut counts = HashMap::<&str, usize>::new();
                for (channel, _) in &writes {
                    *counts.entry(channel.as_str()).or_default() += 1;
                }

                if counts.values().any(|count| *count > 1) {
                    writes
                        .into_iter()
                        .map(|(channel, value)| {
                            StateValue::Object(HashMap::from([(channel.clone(), value.clone())]))
                        })
                        .collect::<Vec<_>>()
                } else {
                    vec![StateValue::Object(
                        writes
                            .into_iter()
                            .map(|(channel, value)| (channel.clone(), value.clone()))
                            .collect(),
                    )]
                }
            };

            grouped
                .entry(task.name.clone())
                .or_default()
                .extend(updates);
        }

        if grouped.is_empty() {
            return None;
        }

        Some(StateValue::Object(
            grouped
                .into_iter()
                .map(|(node, values)| {
                    let value = if values.len() == 1 {
                        values.into_iter().next().unwrap_or(StateValue::Null)
                    } else {
                        StateValue::List(values)
                    };
                    (node, value)
                })
                .collect(),
        ))
    }

    pub(crate) fn tick(&mut self) -> Result<bool, GraphError> {
        if self.step > self.stop {
            self.status = PregelLoopStatus::OutOfSteps;
            return Err(GraphError::PregelRecursionLimitReached(
                self.recursion_limit,
            ));
        }

        self.pending_writes.clear();
        self.task_manager.clear_tasks();

        let tasks = self.task_manager.prepare_tasks(
            self.nodes,
            &self.channels,
            &self.managed,
            self.trigger_to_nodes,
            self.updated_channels.as_ref(),
            self.step,
        )?;

        if tasks.is_empty() {
            self.status = PregelLoopStatus::Done;
            return Ok(false);
        }

        self.status = PregelLoopStatus::Pending;
        Ok(true)
    }

    pub(crate) async fn execute(&mut self) -> Result<(), GraphError>
    where
        StateT: Send,
        UpdateT: Send,
        ContextT: Sync,
    {
        self.pending_writes = self
            .task_manager
            .execute_pending_tasks(&self.runtime_context)?;

        if self.stream_mode == StreamMode::Updates
            && let Some(data) = self.map_output_updates(&self.pending_writes)
        {
            let item = PregelStreamItem {
                step: self.step,
                mode: StreamMode::Updates,
                data,
            };
            let sender = self.stream_sender.clone();
            let _ = sender.send(Ok(item)).await;
        }

        Ok(())
    }

    pub(crate) async fn after_tick(&mut self) -> Result<(), GraphError> {
        let pending_writes = std::mem::take(&mut self.pending_writes);
        let updated_channels = self.apply_writes(&pending_writes)?;
        let stream_channels = self
            .stream_channels
            .unwrap_or(self.output_channels)
            .iter()
            .map(String::as_str)
            .collect::<HashSet<_>>();
        let stream_channels_updated = updated_channels
            .iter()
            .any(|channel| stream_channels.contains(channel.as_str()));

        if self.stream_mode == StreamMode::Values
            && stream_channels_updated
            && let Some(data) = self.read_stream_channels()?
        {
            let item = PregelStreamItem {
                step: self.step,
                mode: StreamMode::Values,
                data,
            };
            let sender = self.stream_sender.clone();
            let _ = sender.send(Ok(item)).await;
        }

        self.updated_channels = Some(updated_channels);
        self.step += 1;

        Ok(())
    }

    pub(crate) fn is_stream_closed(&self) -> bool {
        self.stream_sender.is_closed()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::channel::BaseChannel;
    use crate::channel::binop::BinaryOperatorAggregate;
    use crate::channel::channel_writer::{
        ChannelWriteEntry, ChannelWriteValue, ChannelWriter, ChannelWriterEntry,
    };
    use crate::channel::ephemeral_value::EphemeralValue;
    use crate::channel::last_value::LastValue;
    use crate::channel::named_barrier_value::NamedBarrierValue;
    use crate::graph::node::NodeOutput;
    use crate::managed::ManagedValueSpec;
    use crate::pregel::node::PregelNode;
    use crate::pregel::task::{PregelExecutableTask, PregelTaskWrites};

    struct TestManagedValue;

    impl ManagedValueSpec for TestManagedValue {
        fn copy_box(&self) -> Box<dyn ManagedValueSpec> {
            Box::new(TestManagedValue)
        }
    }

    fn channel() -> Box<DynChannel> {
        Box::new(LastValue::new())
    }

    fn add_list(left: StateValue, right: StateValue) -> Result<StateValue, GraphError> {
        let mut values = match left {
            StateValue::List(values) => values,
            value => vec![value],
        };
        values.push(right);
        Ok(StateValue::List(values))
    }

    struct ChangedUnavailable;

    impl BaseChannel for ChangedUnavailable {
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
            Ok(None)
        }

        fn from_checkpoint(
            &self,
            _checkpoint: Option<Self::Checkpoint>,
        ) -> Result<Self, GraphError> {
            Ok(Self)
        }

        fn copy_box(&self) -> Result<Box<DynChannel>, GraphError> {
            Ok(Box::new(Self))
        }

        fn get(&self) -> Result<Self::Value, GraphError> {
            Err(GraphError::EmptyChannel)
        }

        fn is_available(&self) -> bool {
            false
        }

        fn update(&mut self, values: Vec<Self::Update>) -> Result<bool, GraphError> {
            Ok(!values.is_empty())
        }
    }

    struct FinishOnDemand {
        value: Option<StateValue>,
        finish_value: StateValue,
    }

    impl FinishOnDemand {
        fn new(finish_value: StateValue) -> Self {
            Self {
                value: None,
                finish_value,
            }
        }
    }

    impl BaseChannel for FinishOnDemand {
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
            Ok(Self {
                value: checkpoint,
                finish_value: self.finish_value.clone(),
            })
        }

        fn copy_box(&self) -> Result<Box<DynChannel>, GraphError> {
            Ok(Box::new(self.copy()?))
        }

        fn get(&self) -> Result<Self::Value, GraphError> {
            self.value.clone().ok_or(GraphError::EmptyChannel)
        }

        fn is_available(&self) -> bool {
            self.value.is_some()
        }

        fn update(&mut self, values: Vec<Self::Update>) -> Result<bool, GraphError> {
            let Some(value) = values.into_iter().last() else {
                return Ok(false);
            };
            self.value = Some(value);
            Ok(true)
        }

        fn finish(&mut self) -> Result<bool, GraphError> {
            self.value = Some(self.finish_value.clone());
            Ok(true)
        }
    }

    fn task(
        name: &str,
        path: Vec<&str>,
        triggers: Vec<&str>,
        writes: Vec<(&str, StateValue)>,
    ) -> PregelTaskWrites {
        PregelTaskWrites {
            name: name.to_string(),
            writes: writes
                .into_iter()
                .map(|(channel, value)| (channel.to_string(), value))
                .collect(),
            triggers: triggers.into_iter().map(str::to_string).collect(),
            path: path.into_iter().map(str::to_string).collect(),
        }
    }

    fn noop_node() -> PregelNode<StateValue, StateValue, ()> {
        PregelNode::new(
            Vec::new(),
            Vec::new(),
            None,
            Vec::new(),
            Box::new(|_, _| Ok(NodeOutput::None)),
        )
    }

    fn fixed_writer(channel: &str, value: StateValue) -> ChannelWriter<StateValue, ()> {
        ChannelWriter::new(vec![ChannelWriterEntry::Channel(ChannelWriteEntry {
            channel: channel.to_string(),
            value: ChannelWriteValue::Value(value),
            skip_none: false,
            mapper: None,
        })])
    }

    #[test]
    fn executable_task_projects_to_task_writes() {
        let node = noop_node();
        let executable = PregelExecutableTask {
            name: "node".to_string(),
            input: StateValue::Null,
            bound: &node.bound,
            writes: vec![("out".to_string(), StateValue::Number(1.0))],
            writers: &node.writers,
            triggers: vec!["trigger".to_string()],
            id: "id".to_string(),
            path: vec!["pull".to_string(), "node".to_string()],
        };

        let writes = executable.to_writes();

        assert_eq!(writes.name, "node");
        assert_eq!(
            writes.writes,
            vec![("out".to_string(), StateValue::Number(1.0))]
        );
        assert_eq!(writes.triggers, vec!["trigger".to_string()]);
        assert_eq!(writes.path, vec!["pull".to_string(), "node".to_string()]);
    }

    fn valid_pregel() -> Pregel<StateValue, StateValue, ()> {
        Pregel::new(
            HashMap::from([(
                "a".to_string(),
                PregelNode::new(
                    vec!["input".to_string(), "managed".to_string()],
                    vec!["input".to_string()],
                    None,
                    Vec::new(),
                    Box::new(|_, _| Ok(NodeOutput::<StateValue>::None)),
                ),
            )]),
            HashMap::from([
                ("input".to_string(), channel()),
                ("output".to_string(), channel()),
            ]),
            HashMap::from([(
                "managed".to_string(),
                Box::new(TestManagedValue) as Box<dyn ManagedValueSpec>,
            )]),
            vec!["input".to_string()],
            vec!["output".to_string()],
        )
        .unwrap()
    }

    #[test]
    fn initializes_loop_with_copied_channels() {
        let pregel = valid_pregel();
        let expected_stop = pregel.recursion_limit + 1;
        let (sender, _receiver) = mpsc::channel(1);
        let loop_state = PregelLoop::new(&pregel, Some(StateValue::Number(1.0)), sender).unwrap();

        assert_eq!(loop_state.input, Some(StateValue::Number(1.0)));
        assert_eq!(loop_state.step, 0);
        assert_eq!(loop_state.stop, expected_stop);
        assert_eq!(loop_state.status, PregelLoopStatus::Input);
        assert_eq!(loop_state.nodes.len(), 1);
        assert_eq!(loop_state.channels.len(), 2);
        assert_eq!(loop_state.managed.len(), 1);
        assert_eq!(loop_state.input_channels, ["input".to_string()]);
        assert_eq!(loop_state.output_channels, ["output".to_string()]);
        assert_eq!(loop_state.stream_channels, None);
        assert_eq!(loop_state.stream_mode, StreamMode::Values);
        assert_eq!(loop_state.recursion_limit, expected_stop - 1);
        assert_eq!(loop_state.trigger_to_nodes.len(), 1);
        assert_eq!(loop_state.name, "LangGraph");
        assert_eq!(loop_state.updated_channels, None);
        assert_eq!(loop_state.output, None);
    }

    #[test]
    fn enter_applies_single_input_channel() {
        let pregel = valid_pregel();
        let (sender, _receiver) = mpsc::channel(1);
        let mut loop_state =
            PregelLoop::new(&pregel, Some(StateValue::Number(1.0)), sender).unwrap();

        loop_state.enter().unwrap();

        assert_eq!(loop_state.status, PregelLoopStatus::Pending);
        assert_eq!(
            loop_state.updated_channels,
            Some(HashSet::from(["input".to_string()]))
        );
        assert_eq!(
            loop_state.channels.get("input").unwrap().get().unwrap(),
            StateValue::Number(1.0)
        );
    }

    #[test]
    fn enter_maps_object_input_for_multiple_input_channels() {
        let mut pregel = valid_pregel();
        pregel.channels.insert("other".to_string(), channel());
        pregel.input_channels = vec!["input".to_string(), "other".to_string()];
        pregel.validate().unwrap();
        let (sender, _receiver) = mpsc::channel(1);
        let input = StateValue::Object(HashMap::from([
            ("input".to_string(), StateValue::String("first".to_string())),
            (
                "other".to_string(),
                StateValue::String("second".to_string()),
            ),
            (
                "ignored".to_string(),
                StateValue::String("third".to_string()),
            ),
        ]));
        let mut loop_state = PregelLoop::new(&pregel, Some(input), sender).unwrap();

        loop_state.enter().unwrap();

        assert_eq!(
            loop_state.updated_channels,
            Some(HashSet::from(["input".to_string(), "other".to_string()]))
        );
        assert_eq!(
            loop_state.channels.get("input").unwrap().get().unwrap(),
            StateValue::String("first".to_string())
        );
        assert_eq!(
            loop_state.channels.get("other").unwrap().get().unwrap(),
            StateValue::String("second".to_string())
        );
        assert!(!loop_state.channels.contains_key("ignored"));
    }

    #[test]
    fn enter_rejects_empty_input() {
        let pregel = valid_pregel();
        let (sender, _receiver) = mpsc::channel(1);
        let mut loop_state = PregelLoop::new(&pregel, None, sender).unwrap();

        let error = loop_state.enter().unwrap_err();

        assert!(
            matches!(error, GraphError::EmptyPregelInput(channels) if channels == vec!["input"])
        );
    }

    #[test]
    fn enter_rejects_non_object_input_for_multiple_input_channels() {
        let mut pregel = valid_pregel();
        pregel.channels.insert("other".to_string(), channel());
        pregel.input_channels = vec!["input".to_string(), "other".to_string()];
        pregel.validate().unwrap();
        let (sender, _receiver) = mpsc::channel(1);
        let mut loop_state = PregelLoop::new(
            &pregel,
            Some(StateValue::String("not an object".to_string())),
            sender,
        )
        .unwrap();

        let error = loop_state.enter().unwrap_err();

        assert!(matches!(error, GraphError::InvalidPregelInput(_)));
    }

    #[test]
    fn first_applies_input_writes_through_apply_writes() {
        let mut pregel = valid_pregel();
        pregel
            .channels
            .insert("input".to_string(), Box::new(ChangedUnavailable));
        pregel.validate().unwrap();
        let (sender, _receiver) = mpsc::channel(1);
        let mut loop_state = PregelLoop::new(
            &pregel,
            Some(StateValue::String("value".to_string())),
            sender,
        )
        .unwrap();

        loop_state.enter().unwrap();

        assert_eq!(loop_state.updated_channels, Some(HashSet::new()));
    }

    #[test]
    fn first_uses_supplied_input_channels() {
        let mut pregel = valid_pregel();
        pregel.channels.insert("other".to_string(), channel());
        pregel.channels.insert("unused".to_string(), channel());
        let input = StateValue::Object(HashMap::from([
            (
                "input".to_string(),
                StateValue::String("ignored".to_string()),
            ),
            (
                "other".to_string(),
                StateValue::String("applied".to_string()),
            ),
        ]));
        let (sender, _receiver) = mpsc::channel(1);
        let mut loop_state = PregelLoop::new(&pregel, Some(input), sender).unwrap();
        let input_channels = vec!["other".to_string(), "unused".to_string()];

        let updated = loop_state.first(&input_channels).unwrap();

        assert_eq!(updated, Some(HashSet::from(["other".to_string()])));
        assert!(matches!(
            loop_state.channels.get("input").unwrap().get(),
            Err(GraphError::EmptyChannel)
        ));
        assert_eq!(
            loop_state.channels.get("other").unwrap().get().unwrap(),
            StateValue::String("applied".to_string())
        );
        assert!(matches!(
            loop_state.channels.get("unused").unwrap().get(),
            Err(GraphError::EmptyChannel)
        ));
    }
    #[test]
    fn apply_writes_groups_values_in_sorted_task_path_order() {
        let mut pregel = valid_pregel();
        pregel.channels.insert(
            "sum".to_string(),
            Box::new(BinaryOperatorAggregate::new(add_list)),
        );
        let (sender, _receiver) = mpsc::channel(1);
        let mut loop_state = PregelLoop::new(&pregel, Some(StateValue::Null), sender).unwrap();
        let tasks = vec![
            task(
                "later",
                vec!["pull", "b"],
                vec![],
                vec![("sum", StateValue::String("second".to_string()))],
            ),
            task(
                "earlier",
                vec!["pull", "a"],
                vec![],
                vec![("sum", StateValue::String("first".to_string()))],
            ),
        ];

        let updated = loop_state.apply_writes(&tasks).unwrap();

        assert_eq!(updated, HashSet::from(["sum".to_string()]));
        assert_eq!(
            loop_state.channels.get("sum").unwrap().get().unwrap(),
            StateValue::List(vec![
                StateValue::String("first".to_string()),
                StateValue::String("second".to_string()),
            ])
        );
    }

    #[test]
    fn apply_writes_propagates_channel_update_errors() {
        let mut pregel = valid_pregel();
        pregel.channels.insert("single".to_string(), channel());
        let (sender, _receiver) = mpsc::channel(1);
        let mut loop_state = PregelLoop::new(&pregel, Some(StateValue::Null), sender).unwrap();
        let tasks = vec![
            task(
                "a",
                vec!["pull", "a"],
                vec![],
                vec![("single", StateValue::Number(1.0))],
            ),
            task(
                "b",
                vec!["pull", "b"],
                vec![],
                vec![("single", StateValue::Number(2.0))],
            ),
        ];

        let error = loop_state.apply_writes(&tasks).unwrap_err();

        assert!(matches!(
            error,
            GraphError::MultipleUpdatesWithoutReducer { count: 2 }
        ));
    }

    #[test]
    fn apply_writes_ignores_unknown_channels() {
        let pregel = valid_pregel();
        let (sender, _receiver) = mpsc::channel(1);
        let mut loop_state = PregelLoop::new(&pregel, Some(StateValue::Null), sender).unwrap();
        let tasks = vec![task(
            "a",
            vec!["pull", "a"],
            vec![],
            vec![("missing", StateValue::Number(1.0))],
        )];

        let updated = loop_state.apply_writes(&tasks).unwrap();

        assert!(updated.is_empty());
        assert!(!loop_state.channels.contains_key("missing"));
    }

    #[test]
    fn apply_writes_consumes_read_trigger_channels() {
        let mut pregel = valid_pregel();
        let mut barrier = NamedBarrierValue::new(["a"]);
        barrier
            .update(vec![StateValue::String("a".to_string())])
            .unwrap();
        pregel
            .channels
            .insert("join".to_string(), Box::new(barrier));
        let (sender, _receiver) = mpsc::channel(1);
        let mut loop_state = PregelLoop::new(&pregel, Some(StateValue::Null), sender).unwrap();
        let tasks = vec![task("a", vec!["pull", "a"], vec!["join"], vec![])];

        let updated = loop_state.apply_writes(&tasks).unwrap();

        assert!(updated.is_empty());
        assert!(!loop_state.channels.get("join").unwrap().is_available());
    }

    #[test]
    fn apply_writes_empty_updates_available_channels_during_real_step() {
        let mut pregel = valid_pregel();
        let mut signal = EphemeralValue::new(true);
        signal.update(vec![StateValue::Null]).unwrap();
        pregel
            .channels
            .insert("signal".to_string(), Box::new(signal));
        let (sender, _receiver) = mpsc::channel(1);
        let mut loop_state = PregelLoop::new(&pregel, Some(StateValue::Null), sender).unwrap();
        let tasks = vec![task("a", vec!["pull", "a"], vec!["input"], vec![])];

        let updated = loop_state.apply_writes(&tasks).unwrap();

        assert!(updated.is_empty());
        assert!(!loop_state.channels.get("signal").unwrap().is_available());
    }

    #[test]
    fn apply_writes_without_triggers_only_applies_direct_writes() {
        let mut pregel = valid_pregel();
        let mut signal = EphemeralValue::new(true);
        signal.update(vec![StateValue::Null]).unwrap();
        pregel
            .channels
            .insert("signal".to_string(), Box::new(signal));
        pregel.channels.insert(
            "finish".to_string(),
            Box::new(FinishOnDemand::new(StateValue::String("done".to_string()))),
        );
        let (sender, _receiver) = mpsc::channel(1);
        let mut loop_state = PregelLoop::new(&pregel, Some(StateValue::Null), sender).unwrap();
        let tasks = vec![task(
            "input",
            vec!["input"],
            vec![],
            vec![("output", StateValue::String("written".to_string()))],
        )];

        let updated = loop_state.apply_writes(&tasks).unwrap();

        assert_eq!(updated, HashSet::from(["output".to_string()]));
        assert!(loop_state.channels.get("signal").unwrap().is_available());
        assert!(!loop_state.channels.get("finish").unwrap().is_available());
    }

    #[test]
    fn apply_writes_finishes_when_no_updated_channel_can_trigger_nodes() {
        let mut pregel = valid_pregel();
        pregel.channels.insert(
            "finish".to_string(),
            Box::new(FinishOnDemand::new(StateValue::String("done".to_string()))),
        );
        let (sender, _receiver) = mpsc::channel(1);
        let mut loop_state = PregelLoop::new(&pregel, Some(StateValue::Null), sender).unwrap();
        let tasks = vec![task(
            "a",
            vec!["pull", "a"],
            vec!["input"],
            vec![("output", StateValue::String("value".to_string()))],
        )];

        let updated = loop_state.apply_writes(&tasks).unwrap();

        assert_eq!(
            updated,
            HashSet::from(["output".to_string(), "finish".to_string()])
        );
        assert_eq!(
            loop_state.channels.get("finish").unwrap().get().unwrap(),
            StateValue::String("done".to_string())
        );
    }

    #[test]
    fn tick_prepares_tasks_from_updated_channels() {
        let pregel = valid_pregel();
        let (sender, _receiver) = mpsc::channel(1);
        let mut loop_state = PregelLoop::new(
            &pregel,
            Some(StateValue::String("start".to_string())),
            sender,
        )
        .unwrap();
        loop_state.enter().unwrap();

        let should_continue = loop_state.tick().unwrap();

        assert!(should_continue);
        assert_eq!(loop_state.status, PregelLoopStatus::Pending);
        assert!(loop_state.pending_writes.is_empty());
        assert_eq!(loop_state.task_manager.task_count(), 1);
    }

    #[test]
    fn tick_returns_false_and_marks_done_without_tasks() {
        let mut pregel = valid_pregel();
        pregel
            .channels
            .insert("input".to_string(), Box::new(ChangedUnavailable));
        pregel.validate().unwrap();
        let (sender, _receiver) = mpsc::channel(1);
        let mut loop_state = PregelLoop::new(
            &pregel,
            Some(StateValue::String("start".to_string())),
            sender,
        )
        .unwrap();
        loop_state.enter().unwrap();

        let should_continue = loop_state.tick().unwrap();

        assert!(!should_continue);
        assert_eq!(loop_state.status, PregelLoopStatus::Done);
        assert_eq!(loop_state.task_manager.task_count(), 0);
    }

    #[tokio::test]
    async fn after_tick_applies_pending_writes_and_advances_step() {
        let pregel = Pregel::new(
            HashMap::from([(
                "a".to_string(),
                PregelNode::new(
                    vec!["input".to_string()],
                    vec!["input".to_string()],
                    None,
                    vec![fixed_writer(
                        "output",
                        StateValue::String("done".to_string()),
                    )],
                    Box::new(|_, _| Ok(NodeOutput::<StateValue>::None)),
                ),
            )]),
            HashMap::from([
                ("input".to_string(), channel()),
                ("output".to_string(), channel()),
            ]),
            HashMap::new(),
            vec!["input".to_string()],
            vec!["output".to_string()],
        )
        .unwrap();
        let (sender, _receiver) = mpsc::channel(1);
        let mut loop_state = PregelLoop::new(
            &pregel,
            Some(StateValue::String("start".to_string())),
            sender,
        )
        .unwrap();
        loop_state.enter().unwrap();

        assert!(loop_state.tick().unwrap());
        loop_state.execute().await.unwrap();
        loop_state.after_tick().await.unwrap();

        assert_eq!(loop_state.step, 1);
        assert!(loop_state.pending_writes.is_empty());
        assert_eq!(
            loop_state.updated_channels,
            Some(HashSet::from(["output".to_string()]))
        );
        assert_eq!(
            loop_state.channels.get("output").unwrap().get().unwrap(),
            StateValue::String("done".to_string())
        );
    }

    #[test]
    fn tick_rejects_step_beyond_recursion_limit() {
        let pregel = valid_pregel();
        let (sender, _receiver) = mpsc::channel(1);
        let mut loop_state = PregelLoop::new(&pregel, Some(StateValue::Null), sender).unwrap();
        loop_state.step = loop_state.stop + 1;

        let error = loop_state.tick().unwrap_err();

        assert_eq!(loop_state.status, PregelLoopStatus::OutOfSteps);
        assert!(matches!(
            error,
            GraphError::PregelRecursionLimitReached(limit) if limit == pregel.recursion_limit
        ));
    }

    #[tokio::test]
    async fn execute_runs_prepared_tasks_and_keeps_writes_pending() {
        let pregel = Pregel::new(
            HashMap::from([(
                "a".to_string(),
                PregelNode::new(
                    vec!["input".to_string()],
                    vec!["input".to_string()],
                    None,
                    vec![fixed_writer(
                        "output",
                        StateValue::String("done".to_string()),
                    )],
                    Box::new(|_, _| Ok(NodeOutput::<StateValue>::None)),
                ),
            )]),
            HashMap::from([
                ("input".to_string(), channel()),
                ("output".to_string(), channel()),
            ]),
            HashMap::new(),
            vec!["input".to_string()],
            vec!["output".to_string()],
        )
        .unwrap();
        let (sender, _receiver) = mpsc::channel(1);
        let mut loop_state = PregelLoop::new(
            &pregel,
            Some(StateValue::String("start".to_string())),
            sender,
        )
        .unwrap();
        loop_state.enter().unwrap();
        {
            let tasks = loop_state
                .task_manager
                .prepare_tasks(
                    loop_state.nodes,
                    &loop_state.channels,
                    &loop_state.managed,
                    loop_state.trigger_to_nodes,
                    loop_state.updated_channels.as_ref(),
                    loop_state.step,
                )
                .unwrap();
            assert_eq!(tasks.len(), 1);
        }

        loop_state.execute().await.unwrap();

        assert_eq!(loop_state.pending_writes.len(), 1);
        assert_eq!(loop_state.pending_writes[0].name, "a");
        assert_eq!(
            loop_state.pending_writes[0].writes,
            vec![("output".to_string(), StateValue::String("done".to_string()))]
        );
        assert!(matches!(
            loop_state.channels.get("output").unwrap().get(),
            Err(GraphError::EmptyChannel)
        ));
    }

    #[tokio::test]
    async fn execute_sends_updates_for_output_writes() {
        let mut pregel = Pregel::new(
            HashMap::from([(
                "a".to_string(),
                PregelNode::new(
                    vec!["input".to_string()],
                    vec!["input".to_string()],
                    None,
                    vec![fixed_writer(
                        "output",
                        StateValue::String("done".to_string()),
                    )],
                    Box::new(|_, _| Ok(NodeOutput::<StateValue>::None)),
                ),
            )]),
            HashMap::from([
                ("input".to_string(), channel()),
                ("output".to_string(), channel()),
            ]),
            HashMap::new(),
            vec!["input".to_string()],
            vec!["output".to_string()],
        )
        .unwrap();
        pregel.stream_mode = StreamMode::Updates;
        let (sender, mut receiver) = mpsc::channel(4);
        let mut loop_state = PregelLoop::new(
            &pregel,
            Some(StateValue::String("start".to_string())),
            sender,
        )
        .unwrap();
        loop_state.enter().unwrap();
        assert!(loop_state.tick().unwrap());

        loop_state.execute().await.unwrap();

        let item = receiver.recv().await.unwrap().unwrap();
        assert_eq!(item.step, 0);
        assert_eq!(item.mode, StreamMode::Updates);
        assert_eq!(
            item.data,
            StateValue::Object(HashMap::from([(
                "a".to_string(),
                StateValue::String("done".to_string())
            )]))
        );
    }

    #[tokio::test]
    async fn execute_skips_updates_without_output_writes() {
        let mut pregel = Pregel::new(
            HashMap::from([(
                "a".to_string(),
                PregelNode::new(
                    vec!["input".to_string()],
                    vec!["input".to_string()],
                    None,
                    vec![fixed_writer("side", StateValue::String("side".to_string()))],
                    Box::new(|_, _| Ok(NodeOutput::<StateValue>::None)),
                ),
            )]),
            HashMap::from([
                ("input".to_string(), channel()),
                ("output".to_string(), channel()),
                ("side".to_string(), channel()),
            ]),
            HashMap::new(),
            vec!["input".to_string()],
            vec!["output".to_string()],
        )
        .unwrap();
        pregel.stream_mode = StreamMode::Updates;
        let (sender, mut receiver) = mpsc::channel(4);
        let mut loop_state = PregelLoop::new(
            &pregel,
            Some(StateValue::String("start".to_string())),
            sender,
        )
        .unwrap();
        loop_state.enter().unwrap();
        assert!(loop_state.tick().unwrap());

        loop_state.execute().await.unwrap();

        assert!(receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn after_tick_sends_values_after_applying_writes() {
        let pregel = Pregel::new(
            HashMap::from([(
                "a".to_string(),
                PregelNode::new(
                    vec!["input".to_string()],
                    vec!["input".to_string()],
                    None,
                    vec![fixed_writer(
                        "output",
                        StateValue::String("done".to_string()),
                    )],
                    Box::new(|_, _| Ok(NodeOutput::<StateValue>::None)),
                ),
            )]),
            HashMap::from([
                ("input".to_string(), channel()),
                ("output".to_string(), channel()),
            ]),
            HashMap::new(),
            vec!["input".to_string()],
            vec!["output".to_string()],
        )
        .unwrap();
        let (sender, mut receiver) = mpsc::channel(4);
        let mut loop_state = PregelLoop::new(
            &pregel,
            Some(StateValue::String("start".to_string())),
            sender,
        )
        .unwrap();
        loop_state.enter().unwrap();
        assert!(loop_state.tick().unwrap());
        loop_state.execute().await.unwrap();

        loop_state.after_tick().await.unwrap();

        let item = receiver.recv().await.unwrap().unwrap();
        assert_eq!(item.step, 0);
        assert_eq!(item.mode, StreamMode::Values);
        assert_eq!(item.data, StateValue::String("done".to_string()));
    }

    #[tokio::test]
    async fn after_tick_skips_values_when_stream_channels_unchanged() {
        let pregel = Pregel::new(
            HashMap::from([(
                "a".to_string(),
                PregelNode::new(
                    vec!["input".to_string()],
                    vec!["input".to_string()],
                    None,
                    vec![fixed_writer("side", StateValue::String("side".to_string()))],
                    Box::new(|_, _| Ok(NodeOutput::<StateValue>::None)),
                ),
            )]),
            HashMap::from([
                ("input".to_string(), channel()),
                ("output".to_string(), channel()),
                ("side".to_string(), channel()),
            ]),
            HashMap::new(),
            vec!["input".to_string()],
            vec!["output".to_string()],
        )
        .unwrap();
        let (sender, mut receiver) = mpsc::channel(4);
        let mut loop_state = PregelLoop::new(
            &pregel,
            Some(StateValue::String("start".to_string())),
            sender,
        )
        .unwrap();
        loop_state.enter().unwrap();
        assert!(loop_state.tick().unwrap());
        loop_state.execute().await.unwrap();

        loop_state.after_tick().await.unwrap();

        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn is_stream_closed_reflects_receiver_drop() {
        let pregel = valid_pregel();
        let (sender, receiver) = mpsc::channel(1);
        let loop_state = PregelLoop::new(&pregel, Some(StateValue::Null), sender).unwrap();

        drop(receiver);

        assert!(loop_state.is_stream_closed());
    }
}
