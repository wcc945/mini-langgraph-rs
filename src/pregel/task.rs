use std::collections::{HashMap, HashSet};

use crate::channel::channel_writer::ChannelWriter;
use crate::channel::{DynChannel, StateValue};
use crate::error::GraphError;
use crate::graph::node::NodeOutput;
use crate::managed::ManagedValueSpec;
use crate::pregel::node::{PregelNode, PregelNodeBound};
use crate::runtime::RuntimeContext;

pub(crate) struct PregelExecutableTask<'a, StateT, UpdateT, ContextT> {
    pub(crate) name: String,
    pub(crate) input: StateT,
    pub(crate) bound: &'a PregelNodeBound<StateT, UpdateT, ContextT>,
    pub(crate) writes: Vec<(String, StateValue)>,
    pub(crate) writers: &'a [ChannelWriter<StateT, ContextT>],
    pub(crate) triggers: Vec<String>,
    pub(crate) id: String,
    pub(crate) path: Vec<String>,
}

pub(crate) struct PregelTaskWrites {
    pub(crate) name: String,
    pub(crate) writes: Vec<(String, StateValue)>,
    pub(crate) triggers: Vec<String>,
    pub(crate) path: Vec<String>,
}

impl<'a, StateT, UpdateT, ContextT> PregelExecutableTask<'a, StateT, UpdateT, ContextT> {
    pub(crate) fn to_writes(&self) -> PregelTaskWrites {
        PregelTaskWrites {
            name: self.name.clone(),
            writes: self.writes.clone(),
            triggers: self.triggers.clone(),
            path: self.path.clone(),
        }
    }
}

pub(crate) struct PregelTaskManager<'a, StateT, UpdateT, ContextT> {
    tasks: HashMap<String, PregelExecutableTask<'a, StateT, UpdateT, ContextT>>,
}

impl<'a, StateT, UpdateT, ContextT> PregelTaskManager<'a, StateT, UpdateT, ContextT>
where
    StateT: From<StateValue>,
{
    pub(crate) fn new() -> Self {
        Self {
            tasks: HashMap::new(),
        }
    }

    pub(crate) fn submit_task(
        &mut self,
        task: PregelExecutableTask<'a, StateT, UpdateT, ContextT>,
    ) {
        self.tasks.insert(task.id.clone(), task);
    }

    pub(crate) fn clear_tasks(&mut self) {
        self.tasks.clear();
    }

    #[cfg(test)]
    pub(crate) fn task_count(&self) -> usize {
        self.tasks.len()
    }

    pub(crate) fn prepare_tasks(
        &mut self,
        nodes: &'a HashMap<String, PregelNode<StateT, UpdateT, ContextT>>,
        channels: &HashMap<String, Box<DynChannel>>,
        managed: &HashMap<String, Box<dyn ManagedValueSpec>>,
        trigger_to_nodes: &HashMap<String, Vec<String>>,
        updated_channels: Option<&HashSet<String>>,
        step: usize,
    ) -> Result<Vec<&PregelExecutableTask<'a, StateT, UpdateT, ContextT>>, GraphError> {
        let mut candidate_nodes = match updated_channels {
            Some(updated_channels) => updated_channels
                .iter()
                .filter_map(|channel| trigger_to_nodes.get(channel))
                .flat_map(|nodes| nodes.iter().cloned())
                .collect::<HashSet<_>>()
                .into_iter()
                .collect::<Vec<_>>(),
            None => nodes.keys().cloned().collect(),
        };
        candidate_nodes.sort();
        let mut task_ids = Vec::new();

        for name in candidate_nodes {
            if let Some(task) = self.prepare_task(name, nodes, channels, managed, step)? {
                let id = task.id.clone();
                self.submit_task(task);
                task_ids.push(id);
            }
        }

        Ok(task_ids
            .iter()
            .filter_map(|id| self.tasks.get(id))
            .collect())
    }

    pub(crate) fn prepare_task(
        &self,
        name: String,
        nodes: &'a HashMap<String, PregelNode<StateT, UpdateT, ContextT>>,
        channels: &HashMap<String, Box<DynChannel>>,
        managed: &HashMap<String, Box<dyn ManagedValueSpec>>,
        step: usize,
    ) -> Result<Option<PregelExecutableTask<'a, StateT, UpdateT, ContextT>>, GraphError> {
        let Some(node) = nodes.get(&name) else {
            return Ok(None);
        };

        if !node.triggers.iter().any(|trigger| {
            channels
                .get(trigger)
                .is_some_and(|channel| channel.is_available())
        }) {
            return Ok(None);
        }

        let input = Self::proc_input(&name, node, channels, managed)?;
        let mut triggers = node.triggers.clone();
        triggers.sort();
        triggers.dedup();
        let id = Self::task_id(step, &name, &triggers);

        Ok(Some(PregelExecutableTask {
            name: name.clone(),
            input,
            bound: &node.bound,
            writes: Vec::new(),
            writers: &node.writers,
            triggers,
            id,
            path: vec!["pull".to_string(), name],
        }))
    }

    pub(crate) fn execute_pending_tasks(
        &mut self,
        context: &RuntimeContext<ContextT>,
    ) -> Result<Vec<PregelTaskWrites>, GraphError>
    where
        StateT: Send,
        UpdateT: Into<StateValue> + Send,
        ContextT: Sync,
    {
        let mut tasks = std::mem::take(&mut self.tasks)
            .into_values()
            .collect::<Vec<_>>();
        tasks.sort_by(|left, right| left.path.cmp(&right.path).then(left.id.cmp(&right.id)));

        let executed_tasks = match tasks.len() {
            0 => Vec::new(),
            1 => vec![Self::execute_task_inner(tasks.remove(0), context)?],
            _ => std::thread::scope(|scope| {
                let handles = tasks
                    .into_iter()
                    .map(|task| {
                        let name = task.name.clone();
                        let handle = scope.spawn(move || Self::execute_task_inner(task, context));
                        (name, handle)
                    })
                    .collect::<Vec<_>>();
                let mut executed_tasks = Vec::new();

                for (name, handle) in handles {
                    let task = match handle.join() {
                        Ok(result) => result?,
                        Err(_) => {
                            return Err(GraphError::PregelTaskFailed {
                                node: name,
                                message: "task panicked".to_string(),
                            });
                        }
                    };
                    executed_tasks.push(task);
                }

                Ok(executed_tasks)
            })?,
        };

        let writes = executed_tasks
            .iter()
            .map(PregelExecutableTask::to_writes)
            .collect::<Vec<_>>();
        self.tasks = executed_tasks
            .into_iter()
            .map(|task| (task.id.clone(), task))
            .collect();

        Ok(writes)
    }

    fn execute_task_inner(
        mut task: PregelExecutableTask<'a, StateT, UpdateT, ContextT>,
        context: &RuntimeContext<ContextT>,
    ) -> Result<PregelExecutableTask<'a, StateT, UpdateT, ContextT>, GraphError>
    where
        UpdateT: Into<StateValue>,
    {
        let output =
            (task.bound)(&task.input, context).map_err(|error| GraphError::PregelTaskFailed {
                node: task.name.clone(),
                message: error.to_string(),
            })?;

        let output = match output {
            NodeOutput::Update(update) => update.into(),
            NodeOutput::None => StateValue::Null,
            NodeOutput::Command(_) | NodeOutput::Commands(_) => {
                return Err(GraphError::UnsupportedPregelCommand);
            }
        };

        for writer in task.writers {
            task.writes
                .extend(writer.assemble(&output, true, &task.input, context)?);
        }

        Ok(task)
    }

    fn proc_input(
        name: &str,
        node: &PregelNode<StateT, UpdateT, ContextT>,
        channels: &HashMap<String, Box<DynChannel>>,
        managed: &HashMap<String, Box<dyn ManagedValueSpec>>,
    ) -> Result<StateT, GraphError> {
        let mut values = HashMap::new();

        for channel_name in &node.channels {
            if let Some(channel) = channels.get(channel_name) {
                if channel.is_available() {
                    values.insert(channel_name.clone(), channel.get()?);
                }
                continue;
            }

            if managed.contains_key(channel_name) {
                continue;
            }

            return Err(GraphError::UnknownPregelReadChannel {
                node: name.to_string(),
                channel: channel_name.clone(),
            });
        }

        let input = StateValue::Object(values);

        match &node.mapper {
            Some(mapper) => mapper(input),
            None => Ok(StateT::from(input)),
        }
    }

    fn task_id(step: usize, name: &str, triggers: &[String]) -> String {
        format!("pull:{step}:{name}:{}", triggers.join(","))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channel::BaseChannel;
    use crate::channel::channel_writer::{
        ChannelWriteEntry, ChannelWriteTupleEntry, ChannelWriteValue, ChannelWriterEntry,
    };
    use crate::channel::ephemeral_value::EphemeralValue;
    use crate::channel::last_value::LastValue;
    use crate::graph::node::NodeOutput;

    struct TestManagedValue;

    impl ManagedValueSpec for TestManagedValue {
        fn copy_box(&self) -> Box<dyn ManagedValueSpec> {
            Box::new(TestManagedValue)
        }
    }

    fn last_value(value: Option<StateValue>) -> Box<DynChannel> {
        let mut channel = LastValue::new();
        if let Some(value) = value {
            channel.update(vec![value]).unwrap();
        }
        Box::new(channel)
    }

    fn trigger(value: bool) -> Box<DynChannel> {
        let mut channel = EphemeralValue::new(false);
        if value {
            channel.update(vec![StateValue::Null]).unwrap();
        }
        Box::new(channel)
    }

    fn test_node(
        channels: Vec<&str>,
        triggers: Vec<&str>,
    ) -> PregelNode<StateValue, StateValue, ()> {
        PregelNode::new(
            channels.into_iter().map(str::to_string).collect(),
            triggers.into_iter().map(str::to_string).collect(),
            None,
            Vec::new(),
            Box::new(|state, _| Ok(NodeOutput::Update(state.clone()))),
        )
    }

    fn passthrough_writer(channel: &str) -> ChannelWriter<StateValue, ()> {
        ChannelWriter::new(vec![ChannelWriterEntry::Channel(ChannelWriteEntry {
            channel: channel.to_string(),
            value: ChannelWriteValue::Passthrough,
            skip_none: false,
            mapper: None,
        })])
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
    fn prepare_task_creates_pull_task_when_trigger_is_available() {
        let node = test_node(vec!["input"], vec!["trigger"]);
        let nodes = HashMap::from([("a".to_string(), node)]);
        let channels = HashMap::from([
            (
                "input".to_string(),
                last_value(Some(StateValue::String("value".to_string()))),
            ),
            ("trigger".to_string(), trigger(true)),
        ]);
        let manager = PregelTaskManager::<StateValue, StateValue, ()>::new();

        let task = manager
            .prepare_task("a".to_string(), &nodes, &channels, &HashMap::new(), 2)
            .unwrap()
            .unwrap();

        assert_eq!(task.name, "a");
        assert_eq!(task.id, "pull:2:a:trigger");
        assert_eq!(task.path, vec!["pull".to_string(), "a".to_string()]);
        assert_eq!(task.triggers, vec!["trigger".to_string()]);
        assert!(task.writes.is_empty());
        assert_eq!(task.writers.len(), 0);
        assert_eq!(
            task.input,
            StateValue::Object(HashMap::from([(
                "input".to_string(),
                StateValue::String("value".to_string())
            )]))
        );

        let context = RuntimeContext { context: () };
        let output = (task.bound)(&task.input, &context).unwrap();
        assert!(matches!(output, NodeOutput::Update(_)));
    }

    #[test]
    fn prepare_task_returns_none_when_trigger_is_unavailable() {
        let node = test_node(vec!["input"], vec!["trigger"]);
        let nodes = HashMap::from([("a".to_string(), node)]);
        let channels = HashMap::from([
            (
                "input".to_string(),
                last_value(Some(StateValue::String("value".to_string()))),
            ),
            ("trigger".to_string(), trigger(false)),
        ]);
        let manager = PregelTaskManager::<StateValue, StateValue, ()>::new();

        let task = manager
            .prepare_task("a".to_string(), &nodes, &channels, &HashMap::new(), 2)
            .unwrap();

        assert!(task.is_none());
    }

    #[test]
    fn prepare_task_assembles_only_available_regular_channels() {
        let node = test_node(vec!["left", "right", "empty"], vec!["trigger"]);
        let nodes = HashMap::from([("a".to_string(), node)]);
        let channels = HashMap::from([
            (
                "left".to_string(),
                last_value(Some(StateValue::Number(1.0))),
            ),
            (
                "right".to_string(),
                last_value(Some(StateValue::Number(2.0))),
            ),
            ("empty".to_string(), last_value(None)),
            ("trigger".to_string(), trigger(true)),
        ]);
        let manager = PregelTaskManager::<StateValue, StateValue, ()>::new();

        let task = manager
            .prepare_task("a".to_string(), &nodes, &channels, &HashMap::new(), 0)
            .unwrap()
            .unwrap();

        assert_eq!(
            task.input,
            StateValue::Object(HashMap::from([
                ("left".to_string(), StateValue::Number(1.0)),
                ("right".to_string(), StateValue::Number(2.0)),
            ]))
        );
    }

    #[test]
    fn prepare_task_applies_node_mapper_to_raw_object_input() {
        let node = PregelNode::new(
            vec!["value".to_string()],
            vec!["trigger".to_string()],
            Some(Box::new(|value| match value {
                StateValue::Object(values) => match values.get("value") {
                    Some(StateValue::Number(value)) => Ok(StateValue::Number(value + 1.0)),
                    other => Err(GraphError::InvalidPregelInput(format!(
                        "unexpected value: {other:?}"
                    ))),
                },
                other => Err(GraphError::InvalidPregelInput(format!(
                    "expected object, got {other:?}"
                ))),
            })),
            Vec::new(),
            Box::new(|input, _| Ok(NodeOutput::Update(input.clone()))),
        );
        let nodes = HashMap::from([("a".to_string(), node)]);
        let channels = HashMap::from([
            (
                "value".to_string(),
                last_value(Some(StateValue::Number(2.0))),
            ),
            ("trigger".to_string(), trigger(true)),
        ]);
        let manager = PregelTaskManager::<StateValue, StateValue, ()>::new();

        let task = manager
            .prepare_task("a".to_string(), &nodes, &channels, &HashMap::new(), 0)
            .unwrap()
            .unwrap();

        assert_eq!(task.input, StateValue::Number(3.0));
    }

    #[test]
    fn prepare_tasks_uses_updated_channels_to_limit_candidates() {
        let nodes = HashMap::from([
            ("a".to_string(), test_node(vec!["left"], vec!["to_a"])),
            ("b".to_string(), test_node(vec!["right"], vec!["to_b"])),
        ]);
        let channels = HashMap::from([
            (
                "left".to_string(),
                last_value(Some(StateValue::Number(1.0))),
            ),
            (
                "right".to_string(),
                last_value(Some(StateValue::Number(2.0))),
            ),
            ("to_a".to_string(), trigger(true)),
            ("to_b".to_string(), trigger(true)),
        ]);
        let trigger_to_nodes = HashMap::from([
            ("to_a".to_string(), vec!["a".to_string()]),
            ("to_b".to_string(), vec!["b".to_string()]),
        ]);
        let updated_channels = HashSet::from(["to_b".to_string()]);
        let mut manager = PregelTaskManager::<StateValue, StateValue, ()>::new();

        let task_ids = {
            let tasks = manager
                .prepare_tasks(
                    &nodes,
                    &channels,
                    &HashMap::new(),
                    &trigger_to_nodes,
                    Some(&updated_channels),
                    1,
                )
                .unwrap();
            tasks.iter().map(|task| task.id.clone()).collect::<Vec<_>>()
        };

        assert_eq!(task_ids, vec!["pull:1:b:to_b".to_string()]);
        assert_eq!(manager.tasks.len(), 1);
        assert!(manager.tasks.contains_key("pull:1:b:to_b"));
    }

    #[test]
    fn prepare_tasks_scans_all_nodes_without_updated_channels() {
        let nodes = HashMap::from([
            ("b".to_string(), test_node(vec!["right"], vec!["to_b"])),
            ("a".to_string(), test_node(vec!["left"], vec!["to_a"])),
        ]);
        let channels = HashMap::from([
            (
                "left".to_string(),
                last_value(Some(StateValue::Number(1.0))),
            ),
            (
                "right".to_string(),
                last_value(Some(StateValue::Number(2.0))),
            ),
            ("to_a".to_string(), trigger(true)),
            ("to_b".to_string(), trigger(true)),
        ]);
        let mut manager = PregelTaskManager::<StateValue, StateValue, ()>::new();

        let task_ids = {
            let tasks = manager
                .prepare_tasks(&nodes, &channels, &HashMap::new(), &HashMap::new(), None, 1)
                .unwrap();
            tasks.iter().map(|task| task.id.clone()).collect::<Vec<_>>()
        };

        assert_eq!(
            task_ids,
            vec!["pull:1:a:to_a".to_string(), "pull:1:b:to_b".to_string()]
        );
        assert_eq!(manager.tasks.len(), 2);
    }

    #[test]
    fn prepare_task_allows_managed_read_channels_without_injecting_values() {
        let node = test_node(vec!["state", "runtime"], vec!["trigger"]);
        let nodes = HashMap::from([("a".to_string(), node)]);
        let channels = HashMap::from([
            (
                "state".to_string(),
                last_value(Some(StateValue::String("kept".to_string()))),
            ),
            ("trigger".to_string(), trigger(true)),
        ]);
        let managed = HashMap::from([(
            "runtime".to_string(),
            Box::new(TestManagedValue) as Box<dyn ManagedValueSpec>,
        )]);
        let manager = PregelTaskManager::<StateValue, StateValue, ()>::new();

        let task = manager
            .prepare_task("a".to_string(), &nodes, &channels, &managed, 0)
            .unwrap()
            .unwrap();

        assert_eq!(
            task.input,
            StateValue::Object(HashMap::from([(
                "state".to_string(),
                StateValue::String("kept".to_string())
            )]))
        );
    }

    #[test]
    fn prepare_task_rejects_unknown_read_channel() {
        let node = test_node(vec!["missing"], vec!["trigger"]);
        let nodes = HashMap::from([("a".to_string(), node)]);
        let channels = HashMap::from([("trigger".to_string(), trigger(true))]);
        let manager = PregelTaskManager::<StateValue, StateValue, ()>::new();

        let error =
            match manager.prepare_task("a".to_string(), &nodes, &channels, &HashMap::new(), 0) {
                Ok(_) => panic!("prepare_task should reject unknown read channel"),
                Err(error) => error,
            };

        assert!(matches!(
            error,
            GraphError::UnknownPregelReadChannel { node, channel }
                if node == "a" && channel == "missing"
        ));
    }

    #[test]
    fn execute_task_runs_bound_and_assembles_writers_in_order() {
        let node = PregelNode::new(
            vec!["input".to_string()],
            vec!["trigger".to_string()],
            None,
            vec![passthrough_writer("left"), passthrough_writer("right")],
            Box::new(|_, _| Ok(NodeOutput::Update(StateValue::Number(7.0)))),
        );
        let nodes = HashMap::from([("a".to_string(), node)]);
        let channels = HashMap::from([
            ("input".to_string(), last_value(Some(StateValue::Null))),
            ("trigger".to_string(), trigger(true)),
        ]);
        let manager = PregelTaskManager::<StateValue, StateValue, ()>::new();
        let task = manager
            .prepare_task("a".to_string(), &nodes, &channels, &HashMap::new(), 0)
            .unwrap()
            .unwrap();
        let context = RuntimeContext { context: () };

        let task = PregelTaskManager::execute_task_inner(task, &context).unwrap();

        assert_eq!(
            task.writes,
            vec![
                ("left".to_string(), StateValue::Number(7.0)),
                ("right".to_string(), StateValue::Number(7.0)),
            ]
        );
    }

    #[test]
    fn execute_task_passes_task_input_and_context_to_executable_writer() {
        let writer = ChannelWriter::new(vec![ChannelWriterEntry::Executable(Box::new(
            |state: &StateValue, context: &RuntimeContext<i64>| {
                let StateValue::Object(values) = state else {
                    return Err(GraphError::InvalidPregelInput(format!(
                        "expected object input, got {state:?}"
                    )));
                };
                let Some(StateValue::Number(seed)) = values.get("input") else {
                    return Err(GraphError::InvalidPregelInput(
                        "missing input seed".to_string(),
                    ));
                };

                Ok(vec![ChannelWriteEntry {
                    channel: "seen".to_string(),
                    value: ChannelWriteValue::Value(StateValue::Number(
                        (context.context + *seed as i64) as f64,
                    )),
                    skip_none: false,
                    mapper: None,
                }])
            },
        ))]);
        let node = PregelNode::new(
            vec!["input".to_string()],
            vec!["trigger".to_string()],
            None,
            vec![writer],
            Box::new(|_, _| Ok(NodeOutput::None)),
        );
        let nodes = HashMap::from([("a".to_string(), node)]);
        let channels = HashMap::from([
            (
                "input".to_string(),
                last_value(Some(StateValue::Number(4.0))),
            ),
            ("trigger".to_string(), trigger(true)),
        ]);
        let manager = PregelTaskManager::<StateValue, StateValue, i64>::new();
        let task = manager
            .prepare_task("a".to_string(), &nodes, &channels, &HashMap::new(), 0)
            .unwrap()
            .unwrap();
        let context = RuntimeContext { context: 2 };

        let task = PregelTaskManager::execute_task_inner(task, &context).unwrap();

        assert_eq!(context.context, 2);
        assert_eq!(
            task.writes,
            vec![("seen".to_string(), StateValue::Number(6.0))]
        );
    }

    #[test]
    fn execute_task_keeps_existing_writes_and_appends_new_writes() {
        let node = PregelNode::new(
            vec!["input".to_string()],
            vec!["trigger".to_string()],
            None,
            vec![fixed_writer("new", StateValue::String("write".to_string()))],
            Box::new(|_, _| Ok(NodeOutput::None)),
        );
        let nodes = HashMap::from([("a".to_string(), node)]);
        let channels = HashMap::from([
            ("input".to_string(), last_value(Some(StateValue::Null))),
            ("trigger".to_string(), trigger(true)),
        ]);
        let manager = PregelTaskManager::<StateValue, StateValue, ()>::new();
        let mut task = manager
            .prepare_task("a".to_string(), &nodes, &channels, &HashMap::new(), 0)
            .unwrap()
            .unwrap();
        task.writes
            .push(("old".to_string(), StateValue::String("kept".to_string())));
        let context = RuntimeContext { context: () };

        let task = PregelTaskManager::execute_task_inner(task, &context).unwrap();

        assert_eq!(
            task.writes,
            vec![
                ("old".to_string(), StateValue::String("kept".to_string())),
                ("new".to_string(), StateValue::String("write".to_string())),
            ]
        );
    }

    #[test]
    fn execute_task_returns_null_output_for_none() {
        let state_writer =
            ChannelWriter::new(vec![ChannelWriterEntry::Tuple(ChannelWriteTupleEntry {
                mapper: Box::new(|value| match value {
                    StateValue::Null => Ok(Vec::new()),
                    other => Err(GraphError::InvalidChannelUpdate(format!(
                        "expected null, got {other:?}"
                    ))),
                }),
            })]);
        let node = PregelNode::new(
            vec!["input".to_string()],
            vec!["trigger".to_string()],
            None,
            vec![
                state_writer,
                fixed_writer("next", StateValue::String("ready".to_string())),
            ],
            Box::new(|_, _| Ok(NodeOutput::None)),
        );
        let nodes = HashMap::from([("a".to_string(), node)]);
        let channels = HashMap::from([
            ("input".to_string(), last_value(Some(StateValue::Null))),
            ("trigger".to_string(), trigger(true)),
        ]);
        let manager = PregelTaskManager::<StateValue, StateValue, ()>::new();
        let task = manager
            .prepare_task("a".to_string(), &nodes, &channels, &HashMap::new(), 0)
            .unwrap()
            .unwrap();
        let context = RuntimeContext { context: () };

        let task = PregelTaskManager::execute_task_inner(task, &context).unwrap();

        assert_eq!(
            task.writes,
            vec![("next".to_string(), StateValue::String("ready".to_string()))]
        );
    }

    #[test]
    fn execute_task_rejects_command_outputs() {
        let node = PregelNode::new(
            vec!["input".to_string()],
            vec!["trigger".to_string()],
            None,
            Vec::new(),
            Box::new(|_, _| Ok(NodeOutput::Commands(Vec::new()))),
        );
        let nodes = HashMap::from([("a".to_string(), node)]);
        let channels = HashMap::from([
            ("input".to_string(), last_value(Some(StateValue::Null))),
            ("trigger".to_string(), trigger(true)),
        ]);
        let manager = PregelTaskManager::<StateValue, StateValue, ()>::new();
        let task = manager
            .prepare_task("a".to_string(), &nodes, &channels, &HashMap::new(), 0)
            .unwrap()
            .unwrap();
        let context = RuntimeContext { context: () };

        let error = match PregelTaskManager::execute_task_inner(task, &context) {
            Ok(_) => panic!("execute_task should reject command outputs"),
            Err(error) => error,
        };

        assert!(matches!(error, GraphError::UnsupportedPregelCommand));
    }

    #[test]
    fn execute_task_wraps_bound_errors_with_node_name() {
        let node = PregelNode::new(
            vec!["input".to_string()],
            vec!["trigger".to_string()],
            None,
            Vec::new(),
            Box::new(|_, _| Err(GraphError::InvalidPregelInput("bad".to_string()))),
        );
        let nodes = HashMap::from([("a".to_string(), node)]);
        let channels = HashMap::from([
            ("input".to_string(), last_value(Some(StateValue::Null))),
            ("trigger".to_string(), trigger(true)),
        ]);
        let manager = PregelTaskManager::<StateValue, StateValue, ()>::new();
        let task = manager
            .prepare_task("a".to_string(), &nodes, &channels, &HashMap::new(), 0)
            .unwrap()
            .unwrap();
        let context = RuntimeContext { context: () };

        let error = match PregelTaskManager::execute_task_inner(task, &context) {
            Ok(_) => panic!("execute_task should wrap bound errors"),
            Err(error) => error,
        };

        assert!(matches!(
            error,
            GraphError::PregelTaskFailed { node, message }
                if node == "a" && message == "invalid Pregel input: bad"
        ));
    }

    #[test]
    fn execute_task_propagates_writer_errors() {
        let writer = ChannelWriter::new(vec![ChannelWriterEntry::Tuple(ChannelWriteTupleEntry {
            mapper: Box::new(|_| Err(GraphError::InvalidChannelUpdate("bad writer".to_string()))),
        })]);
        let node = PregelNode::new(
            vec!["input".to_string()],
            vec!["trigger".to_string()],
            None,
            vec![writer],
            Box::new(|_, _| Ok(NodeOutput::Update(StateValue::Null))),
        );
        let nodes = HashMap::from([("a".to_string(), node)]);
        let channels = HashMap::from([
            ("input".to_string(), last_value(Some(StateValue::Null))),
            ("trigger".to_string(), trigger(true)),
        ]);
        let manager = PregelTaskManager::<StateValue, StateValue, ()>::new();
        let task = manager
            .prepare_task("a".to_string(), &nodes, &channels, &HashMap::new(), 0)
            .unwrap()
            .unwrap();
        let context = RuntimeContext { context: () };

        let error = match PregelTaskManager::execute_task_inner(task, &context) {
            Ok(_) => panic!("execute_task should propagate writer errors"),
            Err(error) => error,
        };

        assert!(
            matches!(error, GraphError::InvalidChannelUpdate(message) if message == "bad writer")
        );
    }

    #[test]
    fn execute_pending_tasks_returns_empty_without_tasks() {
        let mut manager = PregelTaskManager::<StateValue, StateValue, ()>::new();
        let context = RuntimeContext { context: () };

        let writes = manager.execute_pending_tasks(&context).unwrap();

        assert!(writes.is_empty());
        assert!(manager.tasks.is_empty());
    }

    #[test]
    fn execute_pending_tasks_runs_tasks_and_returns_writes_in_stable_order() {
        let nodes = HashMap::from([
            (
                "b".to_string(),
                PregelNode::new(
                    vec!["input".to_string()],
                    vec!["trigger".to_string()],
                    None,
                    vec![fixed_writer("out", StateValue::String("b".to_string()))],
                    Box::new(|_, _| Ok(NodeOutput::None)),
                ),
            ),
            (
                "a".to_string(),
                PregelNode::new(
                    vec!["input".to_string()],
                    vec!["trigger".to_string()],
                    None,
                    vec![fixed_writer("out", StateValue::String("a".to_string()))],
                    Box::new(|_, _| Ok(NodeOutput::None)),
                ),
            ),
        ]);
        let channels = HashMap::from([
            ("input".to_string(), last_value(Some(StateValue::Null))),
            ("trigger".to_string(), trigger(true)),
        ]);
        let mut manager = PregelTaskManager::<StateValue, StateValue, ()>::new();
        {
            let tasks = manager
                .prepare_tasks(&nodes, &channels, &HashMap::new(), &HashMap::new(), None, 0)
                .unwrap();
            assert_eq!(tasks.len(), 2);
        }
        let context = RuntimeContext { context: () };

        let writes = manager.execute_pending_tasks(&context).unwrap();

        assert_eq!(
            writes
                .iter()
                .map(|writes| writes.name.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b"]
        );
        assert_eq!(
            writes
                .iter()
                .map(|writes| writes.writes.clone())
                .collect::<Vec<_>>(),
            vec![
                vec![("out".to_string(), StateValue::String("a".to_string()))],
                vec![("out".to_string(), StateValue::String("b".to_string()))],
            ]
        );
        assert_eq!(manager.tasks.len(), 2);
    }
}
