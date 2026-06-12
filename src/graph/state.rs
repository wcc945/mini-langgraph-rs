use crate::channel::DynChannel;
use crate::channel::last_value::LastValue;
use crate::error::GraphError;
use crate::graph::branch::{BranchPathFn, BranchSpec};
use crate::graph::compiled::CompiledStateGraph;
use crate::graph::consts::{END, RESERVED, START};
use crate::graph::node::{NodeFn, StateNodeSpec};
use crate::graph::schema::StateSchema;
use crate::graph::waiting_edge::WaitingEdgeSpec;
use crate::managed::ManagedValueSpec;
use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;

pub struct StateGraph<StateT, UpdateT, ContextT = (), InputT = StateT, OutputT = StateT> {
    nodes: HashMap<String, StateNodeSpec<StateT, UpdateT, ContextT>>,
    edges: HashSet<(String, String)>,
    branches: HashMap<String, HashMap<String, BranchSpec<StateT, ContextT>>>,
    waiting_edges: HashSet<WaitingEdgeSpec>,
    pub channels: HashMap<String, Box<DynChannel>>,
    pub managed: HashMap<String, Box<dyn ManagedValueSpec>>,
    _marker: PhantomData<(InputT, OutputT)>,
}

pub trait IntoEdgeStarts {
    fn into_edge_starts(self) -> Vec<String>;
}

impl IntoEdgeStarts for String {
    fn into_edge_starts(self) -> Vec<String> {
        vec![self]
    }
}

impl IntoEdgeStarts for &str {
    fn into_edge_starts(self) -> Vec<String> {
        vec![self.to_string()]
    }
}

impl<S> IntoEdgeStarts for Vec<S>
where
    S: Into<String>,
{
    fn into_edge_starts(self) -> Vec<String> {
        self.into_iter().map(Into::into).collect()
    }
}

impl<S, const N: usize> IntoEdgeStarts for [S; N]
where
    S: Into<String>,
{
    fn into_edge_starts(self) -> Vec<String> {
        self.into_iter().map(Into::into).collect()
    }
}

impl<StateT, UpdateT, ContextT, InputT, OutputT> Default for StateGraph<StateT, UpdateT, ContextT, InputT, OutputT> {
    fn default() -> Self {
        Self::new()
    }
}

impl<StateT, UpdateT, ContextT, InputT, OutputT>
    StateGraph<StateT, UpdateT, ContextT, InputT, OutputT>
{
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            edges: HashSet::new(),
            branches: HashMap::new(),
            waiting_edges: HashSet::new(),
            channels: HashMap::new(),
            managed: HashMap::new(),
            _marker: PhantomData,
        }
    }

    pub fn with_channels(channels: impl IntoIterator<Item = impl Into<String>>) -> Self {
        let mut graph = Self::new();
        graph.channels = channels
            .into_iter()
            .map(|channel| {
                (
                    channel.into(),
                    Box::new(LastValue::new()) as Box<DynChannel>,
                )
            })
            .collect();
        graph
    }
}

#[allow(private_bounds)]
impl<StateT, UpdateT, ContextT, InputT, OutputT>
    StateGraph<StateT, UpdateT, ContextT, InputT, OutputT>
where
    StateT: StateSchema,
{
    pub fn with_schema() -> Self {
        let mut graph = Self::new();
        graph.channels = StateT::channels();
        graph.managed = StateT::managed();
        graph
    }
}

impl<StateT, UpdateT, ContextT, InputT, OutputT>
    StateGraph<StateT, UpdateT, ContextT, InputT, OutputT>
{
    pub fn add_node(
        &mut self,
        node: impl Into<String>,
        action: NodeFn<StateT, UpdateT, ContextT>,
    ) -> Result<&mut Self, GraphError> {
        let node = node.into();

        if self.nodes.contains_key(&node) {
            return Err(GraphError::DuplicateNode(node));
        }

        if [START, END].contains(&node.as_str()) {
            return Err(GraphError::ReservedNodeName(node));
        }

        if let Some(character) = RESERVED.iter().find(|character| node.contains(*character)) {
            return Err(GraphError::ReservedNodeCharacter {
                node,
                character: (*character).to_string(),
            });
        }

        self.nodes.insert(node, StateNodeSpec::new(action));

        Ok(self)
    }

    pub fn add_edge(
        &mut self,
        start: impl IntoEdgeStarts,
        end: impl Into<String>,
    ) -> Result<&mut Self, GraphError> {
        let start = start.into_edge_starts();
        let end = end.into();

        if start.is_empty() {
            return Err(GraphError::EmptyEdgeStarts);
        }

        if start.len() > 1 && start.iter().any(|st| st == START) {
            return Err(GraphError::StartCannotBeWaitingEdgeStart);
        }

        if end == START {
            return Err(GraphError::StartCannotBeEnd);
        }

        if end != END && !self.nodes.contains_key(&end) {
            return Err(GraphError::UnknownNode(end));
        }

        for st in start.iter() {
            if st == END {
                return Err(GraphError::EndCannotBeStart);
            }

            if st != START && !self.nodes.contains_key(st) {
                return Err(GraphError::UnknownNode(st.clone()));
            }
        }

        if start.len() == 1 {
            let start_node = start.into_iter().next().unwrap();
            self.edges.insert((start_node, end));
        } else {
            self.waiting_edges.insert(WaitingEdgeSpec::new(start, end));
        }
        Ok(self)
    }

    pub fn add_conditional_edges(
        &mut self,
        start: impl Into<String>,
        name: impl Into<String>,
        path: BranchPathFn<StateT, ContextT>,
        ends: HashMap<String, String>,
    ) -> Result<&mut Self, GraphError> {
        let start = start.into();
        let name = name.into();

        if start != START && !self.nodes.contains_key(&start) {
            return Err(GraphError::UnknownNode(start));
        }

        let branches = self.branches.entry(start.clone()).or_default();

        if branches.contains_key(&name) {
            return Err(GraphError::DuplicateBranch {
                node: start,
                branch: name,
            });
        }

        branches.insert(name, BranchSpec::new(path, Some(ends)));

        Ok(self)
    }

    pub fn add_sequence<Nodes, Name>(&mut self, nodes: Nodes) -> Result<&mut Self, GraphError>
    where
        Nodes: IntoIterator<Item = (Name, NodeFn<StateT, UpdateT, ContextT>)>,
        Name: Into<String>,
    {
        let mut previous_name: Option<String> = None;
        let mut has_nodes = false;

        for (name, action) in nodes {
            has_nodes = true;

            let name = name.into();
            self.add_node(name.clone(), action)?;

            if let Some(previous_name) = previous_name {
                self.add_edge(previous_name, name.clone())?;
            }

            previous_name = Some(name);
        }

        if !has_nodes {
            return Err(GraphError::EmptySequence);
        }

        Ok(self)
    }

    pub fn set_entry_point(&mut self, key: impl Into<String>) -> Result<&mut Self, GraphError> {
        self.add_edge(START, key)
    }

    pub fn set_conditional_entry_point(
        &mut self,
        name: impl Into<String>,
        path: BranchPathFn<StateT, ContextT>,
        ends: HashMap<String, String>,
    ) -> Result<&mut Self, GraphError> {
        self.add_conditional_edges(START, name, path, ends)
    }

    pub fn set_finish_point(&mut self, key: impl Into<String>) -> Result<&mut Self, GraphError> {
        self.add_edge(key.into(), END)
    }

    pub fn compile(
        self,
    ) -> Result<CompiledStateGraph<StateT, UpdateT, ContextT, InputT, OutputT>, GraphError>
    where
        StateT: 'static,
        ContextT: 'static,
    {
        self.validate()?;

        let StateGraph {
            nodes,
            edges,
            branches,
            waiting_edges,
            channels,
            managed,
            _marker: _,
        } = self;

        let output_channels = if channels.is_empty() {
            vec![START.to_string()]
        } else {
            channels.keys().cloned().collect()
        };

        let mut compiled = CompiledStateGraph::new(channels, managed, output_channels);

        compiled.attach_node(START.to_string(), None);

        for (name, node) in nodes {
            compiled.attach_node(name, Some(node));
        }
        for (start, end) in &edges {
            compiled.attach_edge(vec![start.clone()], end);
        }
        for waiting_edge in &waiting_edges {
            compiled.attach_edge(waiting_edge.starts.clone(), &waiting_edge.end);
        }
        for (start, branches) in branches {
            for (name, branch) in branches {
                compiled.attach_branch(&start, &name, branch)?;
            }
        }

        compiled.validate()
    }

    /// 校验当前 builder 中保存的图结构是否合法。
    ///
    /// MVP 版只检查核心构图约束：所有边和条件分支的起点必须是已注册节点或 `START`，
    /// 所有终点必须是已注册节点或 `END`，并且图必须至少有一个从 `START` 出发的入口。
    pub(crate) fn validate(&self) -> Result<(), GraphError> {
        let mut all_sources = HashSet::new();

        for (start, _) in &self.edges {
            all_sources.insert(start.clone());
        }
        for waiting_edge in &self.waiting_edges {
            for start in &waiting_edge.starts {
                all_sources.insert(start.clone());
            }
        }
        for start in self.branches.keys() {
            all_sources.insert(start.clone());
        }

        for source in &all_sources {
            if source != START && !self.nodes.contains_key(source) {
                return Err(GraphError::UnknownEdgeSource(source.clone()));
            }
        }

        if !all_sources.contains(START) {
            return Err(GraphError::MissingEntrypoint);
        }

        let mut all_targets = HashSet::new();

        for (_, end) in &self.edges {
            all_targets.insert(end.clone());
        }
        for waiting_edge in &self.waiting_edges {
            all_targets.insert(waiting_edge.end.clone());
        }
        for (start, branches) in &self.branches {
            for (name, branch) in branches {
                if let Some(ends) = &branch.ends {
                    for end in ends.values() {
                        if end != END && !self.nodes.contains_key(end) {
                            return Err(GraphError::UnknownBranchTarget {
                                node: start.clone(),
                                branch: name.clone(),
                                target: end.clone(),
                            });
                        }
                        all_targets.insert(end.clone());
                    }
                } else {
                    all_targets.insert(END.to_string());
                    for node in self.nodes.keys() {
                        if node != start {
                            all_targets.insert(node.clone());
                        }
                    }
                }
            }
        }

        for target in &all_targets {
            if target != END && !self.nodes.contains_key(target) {
                return Err(GraphError::UnknownEdgeTarget(target.clone()));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channel::StateValue;
    use crate::channel::last_value::LastValue;
    use crate::graph::node::NodeOutput;
    use crate::managed::ManagedValueSpec;
    use crate::pregel::pregel::StreamMode;
    use crate::runtime::RuntimeContext;

    struct TestManagedValue;

    impl ManagedValueSpec for TestManagedValue {
        fn copy_box(&self) -> Box<dyn ManagedValueSpec> {
            Box::new(TestManagedValue)
        }
    }

    struct TestSchema;

    impl StateSchema for TestSchema {
        fn channels() -> HashMap<String, Box<DynChannel>> {
            HashMap::from([
                (
                    "left".to_string(),
                    Box::new(LastValue::new()) as Box<DynChannel>,
                ),
                (
                    "right".to_string(),
                    Box::new(LastValue::new()) as Box<DynChannel>,
                ),
            ])
        }

        fn managed() -> HashMap<String, Box<dyn ManagedValueSpec>> {
            HashMap::from([(
                "runtime".to_string(),
                Box::new(TestManagedValue) as Box<dyn ManagedValueSpec>,
            )])
        }
    }

    fn noop_node() -> NodeFn<i32, i32, ()> {
        Box::new(|_, _| Ok(NodeOutput::None))
    }

    fn route_path() -> BranchPathFn<i32, ()> {
        Box::new(|_, _| Some("next".to_string()))
    }

    fn expect_compile_error<T>(result: Result<T, GraphError>) -> GraphError {
        match result {
            Err(error) => error,
            Ok(_) => panic!("compile should fail"),
        }
    }

    #[test]
    fn add_sequence_adds_nodes_and_edges_in_order() {
        let mut graph: StateGraph<i32, i32> = StateGraph::new();

        graph
            .add_sequence([("a", noop_node()), ("b", noop_node())])
            .unwrap();

        assert!(graph.nodes.contains_key("a"));
        assert!(graph.nodes.contains_key("b"));
        assert!(graph.edges.contains(&("a".to_string(), "b".to_string())));
    }

    #[test]
    fn add_sequence_rejects_empty_sequence() {
        let mut graph: StateGraph<i32, i32> = StateGraph::new();
        let nodes: Vec<(&str, NodeFn<i32, i32, ()>)> = Vec::new();

        assert!(graph.add_sequence(nodes).is_err());
    }

    #[test]
    fn set_entry_and_finish_points_add_start_and_end_edges() {
        let mut graph: StateGraph<i32, i32> = StateGraph::new();

        graph.add_node("a", noop_node()).unwrap();
        graph.set_entry_point("a").unwrap();
        graph.set_finish_point("a").unwrap();

        assert!(graph.edges.contains(&(START.to_string(), "a".to_string())));
        assert!(graph.edges.contains(&("a".to_string(), END.to_string())));
    }

    #[test]
    fn set_conditional_entry_point_adds_start_branch() {
        let mut graph: StateGraph<i32, i32> = StateGraph::new();
        let ends = HashMap::from([("next".to_string(), "a".to_string())]);

        graph
            .set_conditional_entry_point("route", route_path(), ends)
            .unwrap();

        assert!(
            graph
                .branches
                .get(START)
                .is_some_and(|branches| branches.contains_key("route"))
        );
    }

    #[test]
    fn validate_accepts_graph_with_entrypoint_and_finish_point() {
        let mut graph: StateGraph<i32, i32> = StateGraph::new();

        graph.add_node("a", noop_node()).unwrap();
        graph.set_entry_point("a").unwrap();
        graph.set_finish_point("a").unwrap();

        assert!(graph.validate().is_ok());
    }

    #[test]
    fn validate_rejects_graph_without_entrypoint() {
        let mut graph: StateGraph<i32, i32> = StateGraph::new();

        graph.add_node("a", noop_node()).unwrap();
        graph.add_node("b", noop_node()).unwrap();
        graph.add_edge("a", "b").unwrap();

        assert!(graph.validate().is_err());
    }

    #[test]
    fn validate_rejects_unknown_conditional_branch_target() {
        let mut graph: StateGraph<i32, i32> = StateGraph::new();
        let ends = HashMap::from([("next".to_string(), "missing".to_string())]);

        graph
            .set_conditional_entry_point("route", route_path(), ends)
            .unwrap();

        assert!(graph.validate().is_err());
    }

    #[test]
    fn compile_consumes_builder_and_returns_compiled_graph() {
        let mut graph: StateGraph<i32, i32> = StateGraph::new();

        graph.add_node("a", noop_node()).unwrap();
        graph.set_entry_point("a").unwrap();
        graph.set_finish_point("a").unwrap();

        let compiled = graph.compile().unwrap();

        assert!(compiled.pregel.nodes.contains_key("a"));
    }

    #[test]
    fn compile_runs_state_graph_validation() {
        let mut graph: StateGraph<i32, i32> = StateGraph::new();

        graph.add_node("a", noop_node()).unwrap();

        let error = expect_compile_error(graph.compile());

        assert!(matches!(error, GraphError::MissingEntrypoint));
    }

    #[test]
    fn compile_entrypoint_adds_writer_to_start_node() {
        let mut graph: StateGraph<i32, i32> = StateGraph::new();

        graph.add_node("a", noop_node()).unwrap();
        graph.set_entry_point("a").unwrap();
        graph.set_finish_point("a").unwrap();

        let compiled = graph.compile().unwrap();
        let start = compiled.pregel.nodes.get(START).unwrap();
        let node = compiled.pregel.nodes.get("a").unwrap();
        let context = RuntimeContext::new(());
        let writes = start.writers[0]
            .assemble(&StateValue::Null, false, &0, &context)
            .unwrap();

        assert_eq!(start.triggers, vec![START.to_string()]);
        assert_eq!(writes, vec![("branch:to:a".to_string(), StateValue::Null)]);
        assert_eq!(node.triggers, vec!["branch:to:a".to_string()]);
        assert!(compiled.pregel.channels.contains_key(START));
    }

    #[test]
    fn compile_attach_node_builds_branch_trigger_channel() {
        let mut graph: StateGraph<i32, i32> = StateGraph::new();

        graph.add_node("a", noop_node()).unwrap();
        graph.set_entry_point("a").unwrap();
        graph.set_finish_point("a").unwrap();

        let compiled = graph.compile().unwrap();
        let node = compiled.pregel.nodes.get("a").unwrap();

        assert!(node.triggers.contains(&"branch:to:a".to_string()));
        assert!(compiled.pregel.channels.contains_key("branch:to:a"));
    }

    #[test]
    fn compile_attach_node_reads_state_channels_and_managed_values() {
        let mut graph: StateGraph<i32, i32> = StateGraph::new();

        graph
            .channels
            .insert("left".to_string(), Box::new(LastValue::new()));
        graph
            .channels
            .insert("right".to_string(), Box::new(LastValue::new()));
        graph
            .managed
            .insert("runtime".to_string(), Box::new(TestManagedValue));
        graph.add_node("a", noop_node()).unwrap();
        graph.set_entry_point("a").unwrap();
        graph.set_finish_point("a").unwrap();

        let compiled = graph.compile().unwrap();
        let node = compiled.pregel.nodes.get("a").unwrap();

        assert_eq!(
            node.channels,
            vec![
                "left".to_string(),
                "right".to_string(),
                "runtime".to_string()
            ]
        );
    }

    #[test]
    fn compile_attach_node_installs_state_writer() {
        let mut graph: StateGraph<i32, i32> = StateGraph::new();

        graph
            .channels
            .insert("value".to_string(), Box::new(LastValue::new()));
        graph.add_node("a", noop_node()).unwrap();
        graph.set_entry_point("a").unwrap();
        graph.set_finish_point("a").unwrap();

        let compiled = graph.compile().unwrap();
        let node = compiled.pregel.nodes.get("a").unwrap();

        assert_eq!(node.writers.len(), 1);
    }

    #[test]
    fn compile_sets_stream_channels_to_output_channels() {
        let mut graph: StateGraph<i32, i32> = StateGraph::new();

        graph
            .channels
            .insert("value".to_string(), Box::new(LastValue::new()));
        graph.add_node("a", noop_node()).unwrap();
        graph.set_entry_point("a").unwrap();
        graph.set_finish_point("a").unwrap();

        let compiled = graph.compile().unwrap();

        assert_eq!(
            compiled.pregel.stream_channels,
            Some(compiled.pregel.output_channels.clone())
        );
    }

    #[test]
    fn compiled_graph_invoke_returns_final_state_channel() {
        let mut graph: StateGraph<StateValue, StateValue> = StateGraph::new();
        graph
            .channels
            .insert("value".to_string(), Box::new(LastValue::new()));
        graph
            .add_node(
                "write",
                Box::new(|_, _| {
                    Ok(NodeOutput::Update(StateValue::Object(HashMap::from([(
                        "value".to_string(),
                        StateValue::String("done".to_string()),
                    )]))))
                }),
            )
            .unwrap();
        graph.set_entry_point("write").unwrap();
        graph.set_finish_point("write").unwrap();
        let compiled = graph.compile().unwrap();

        let output = compiled
            .invoke(Some(StateValue::Null), RuntimeContext::default())
            .unwrap();

        assert_eq!(output, StateValue::String("done".to_string()));
    }

    #[tokio::test]
    async fn compiled_graph_stream_with_updates_returns_node_update() {
        let mut graph: StateGraph<StateValue, StateValue> = StateGraph::new();
        graph
            .channels
            .insert("value".to_string(), Box::new(LastValue::new()));
        graph
            .add_node(
                "write",
                Box::new(|_, _| {
                    Ok(NodeOutput::Update(StateValue::Object(HashMap::from([(
                        "value".to_string(),
                        StateValue::Number(1.0),
                    )]))))
                }),
            )
            .unwrap();
        graph.set_entry_point("write").unwrap();
        graph.set_finish_point("write").unwrap();
        let compiled = graph.compile().unwrap();
        let context = RuntimeContext::new(()).with_stream_mode(StreamMode::Updates);

        let mut receiver = compiled.stream(Some(StateValue::Null), context).unwrap();
        let item = receiver.recv().await.unwrap().unwrap();

        assert_eq!(item.mode, StreamMode::Updates);
        assert_eq!(
            item.data,
            StateValue::Object(HashMap::from([(
                "write".to_string(),
                StateValue::Number(1.0)
            )]))
        );
    }

    #[test]
    fn compiled_graph_executes_conditional_edge() {
        let mut graph: StateGraph<StateValue, StateValue> = StateGraph::new();
        graph
            .channels
            .insert("value".to_string(), Box::new(LastValue::new()));
        graph
            .add_node("route", Box::new(|_, _| Ok(NodeOutput::None)))
            .unwrap();
        graph
            .add_node(
                "next",
                Box::new(|_, _| {
                    Ok(NodeOutput::Update(StateValue::Object(HashMap::from([(
                        "value".to_string(),
                        StateValue::String("routed".to_string()),
                    )]))))
                }),
            )
            .unwrap();
        graph.set_entry_point("route").unwrap();
        graph
            .add_conditional_edges(
                "route",
                "choose",
                Box::new(|_, _| Some("next".to_string())),
                HashMap::from([("next".to_string(), "next".to_string())]),
            )
            .unwrap();
        graph.set_finish_point("next").unwrap();
        let compiled = graph.compile().unwrap();

        let output = compiled
            .invoke(Some(StateValue::Null), RuntimeContext::default())
            .unwrap();

        assert_eq!(output, StateValue::String("routed".to_string()));
    }

    #[test]
    fn compiled_graph_executes_waiting_edge_after_all_starts() {
        let mut graph: StateGraph<StateValue, StateValue> = StateGraph::new();
        graph
            .channels
            .insert("value".to_string(), Box::new(LastValue::new()));
        graph
            .add_node("a", Box::new(|_, _| Ok(NodeOutput::None)))
            .unwrap();
        graph
            .add_node("b", Box::new(|_, _| Ok(NodeOutput::None)))
            .unwrap();
        graph
            .add_node(
                "join",
                Box::new(|_, _| {
                    Ok(NodeOutput::Update(StateValue::Object(HashMap::from([(
                        "value".to_string(),
                        StateValue::String("joined".to_string()),
                    )]))))
                }),
            )
            .unwrap();
        graph.add_edge(START, "a").unwrap();
        graph.add_edge(START, "b").unwrap();
        graph.add_edge(["a", "b"], "join").unwrap();
        graph.set_finish_point("join").unwrap();
        let compiled = graph.compile().unwrap();

        let output = compiled
            .invoke(Some(StateValue::Null), RuntimeContext::default())
            .unwrap();

        assert_eq!(output, StateValue::String("joined".to_string()));
    }

    #[test]
    fn with_schema_adds_state_channels() {
        let mut graph: StateGraph<TestSchema, i32> = StateGraph::with_schema();

        graph
            .add_node("a", Box::new(|_, _| Ok(NodeOutput::None)))
            .unwrap();
        graph.set_entry_point("a").unwrap();
        graph.set_finish_point("a").unwrap();

        let compiled = graph.compile().unwrap();
        let node = compiled.pregel.nodes.get("a").unwrap();

        assert!(node.channels.contains(&"left".to_string()));
        assert!(node.channels.contains(&"right".to_string()));
        assert_eq!(compiled.pregel.output_channels.len(), 2);
    }

    #[test]
    fn with_schema_adds_managed_values() {
        let mut graph: StateGraph<TestSchema, i32> = StateGraph::with_schema();

        graph
            .add_node("a", Box::new(|_, _| Ok(NodeOutput::None)))
            .unwrap();
        graph.set_entry_point("a").unwrap();
        graph.set_finish_point("a").unwrap();

        let compiled = graph.compile().unwrap();
        let node = compiled.pregel.nodes.get("a").unwrap();

        assert!(node.channels.contains(&"runtime".to_string()));
        assert!(compiled.pregel.managed.contains_key("runtime"));
    }

    #[test]
    fn new_keeps_empty_schema_tables() {
        let graph: StateGraph<TestSchema, i32> = StateGraph::new();

        assert!(graph.channels.is_empty());
        assert!(graph.managed.is_empty());
    }

    #[test]
    fn compile_regular_edge_reuses_target_branch_trigger() {
        let mut graph: StateGraph<i32, i32> = StateGraph::new();

        graph.add_node("a", noop_node()).unwrap();
        graph.add_node("b", noop_node()).unwrap();
        graph.set_entry_point("a").unwrap();
        graph.add_edge("a", "b").unwrap();
        graph.set_finish_point("b").unwrap();

        let compiled = graph.compile().unwrap();
        let node = compiled.pregel.nodes.get("b").unwrap();

        assert_eq!(node.triggers, vec!["branch:to:b".to_string()]);
        assert!(compiled.pregel.channels.contains_key("branch:to:b"));
        assert_eq!(compiled.pregel.nodes.get("a").unwrap().writers.len(), 2);
    }

    #[test]
    fn compile_does_not_trigger_end_node() {
        let mut graph: StateGraph<i32, i32> = StateGraph::new();

        graph.add_node("a", noop_node()).unwrap();
        graph.set_entry_point("a").unwrap();
        graph.set_finish_point("a").unwrap();

        let compiled = graph.compile().unwrap();

        assert!(!compiled.pregel.nodes.contains_key(END));
        assert!(
            compiled
                .pregel
                .nodes
                .values()
                .all(|node| !node.triggers.iter().any(|trigger| trigger == END))
        );
    }

    #[test]
    fn compile_attaches_conditional_entry_point() {
        let mut graph: StateGraph<i32, i32> = StateGraph::new();
        let ends = HashMap::from([("next".to_string(), "a".to_string())]);

        graph.add_node("a", noop_node()).unwrap();
        graph
            .set_conditional_entry_point("route", route_path(), ends)
            .unwrap();
        graph.set_finish_point("a").unwrap();

        let compiled = graph.compile().unwrap();
        let start = compiled.pregel.nodes.get(START).unwrap();
        let context = RuntimeContext::new(());
        let writes = start.writers[0]
            .assemble(&StateValue::Null, false, &0, &context)
            .unwrap();

        assert_eq!(start.triggers, vec![START.to_string()]);
        assert_eq!(writes, vec![("branch:to:a".to_string(), StateValue::Null)]);
        assert!(compiled.pregel.channels.contains_key("branch:to:a"));
    }

    #[test]
    fn compile_attaches_node_conditional_branch_writer() {
        let mut graph: StateGraph<i32, i32> = StateGraph::new();
        let ends = HashMap::from([("next".to_string(), "b".to_string())]);

        graph.add_node("a", noop_node()).unwrap();
        graph.add_node("b", noop_node()).unwrap();
        graph.set_entry_point("a").unwrap();
        graph
            .add_conditional_edges("a", "route", route_path(), ends)
            .unwrap();
        graph.set_finish_point("b").unwrap();

        let compiled = graph.compile().unwrap();
        let node = compiled.pregel.nodes.get("a").unwrap();
        let context = RuntimeContext::new(());
        let writes = node.writers[1]
            .assemble(&StateValue::Null, false, &0, &context)
            .unwrap();

        assert_eq!(node.writers.len(), 2);
        assert_eq!(writes, vec![("branch:to:b".to_string(), StateValue::Null)]);
        assert!(compiled.pregel.channels.contains_key("branch:to:b"));
    }

    #[test]
    fn compile_attaches_waiting_edge_starts_for_now() {
        let mut graph: StateGraph<i32, i32> = StateGraph::new();

        graph.add_node("a", noop_node()).unwrap();
        graph.add_node("b", noop_node()).unwrap();
        graph.add_node("c", noop_node()).unwrap();
        graph.set_entry_point("a").unwrap();
        graph.add_edge(["a", "b"], "c").unwrap();
        graph.set_finish_point("c").unwrap();

        let compiled = graph.compile().unwrap();
        let node = compiled.pregel.nodes.get("c").unwrap();

        assert!(node.triggers.contains(&"join:a+b:c".to_string()));
        assert!(compiled.pregel.channels.contains_key("join:a+b:c"));
        assert_eq!(compiled.pregel.nodes.get("a").unwrap().writers.len(), 2);
        assert_eq!(compiled.pregel.nodes.get("b").unwrap().writers.len(), 2);
    }

    #[test]
    fn add_node_rejects_duplicate_name() {
        let mut graph: StateGraph<i32, i32> = StateGraph::new();

        graph.add_node("a", noop_node()).unwrap();
        let error = match graph.add_node("a", noop_node()) {
            Err(error) => error,
            Ok(_) => panic!("duplicate node should be rejected"),
        };

        assert!(matches!(error, GraphError::DuplicateNode(name) if name == "a"));
    }

    #[test]
    fn add_node_rejects_reserved_name_and_character() {
        let mut graph: StateGraph<i32, i32> = StateGraph::new();

        let reserved_name = match graph.add_node(START, noop_node()) {
            Err(error) => error,
            Ok(_) => panic!("reserved node name should be rejected"),
        };
        let reserved_character = match graph.add_node("a:b", noop_node()) {
            Err(error) => error,
            Ok(_) => panic!("reserved node character should be rejected"),
        };

        assert!(matches!(reserved_name, GraphError::ReservedNodeName(name) if name == START));
        assert!(matches!(
            reserved_character,
            GraphError::ReservedNodeCharacter { node, character }
                if node == "a:b" && character == ":"
        ));
    }

    #[test]
    fn add_edge_rejects_invalid_endpoints() {
        let mut graph: StateGraph<i32, i32> = StateGraph::new();

        graph.add_node("a", noop_node()).unwrap();

        let start_as_end = match graph.add_edge("a", START) {
            Err(error) => error,
            Ok(_) => panic!("START as edge end should be rejected"),
        };
        let end_as_start = match graph.add_edge(END, "a") {
            Err(error) => error,
            Ok(_) => panic!("END as edge start should be rejected"),
        };
        let start_in_waiting_edge = match graph.add_edge([START, "a"], END) {
            Err(error) => error,
            Ok(_) => panic!("START in waiting edge should be rejected"),
        };

        assert!(matches!(start_as_end, GraphError::StartCannotBeEnd));
        assert!(matches!(end_as_start, GraphError::EndCannotBeStart));
        assert!(matches!(
            start_in_waiting_edge,
            GraphError::StartCannotBeWaitingEdgeStart
        ));
    }

    #[test]
    fn add_conditional_edges_rejects_duplicate_branch_name() {
        let mut graph: StateGraph<i32, i32> = StateGraph::new();
        let ends = HashMap::from([("next".to_string(), END.to_string())]);

        graph
            .set_conditional_entry_point("route", route_path(), ends.clone())
            .unwrap();
        let error = match graph.set_conditional_entry_point("route", route_path(), ends) {
            Err(error) => error,
            Ok(_) => panic!("duplicate branch should be rejected"),
        };

        assert!(matches!(
            error,
            GraphError::DuplicateBranch { node, branch }
                if node == START && branch == "route"
        ));
    }
}
