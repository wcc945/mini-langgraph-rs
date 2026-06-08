use crate::channel::DynChannel;
use crate::error::GraphError;
use crate::graph::branch::{BranchPathFn, BranchSpec};
use crate::graph::consts::{END, RESERVED, START};
use crate::graph::node::{NodeFn, StateNodeSpec};
use crate::graph::waiting_edge::WaitingEdgeSpec;
use crate::managed::ManagedValueSpec;
use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;

pub struct StateGraph<StateT, UpdateT, ContextT = (), InputT = StateT, OutputT = StateT> {
    nodes: HashMap<String, StateNodeSpec<StateT, UpdateT, ContextT>>,
    edges: HashSet<(String, String)>,
    branches: HashMap<String, HashMap<String, BranchSpec<StateT, ContextT>>>,
    waiting_edges: HashSet<WaitingEdgeSpec>,
    channels: HashMap<String, Box<DynChannel>>,
    managed: HashMap<String, Box<dyn ManagedValueSpec>>,
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
    use crate::graph::node::NodeOutput;

    fn noop_node() -> NodeFn<i32, i32, ()> {
        Box::new(|_, _| Ok(NodeOutput::None))
    }

    fn route_path() -> BranchPathFn<i32, ()> {
        Box::new(|_, _| Some("next".to_string()))
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
