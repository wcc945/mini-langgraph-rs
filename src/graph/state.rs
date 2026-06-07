use crate::channel::DynChannel;
use crate::graph::branch::BranchSpec;
use crate::graph::node::StateNodeSpec;
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

impl<StateT, UpdateT, ContextT, InputT, OutputT> Default
    for StateGraph<StateT, UpdateT, ContextT, InputT, OutputT>
{
    fn default() -> Self {
        Self::new()
    }
}
