use crate::graph::node::StateNodeSpec;
use std::collections::HashMap;

// pub struct StateGraph<StateT, UpdateT, ContextT = (), InputT = StateT, OutputT = StateT> {
//     nodes: HashMap<String, StateNodeSpec<StateT, UpdateT, ContextT>>,
// }

pub struct StateGraph<StateT, UpdateT, ContextT = ()> {
    nodes: HashMap<String, StateNodeSpec<StateT, UpdateT, ContextT>>,
}
