mod branch;
mod node;
mod state;
mod waiting_edge;

pub use state::StateGraph;
pub use waiting_edge::WaitingEdgeSpec;
pub const START: &str = "__start__"; //虚拟起点
pub const END: &str = "__end__"; //虚拟终点
