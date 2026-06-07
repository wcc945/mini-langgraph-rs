mod node;
mod state;

pub use state::StateGraph;
pub const START: &str = "__start__"; //虚拟起点
pub const END: &str = "__end__"; //虚拟终点
