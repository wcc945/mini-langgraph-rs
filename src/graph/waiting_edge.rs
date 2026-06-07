#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WaitingEdgeSpec {
    pub starts: Vec<String>,
    pub end: String,
}

impl WaitingEdgeSpec {
    pub fn new(mut starts: Vec<String>, end: impl Into<String>) -> Self {
        starts.sort();
        starts.dedup();

        Self {
            starts,
            end: end.into(),
        }
    }
}
