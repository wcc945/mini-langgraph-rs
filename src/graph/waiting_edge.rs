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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_sorts_and_deduplicates_start_nodes() {
        let spec = WaitingEdgeSpec::new(
            vec!["b".to_string(), "a".to_string(), "b".to_string()],
            "end",
        );

        assert_eq!(spec.starts, vec!["a".to_string(), "b".to_string()]);
        assert_eq!(spec.end, "end");
    }

    #[test]
    fn equivalent_start_sets_produce_equal_specs() {
        let first = WaitingEdgeSpec::new(vec!["a".to_string(), "b".to_string()], "end");
        let second = WaitingEdgeSpec::new(vec!["b".to_string(), "a".to_string()], "end");

        assert_eq!(first, second);
    }
}
