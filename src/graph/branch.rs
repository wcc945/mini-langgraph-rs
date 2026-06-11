use crate::error::GraphError;
use crate::runtime::RuntimeContext;
use std::collections::HashMap;

pub enum BranchOutput {
    One(String),
    Many(Vec<String>),
    //Send(Vec<String>),//todo
}

pub type BranchPathFn<StateT, ContextT> =
    Box<dyn Fn(&StateT, &RuntimeContext<ContextT>) -> Option<String> + Send + Sync + 'static>;

pub struct BranchSpec<StateT, ContextT> {
    pub path: BranchPathFn<StateT, ContextT>,
    pub ends: Option<HashMap<String, String>>,
}

impl<StateT, ContextT> BranchSpec<StateT, ContextT> {
    pub fn new(
        path: BranchPathFn<StateT, ContextT>,
        ends: Option<HashMap<String, String>>,
    ) -> Self {
        Self { path, ends }
    }

    pub fn resolve(&self, output: BranchOutput) -> Result<Vec<String>, GraphError> {
        let keys = match output {
            BranchOutput::One(key) => vec![key],
            BranchOutput::Many(keys) => keys,
        };

        keys.into_iter()
            .map(|key| {
                if let Some(ends) = &self.ends {
                    ends.get(&key)
                        .cloned()
                        .ok_or_else(|| GraphError::InvalidBranchTarget(key))
                } else {
                    Ok(key)
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn never_called_path() -> BranchPathFn<i32, ()> {
        Box::new(|_, _| None)
    }

    #[test]
    fn resolve_returns_key_directly_when_no_path_map_exists() {
        let branch = BranchSpec::new(never_called_path(), None);

        let targets = branch
            .resolve(BranchOutput::One("next".to_string()))
            .unwrap();

        assert_eq!(targets, vec!["next".to_string()]);
    }

    #[test]
    fn resolve_maps_multiple_keys_through_path_map() {
        let branch = BranchSpec::new(
            never_called_path(),
            Some(HashMap::from([
                ("left".to_string(), "node_a".to_string()),
                ("right".to_string(), "node_b".to_string()),
            ])),
        );

        let targets = branch
            .resolve(BranchOutput::Many(vec![
                "left".to_string(),
                "right".to_string(),
            ]))
            .unwrap();

        assert_eq!(targets, vec!["node_a".to_string(), "node_b".to_string()]);
    }

    #[test]
    fn resolve_rejects_key_missing_from_path_map() {
        let branch = BranchSpec::new(
            never_called_path(),
            Some(HashMap::from([("known".to_string(), "node".to_string())])),
        );

        let error = branch
            .resolve(BranchOutput::One("missing".to_string()))
            .unwrap_err();

        assert!(matches!(error, GraphError::InvalidBranchTarget(key) if key == "missing"));
    }
}
