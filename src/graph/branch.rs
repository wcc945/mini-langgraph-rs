use crate::error::GraphError;
use crate::runtime::RuntimeContext;
use std::collections::HashMap;

pub enum BranchOutput {
    One(String),
    Many(Vec<String>),
    //Send(Vec<String>),//todo
}

pub type BranchPathFn<StateT, ContextT> =
    Box<dyn Fn(&StateT, &mut RuntimeContext<ContextT>) -> Option<String> + Send + Sync + 'static>;

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
