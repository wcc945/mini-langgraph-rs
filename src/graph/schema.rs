use std::collections::HashMap;

use crate::channel::DynChannel;
use crate::managed::ManagedValueSpec;

pub(crate) trait StateSchema {
    fn channels() -> HashMap<String, Box<DynChannel>>;

    fn managed() -> HashMap<String, Box<dyn ManagedValueSpec>> {
        HashMap::new()
    }
}
