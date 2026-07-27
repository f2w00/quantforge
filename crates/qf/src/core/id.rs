use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Deserialize, Serialize)]
pub struct JournalId(pub String);

impl JournalId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Deserialize, Serialize)]
pub struct StrategyId(pub String);

impl StrategyId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}
