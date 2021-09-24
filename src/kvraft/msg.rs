use madsim::Request;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Request)]
#[rtype("String")]
pub enum Op {
    Get { key: String },
    Put { key: String, value: String },
    Append { key: String, value: String },
}

#[derive(thiserror::Error, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Error {
    #[error("not leader, hint: {hint}")]
    NotLeader { hint: usize },
    #[error("timeout")]
    Timeout,
    #[error("failed to reach consensus")]
    Failed,
}
