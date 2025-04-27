use crate::node::NodeId;
use serde::{Deserialize, Serialize};

pub trait MessageSender<M> {
    async fn send_node_message(&self, from: NodeId, to: NodeId, message: M) -> anyhow::Result<()>;

    async fn deliver_message(
        &self,
        from: NodeId,
        to: NodeId,
        message: LogEntryResponseType,
    ) -> anyhow::Result<()>;
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum LogEntryType {
    Read {
        key: String,
        msg_id: usize,
    },
    Write {
        key: String,
        value: String,
        msg_id: usize,
    },
    Cas {
        key: String,
        from: String,
        to: String,
        msg_id: usize,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogEntryResponseType {
    Read {
        in_reply_to: usize,
        value: Option<String>,
    },
    Write {
        in_reply_to: usize,
    },
    Cas {
        in_reply_to: usize,
        written: bool,
    },
}
