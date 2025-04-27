use crate::raft;
use crate::raft::NodeId;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientRequest {
    Read {
        key: usize,
        msg_id: usize,
    },
    Write {
        key: usize,
        value: usize,
        msg_id: usize,
    },
    Cas {
        key: usize,
        from: usize,
        to: usize,
        msg_id: usize,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientResponse {
    ReadOk {
        in_reply_to: usize,
        value: Option<usize>,
    },
    WriteOk {
        in_reply_to: usize,
    },
    CasOk {
        in_reply_to: usize,
        written: bool,
    },
}

pub trait RaftTransport {
    async fn send_raft_message(
        &self,
        from: NodeId,
        to: NodeId,
        message: raft::RaftMessage,
    ) -> anyhow::Result<()>;

    async fn send_client_response(
        &self,
        from: NodeId,
        to: NodeId,
        response: ClientResponse,
    ) -> anyhow::Result<()>;
}
