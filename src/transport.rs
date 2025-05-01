use crate::raft;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
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

pub trait RaftTransport<Id> {
    async fn send_raft_message(
        &mut self,
        from: Id,
        to: Id,
        message: raft::RaftMessage<Id>,
    ) -> anyhow::Result<()>;

    async fn send_client_response(
        &mut self,
        from: Id,
        to: Id,
        response: ClientResponse,
    ) -> anyhow::Result<()>;
}
