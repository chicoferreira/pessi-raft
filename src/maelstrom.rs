use crate::raft;
use crate::raft::NodeId;
use crate::transport::{ClientResponse, RaftTransport};
use anyhow::Context;
use serde::{Deserialize, Serialize};
use tokio::io;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

#[derive(Debug, Deserialize, Serialize)]
pub struct MaelstromMessage {
    pub src: String,
    pub dest: String,
    pub body: MaelstromBody,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum MaelstromBody {
    Init {
        msg_id: usize,
        node_id: NodeId,
        node_ids: Vec<NodeId>,
    },
    InitOk {
        in_reply_to: usize,
    },
    Read {
        key: String,
        msg_id: usize,
    },
    ReadOk {
        in_reply_to: usize,
        value: String,
    },
    Write {
        key: String,
        value: String,
        msg_id: usize,
    },
    WriteOk {
        in_reply_to: usize,
    },
    Cas {
        key: String,
        from: String,
        to: String,
        msg_id: usize,
    },
    CasOk {
        in_reply_to: usize,
    },
    Error {
        in_reply_to: usize,
        code: ErrorCode,
        text: Option<String>,
    },
    Raft(raft::RaftMessage),
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub enum ErrorCode {
    Timeout = 0,
    NodeNotFound = 1,
    NotSupported = 10,
    TemporarilyUnavailable = 11,
    MalformedRequest = 12,
    Crash = 13,
    Abort = 14,
    KeyDoesNotExist = 20,
    KeyAlreadyExists = 21,
    PreconditionFailed = 22,
    TxnConflict = 30,
}

pub struct MaelstromServer {}

impl MaelstromServer {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn receive_message(&self) -> anyhow::Result<MaelstromMessage> {
        let mut input = String::new();
        let mut reader = BufReader::new(io::stdin());
        reader.read_line(&mut input).await?;

        let message: MaelstromMessage =
            serde_json::from_str(&input).context("Failed to deserialize message")?;
        Ok(message)
    }

    pub async fn init(&self) -> anyhow::Result<(NodeId, Vec<NodeId>)> {
        let init_message = self.receive_message().await?;
        let MaelstromBody::Init {
            msg_id,
            node_id,
            node_ids,
        } = init_message.body
        else {
            anyhow::bail!("Expected Init message, got {:?}", init_message.body);
        };

        let init_ok = MaelstromBody::InitOk {
            in_reply_to: msg_id,
        };
        self.send_message(node_id.clone(), init_message.src, init_ok)
            .await?;

        Ok((node_id, node_ids))
    }

    pub async fn send_message(
        &self,
        from: NodeId,
        to: NodeId,
        message: MaelstromBody,
    ) -> anyhow::Result<()> {
        let message = MaelstromMessage {
            src: from,
            dest: to,
            body: message,
        };

        let output = serde_json::to_string(&message).context("Failed to serialize message")?;

        let mut stdout = io::stdout();
        stdout.write_all(output.as_bytes()).await?;
        stdout.write_all(b"\n").await?;
        stdout.flush().await?;

        Ok(())
    }
}

impl RaftTransport for MaelstromServer {
    async fn send_raft_message(
        &self,
        from: NodeId,
        to: NodeId,
        message: raft::RaftMessage,
    ) -> anyhow::Result<()> {
        self.send_message(from, to, MaelstromBody::Raft(message))
            .await
            .context("Failed to send node message")
    }

    async fn send_client_response(
        &self,
        from: NodeId,
        to: NodeId,
        message: ClientResponse,
    ) -> anyhow::Result<()> {
        match message {
            ClientResponse::ReadOk { in_reply_to, value } => {
                if let Some(value) = value {
                    let read_ok = MaelstromBody::ReadOk { in_reply_to, value };
                    self.send_message(from, to, read_ok).await?;
                } else {
                    let error = MaelstromBody::Error {
                        in_reply_to,
                        code: ErrorCode::KeyDoesNotExist,
                        text: Some("Key does not exist".to_string()),
                    };
                    self.send_message(from, to, error).await?;
                }
            }
            ClientResponse::WriteOk { in_reply_to } => {
                let write_ok = MaelstromBody::WriteOk { in_reply_to };
                self.send_message(from, to, write_ok).await?;
            }
            ClientResponse::CasOk {
                in_reply_to,
                written,
            } => {
                if written {
                    let cas_ok = MaelstromBody::CasOk { in_reply_to };
                    self.send_message(from, to, cas_ok).await?;
                } else {
                    let error = MaelstromBody::Error {
                        in_reply_to,
                        code: ErrorCode::PreconditionFailed,
                        text: Some("Precondition failed".to_string()),
                    };
                    self.send_message(from, to, error).await?;
                }
            }
        }

        Ok(())
    }
}
