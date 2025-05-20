use crate::raft;
use crate::raft::{Node, RaftError};
use crate::transport::{ClientRequest, ClientResponse, RaftTransport};
use anyhow::{Context, bail};
use log::debug;
use serde::{Deserialize, Serialize};
use std::mem;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

#[derive(Debug, Deserialize, Serialize)]
pub struct MaelstromMessage {
    pub src: String,
    pub dest: String,
    pub body: MaelstromBody,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum MaelstromBody {
    Init {
        msg_id: usize,
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk {
        in_reply_to: usize,
    },
    Read {
        key: usize,
        msg_id: usize,
    },
    ReadOk {
        in_reply_to: usize,
        value: usize,
    },
    Write {
        key: usize,
        value: usize,
        msg_id: usize,
    },
    WriteOk {
        in_reply_to: usize,
    },
    Cas {
        key: usize,
        from: usize,
        to: usize,
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
    Raft(raft::RaftMessage<String>),
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

pub struct MaelstromServer {
    reader: BufReader<tokio::io::Stdin>,
    buffer: Vec<u8>,
}

impl MaelstromServer {
    pub fn new() -> Self {
        let reader = BufReader::new(tokio::io::stdin());
        let buffer = Vec::new();
        Self { reader, buffer }
    }

    pub async fn receive_message(&mut self) -> anyhow::Result<Option<MaelstromMessage>> {
        let _ = self.reader.read_until(b'\n', &mut self.buffer).await?;

        let buffer = mem::take(&mut self.buffer);
        let input = String::from_utf8(buffer).context("Failed to read line")?;

        debug!("Received message: {}", input);

        let input = input.trim();
        if input.is_empty() {
            return Ok(None);
        }

        let message: MaelstromMessage =
            serde_json::from_str(input).context("Failed to deserialize message")?;
        Ok(Some(message))
    }

    pub async fn init(&mut self) -> anyhow::Result<(String, Vec<String>)> {
        let init_message = self.receive_message().await?;
        let Some(init_message) = init_message else {
            bail!("Expected Init message, got None");
        };

        let MaelstromBody::Init {
            msg_id,
            node_id,
            node_ids,
        } = init_message.body
        else {
            bail!("Expected Init message, got {:?}", init_message.body);
        };

        let init_ok = MaelstromBody::InitOk {
            in_reply_to: msg_id,
        };
        self.send_message(node_id.clone(), init_message.src, init_ok)
            .await?;

        Ok((node_id, node_ids))
    }

    pub async fn handle_message(
        &mut self,
        message: anyhow::Result<Option<MaelstromMessage>>,
        node: &mut Node<String>,
    ) -> anyhow::Result<()> {
        let message = message.context("Error receiving message")?;

        let Some(message) = message else {
            return Ok(());
        };

        debug!("Received message: {:?}", message);
        if let MaelstromBody::Raft(raft_msg) = message.body {
            // ignore event, maybe in the future we can add this to a queue and handle election event
            let _ = node
                .handle_raft_message(self, raft_msg)
                .await
                .context("Error handling raft message")?;
            return Ok(());
        }

        #[rustfmt::skip]
        let (request, msg_id) = match message.body.clone() {
            MaelstromBody::Read { key, msg_id } => {
                let req = ClientRequest::Read { key, msg_id };
                (req, msg_id)
            }
            MaelstromBody::Write { key, value, msg_id } => {
                let req = ClientRequest::Write { key, value, msg_id };
                (req, msg_id)
            }
            MaelstromBody::Cas { key, from, to, msg_id } => {
                let req = ClientRequest::Cas { key, from, to, msg_id };
                (req, msg_id)
            }
            _ => {
                bail!("Unexpected message type: {:?}", message.body);
            }
        };

        let append = node.append_to_log(self, message.src.clone(), request).await;

        if let Err(RaftError::NotLeader { leader }) = append {
            if let Some(leader_id) = leader {
                debug!(
                    "Received change log message and I am not leader. Forwarding to leader: {:?}",
                    leader_id
                );
                self.send_message(message.src.clone(), leader_id, message.body)
                    .await?;
            } else {
                debug!(
                    "Received change log message and I am not leader. There is no elected leader."
                );
                let body = MaelstromBody::Error {
                    in_reply_to: msg_id,
                    code: ErrorCode::TemporarilyUnavailable,
                    text: Some("Leader is not elected".to_string()),
                };

                self.send_message(node.get_id().clone(), message.src.clone(), body)
                    .await?;
            }
            Ok(())
        } else {
            append.context("Error handling client request")
        }
    }

    pub async fn send_message(
        &self,
        from: String,
        to: String,
        message: MaelstromBody,
    ) -> anyhow::Result<()> {
        let message = MaelstromMessage {
            src: from,
            dest: to,
            body: message,
        };

        let output = serde_json::to_string(&message).context("Failed to serialize message")?;

        let mut stdout = tokio::io::stdout();
        stdout.write_all(output.as_bytes()).await?;
        stdout.write_all(b"\n").await?;
        stdout.flush().await?;

        Ok(())
    }
}

impl RaftTransport<String> for MaelstromServer {
    async fn send_raft_message(
        &mut self,
        from: String,
        to: String,
        message: raft::RaftMessage<String>,
    ) -> anyhow::Result<()> {
        self.send_message(from, to, MaelstromBody::Raft(message))
            .await
            .context("Failed to send node message")
    }

    async fn send_client_response(
        &mut self,
        from: String,
        to: String,
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
