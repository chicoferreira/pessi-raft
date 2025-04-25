use crate::server::MessageSender;
use anyhow::Result;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::cmp::min;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

pub type NodeId = String;
pub type TermId = usize;

// TODO: handle leader timeout

#[derive(Debug, Default)]
pub struct Node {
    id: NodeId,
    nodes: Vec<NodeId>,
    current_term: TermId,
    voted_for: Option<NodeId>,
    log: Vec<LogEntry>,
    commit_length: usize,
    current_role: Role,
    current_leader: Option<NodeId>,
    votes_received: HashSet<NodeId>,
    sent_length: HashMap<NodeId, usize>,
    acked_length: HashMap<NodeId, usize>,
    election_deadline: Option<Instant>,
    election_timeout_range: (Duration, Duration), // min, max
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    #[default]
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: TermId,
    pub command: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteRequestMessage {
    pub node_id: NodeId,
    pub current_term: TermId,
    pub log_length: usize,
    pub last_log_term: TermId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteResponseMessage {
    pub node_id: NodeId,
    pub current_term: TermId,
    pub vote_granted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogRequestMessage {
    pub leader_id: NodeId,
    pub current_term: TermId,
    pub prefix_length: usize,
    pub prefix_term: TermId,
    pub commit_length: usize,
    pub suffix: Vec<LogEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogResponseMessage {
    pub node_id: NodeId,
    pub current_term: TermId,
    pub ack: usize,
    pub success: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Message {
    VoteRequest(VoteRequestMessage),
    VoteResponse(VoteResponseMessage),
    LogRequest(LogRequestMessage),
    LogResponse(LogResponseMessage),
}

const ELECTION_TIMEOUT_MIN: Duration = Duration::from_millis(150);
const ELECTION_TIMEOUT_MAX: Duration = Duration::from_millis(300);

impl Node {
    pub fn new(id: NodeId, nodes: Vec<NodeId>) -> Self {
        Self {
            id,
            nodes,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            commit_length: 0,
            current_role: Role::Follower,
            current_leader: None,
            votes_received: HashSet::new(),
            sent_length: HashMap::new(),
            acked_length: HashMap::new(),
            election_deadline: None,
            election_timeout_range: (ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX),
        }
    }

    pub async fn start_election(&mut self, server: &impl MessageSender<Message>) -> Result<()> {
        self.current_role = Role::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.id.clone());
        self.votes_received.insert(self.id.clone());

        let last_term = self.log.last().map(|e| e.term).unwrap_or(0);

        let vote_request_message = Message::VoteRequest(VoteRequestMessage {
            node_id: self.id.clone(),
            current_term: self.current_term,
            log_length: self.log.len(),
            last_log_term: last_term,
        });

        let nodes = self.nodes.clone();
        for node in nodes {
            if node != self.id {
                self.send_message(server, node, vote_request_message.clone())
                    .await?
            }
        }

        self.start_election_timer();
        Ok(())
    }

    pub fn start_election_timer(&mut self) {
        // pick a random timeout
        let (min, max) = self.election_timeout_range;
        let random_duration = rand::rng().random_range(min..max);

        self.election_deadline = Some(Instant::now() + random_duration);
    }

    pub fn cancel_election_timer(&mut self) {
        self.election_deadline = None;
    }

    pub async fn handle_vote_request(
        &mut self,
        server: &impl MessageSender<Message>,
        candidate_vote_request: VoteRequestMessage,
    ) -> Result<()> {
        if candidate_vote_request.current_term > self.current_term {
            self.current_term = candidate_vote_request.current_term;
            self.current_role = Role::Follower;
            self.voted_for = None;
        }

        let last_term = self.log.last().map(|e| e.term).unwrap_or(0);

        let log_ok = (candidate_vote_request.last_log_term > last_term)
            || (candidate_vote_request.last_log_term == last_term
                && candidate_vote_request.log_length >= self.log.len());

        // TODO: confirm if candidate_current_term should be the same as our current_term
        let vote_granted = candidate_vote_request.current_term == self.current_term
            && log_ok
            && self
                .voted_for
                .clone()
                .is_none_or(|n| n == candidate_vote_request.node_id);

        if vote_granted {
            self.voted_for = Some(candidate_vote_request.node_id.clone());
        }

        let vote_response_message = Message::VoteResponse(VoteResponseMessage {
            node_id: self.id.clone(),
            current_term: self.current_term,
            vote_granted,
        });

        self.send_message(
            server,
            candidate_vote_request.node_id,
            vote_response_message,
        )
        .await
    }

    pub async fn handle_vote_response(
        &mut self,
        server: &impl MessageSender<Message>,
        vote_response: VoteResponseMessage,
    ) -> Result<()> {
        if matches!(self.current_role, Role::Candidate)
            && vote_response.current_term == self.current_term
            && vote_response.vote_granted
        {
            self.votes_received.insert(vote_response.node_id);
            // ceil((self.nodes.len() + 1) / 2)
            if self.votes_received.len() >= (self.nodes.len() + 1).div_ceil(2) {
                self.current_role = Role::Leader;
                self.current_leader = Some(self.id.clone());
                self.cancel_election_timer();
                for follower in &self.nodes {
                    if *follower != self.id {
                        self.sent_length.insert(follower.clone(), self.log.len());
                        self.acked_length.insert(follower.clone(), 0);
                        self.replicate_log(server, follower.clone()).await?;
                    }
                }
            }
        } else if vote_response.current_term > self.current_term {
            self.current_term = vote_response.current_term;
            self.current_role = Role::Follower;
            self.voted_for = None;
            self.cancel_election_timer();
        }

        Ok(())
    }

    pub async fn append_to_log(
        &mut self,
        server: &impl MessageSender<Message>,
        message: String,
    ) -> Result<()> {
        if matches!(self.current_role, Role::Leader) {
            self.log.push(LogEntry {
                term: self.current_term,
                command: message,
            });
            self.acked_length.insert(self.id.clone(), self.log.len());
            for follower in &self.nodes {
                if *follower != self.id {
                    self.replicate_log(server, follower.clone()).await?;
                }
            }
        } else {
            // TODO: forward the request to the currentLeader
        }

        Ok(())
    }

    pub async fn tick_periodically(&mut self, server: &impl MessageSender<Message>) -> Result<()> {
        if matches!(self.current_role, Role::Leader) {
            for follower in &self.nodes {
                if *follower != self.id {
                    self.replicate_log(server, follower.clone()).await?;
                }
            }
        }

        if let Some(deadline) = self.election_deadline {
            if Instant::now() >= deadline {
                // timeout fired!
                self.start_election(server).await?;
            }
        }

        Ok(())
    }

    async fn replicate_log(
        &self,
        server: &impl MessageSender<Message>,
        node: NodeId,
    ) -> Result<()> {
        let prefix_length = self.sent_length.get(&node).copied().unwrap_or(0);
        let suffix = self.log[prefix_length..].to_vec();

        let prefix_term = if prefix_length > 0 {
            self.log[prefix_length - 1].term
        } else {
            0
        };

        let log_request_message = Message::LogRequest(LogRequestMessage {
            leader_id: self.id.clone(),
            current_term: self.current_term,
            prefix_length,
            prefix_term,
            commit_length: self.commit_length,
            suffix,
        });

        self.send_message(server, node, log_request_message).await
    }

    pub async fn handle_log_request(
        &mut self,
        server: &impl MessageSender<Message>,
        log_request: LogRequestMessage,
    ) -> Result<()> {
        if log_request.current_term > self.current_term {
            self.current_term = log_request.current_term;
            self.voted_for = None;
            self.cancel_election_timer()
        }
        if log_request.current_term == self.current_term {
            self.current_role = Role::Follower;
            self.current_leader = Some(log_request.leader_id.clone());
        }

        let log_ok = (self.log.len() >= log_request.prefix_length)
            && (log_request.prefix_length == 0
                || self.log[log_request.prefix_length - 1].term == log_request.prefix_term);

        let message = if log_request.current_term == self.current_term && log_ok {
            let ack = log_request.prefix_length + log_request.suffix.len();
            self.append_entries(
                log_request.prefix_length,
                log_request.commit_length,
                log_request.suffix,
            );

            LogResponseMessage {
                node_id: self.id.clone(),
                current_term: self.current_term,
                ack,
                success: true,
            }
        } else {
            LogResponseMessage {
                node_id: self.id.clone(),
                current_term: self.current_term,
                ack: 0,
                success: false,
            }
        };

        let message = Message::LogResponse(message);
        self.send_message(server, log_request.leader_id, message)
            .await
    }

    fn append_entries(
        &mut self,
        prefix_length: usize,
        leader_commit: usize,
        suffix: Vec<LogEntry>,
    ) {
        if !suffix.is_empty() && self.log.len() > prefix_length {
            let index = min(self.log.len(), prefix_length + suffix.len()) - 1;
            if self.log[index].term != suffix[index - prefix_length].term {
                self.log.truncate(prefix_length);
            }
        }
        if prefix_length + suffix.len() > self.log.len() {
            for entry in suffix.into_iter().skip(self.log.len() - prefix_length) {
                self.log.push(entry)
            }
        }
        if leader_commit > self.commit_length {
            // TODO: deliver the entries from commit_length to leader_commit - 1 to the application
            self.commit_length = leader_commit;
        }
    }

    pub async fn handle_log_response(
        &mut self,
        server: &impl MessageSender<Message>,
        log_response: LogResponseMessage,
    ) -> Result<()> {
        if log_response.current_term == self.current_term
            && matches!(self.current_role, Role::Leader)
        {
            let follower_acked_length = self
                .acked_length
                .get(&log_response.node_id)
                .copied()
                .unwrap_or(0);

            let follower_sent_length = self
                .sent_length
                .get(&log_response.node_id)
                .copied()
                .unwrap_or(0);

            if log_response.success && log_response.ack >= follower_acked_length {
                self.sent_length
                    .insert(log_response.node_id.clone(), log_response.ack);
                self.acked_length
                    .insert(log_response.node_id, log_response.ack);
                self.commit_log_entries();
            } else if follower_sent_length > 0 {
                self.sent_length
                    .insert(log_response.node_id.clone(), follower_sent_length - 1);
                self.replicate_log(server, log_response.node_id).await?;
            }
        } else if log_response.current_term > self.current_term {
            self.current_term = log_response.current_term;
            self.current_role = Role::Follower;
            self.voted_for = None;
            self.cancel_election_timer();
        }

        Ok(())
    }

    fn commit_log_entries(&mut self) {
        while self.commit_length < self.log.len() {
            let acks = self
                .nodes
                .iter()
                .filter(|&node| {
                    self.acked_length.get(node).copied().unwrap_or(0) > self.commit_length
                })
                .count();

            // ceil((self.nodes.len() + 1) / 2)
            if acks >= (self.nodes.len() + 1).div_ceil(2) {
                // TODO: deliver log[commit_length] to the application
                self.commit_length += 1;
            } else {
                break;
            }
        }
    }

    pub async fn handle_message(
        &mut self,
        server: &impl MessageSender<Message>,
        message: Message,
    ) -> Result<()> {
        match message {
            Message::VoteRequest(req) => self.handle_vote_request(server, req).await,
            Message::VoteResponse(res) => self.handle_vote_response(server, res).await,
            Message::LogRequest(req) => self.handle_log_request(server, req).await,
            Message::LogResponse(res) => self.handle_log_response(server, res).await,
        }
    }

    async fn send_message(
        &self,
        server: &impl MessageSender<Message>,
        to: NodeId,
        message: Message,
    ) -> Result<()> {
        server.send_node_message(self.id.clone(), to, message).await
    }
}
