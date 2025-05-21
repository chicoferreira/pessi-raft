use crate::raft::RaftEvent::ElectionStarted;
use crate::transport::{ClientRequest, ClientResponse, RaftTransport};
use anyhow::Result;
use log::debug;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::cmp::min;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::time::{Duration, Instant};
use thiserror::Error;

pub type TermId = usize;

#[derive(Debug, Clone)]
pub struct Node<Id: Clone + Hash + Eq> {
    id: Id,
    nodes: Vec<Id>,
    current_term: TermId,
    voted_for: Option<Id>,
    log: Vec<LogEntry<Id>>,
    commit_length: usize,
    current_role: Role,
    current_leader: Option<Id>,
    votes_received: Vec<Id>,
    sent_length: HashMap<Id, usize>,
    acked_length: HashMap<Id, usize>,
    election_deadline: Option<Instant>,
    election_timeout_range: (Duration, Duration), // min, max
    commited_log: HashMap<usize, usize>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    #[default]
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
pub struct LogEntry<Id> {
    pub term: TermId,
    /// The node that created this entry
    pub from: Id,
    pub request: ClientRequest,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VoteRequestMessage<Id> {
    pub node_id: Id,
    pub current_term: TermId,
    pub log_length: usize,
    pub last_log_term: TermId,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VoteResponseMessage<Id> {
    pub node_id: Id,
    pub current_term: TermId,
    pub vote_granted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LogRequestMessage<Id> {
    pub leader_id: Id,
    pub current_term: TermId,
    pub prefix_length: usize,
    pub prefix_term: TermId,
    pub commit_length: usize,
    pub suffix: Vec<LogEntry<Id>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LogResponseMessage<Id> {
    pub node_id: Id,
    pub current_term: TermId,
    pub ack: usize,
    pub success: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "raft_type")]
#[serde(rename_all = "snake_case")]
pub enum RaftMessage<Id> {
    VoteRequest(VoteRequestMessage<Id>),
    VoteResponse(VoteResponseMessage<Id>),
    LogRequest(LogRequestMessage<Id>),
    LogResponse(LogResponseMessage<Id>),
}

#[derive(Debug, Error)]
#[error("Raft error")]
pub enum RaftError<Id> {
    NotLeader { leader: Option<Id> },
    Anyhow(#[from] anyhow::Error),
}

pub enum RaftEvent<Id> {
    LeaderElected { leader: Id },
    ElectionStarted,
}

const ELECTION_TIMEOUT_MIN: Duration = Duration::from_millis(200);
const ELECTION_TIMEOUT_MAX: Duration = Duration::from_millis(1000);

impl<Id: Clone + Default + Eq + Hash + std::fmt::Display> Node<Id> {
    pub fn new(id: Id, nodes: Vec<Id>) -> Self {
        let mut node = Self {
            id,
            nodes,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            commit_length: 0,
            current_role: Role::Follower,
            current_leader: None,
            votes_received: Vec::new(),
            sent_length: HashMap::new(),
            acked_length: HashMap::new(),
            election_deadline: None,
            election_timeout_range: (ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX),
            commited_log: HashMap::new(),
        };
        node.start_election_timer();
        node
    }

    pub async fn start_election(
        &mut self,
        transport: &mut impl RaftTransport<Id>,
    ) -> Result<RaftEvent<Id>> {
        debug!(
            "Starting election for term {} with me as a candidate",
            self.current_term
        );
        self.current_role = Role::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.id.clone());
        self.votes_received.clear();
        self.votes_received.push(self.id.clone());

        let last_term = self.log.last().map(|e| e.term).unwrap_or(0);

        let vote_request_message = RaftMessage::VoteRequest(VoteRequestMessage {
            node_id: self.id.clone(),
            current_term: self.current_term,
            log_length: self.log.len(),
            last_log_term: last_term,
        });

        let nodes = self.nodes.clone();
        for node in nodes {
            if node != self.id {
                debug!("Sending vote request to {}", node);
                self.send_message(transport, node, vote_request_message.clone())
                    .await?
            }
        }

        self.start_election_timer();
        Ok(ElectionStarted)
    }

    pub fn start_election_timer(&mut self) {
        // pick a random timeout
        let (min, max) = self.election_timeout_range;
        let random_duration = rand::rng().random_range(min..max);

        self.election_deadline = Some(Instant::now() + random_duration);
        debug!(
            "Election timer changed to deadline {:?}",
            self.election_deadline
        );
    }

    pub fn cancel_election_timer(&mut self) {
        self.election_deadline = None;
    }

    pub async fn handle_vote_request(
        &mut self,
        transport: &mut impl RaftTransport<Id>,
        candidate_vote_request: VoteRequestMessage<Id>,
    ) -> Result<Option<RaftEvent<Id>>> {
        debug!(
            "Received vote request from {} for term {}",
            candidate_vote_request.node_id, candidate_vote_request.current_term
        );
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

        debug!("Voting {vote_granted}.");

        let vote_response_message = RaftMessage::VoteResponse(VoteResponseMessage {
            node_id: self.id.clone(),
            current_term: self.current_term,
            vote_granted,
        });

        self.send_message(
            transport,
            candidate_vote_request.node_id,
            vote_response_message,
        )
        .await
        .map(|_| None)
    }

    pub async fn handle_vote_response(
        &mut self,
        transport: &mut impl RaftTransport<Id>,
        vote_response: VoteResponseMessage<Id>,
    ) -> Result<Option<RaftEvent<Id>>> {
        let mut event = None;

        let from = vote_response.node_id;
        if matches!(self.current_role, Role::Candidate)
            && vote_response.current_term == self.current_term
            && vote_response.vote_granted
        {
            debug!("Received positive vote from {from} to become a leader");

            if self.votes_received.contains(&from) {
                return Ok(event);
            }

            self.votes_received.push(from);
            let received_votes = self.votes_received.len();
            if received_votes >= self.quorum() {
                debug!("Received quorum votes ({received_votes} votes), becoming leader");
                self.current_role = Role::Leader;
                self.current_leader = Some(self.id.clone());
                self.cancel_election_timer();
                for follower in &self.nodes {
                    if *follower != self.id {
                        self.sent_length.insert(follower.clone(), self.log.len());
                        self.acked_length.insert(follower.clone(), 0);
                        self.replicate_log(transport, follower.clone()).await?;
                    }
                }

                event = Some(RaftEvent::LeaderElected {
                    leader: self.id.clone(),
                });
            }
        } else if vote_response.current_term > self.current_term {
            debug!("Received vote response with higher term, becoming follower");
            self.current_term = vote_response.current_term;
            self.current_role = Role::Follower;
            self.voted_for = None;
            self.start_election_timer();
        } else if !vote_response.vote_granted {
            debug!("Received negative vote response from {from}");
        }

        Ok(event)
    }

    pub async fn append_to_log(
        &mut self,
        transport: &mut impl RaftTransport<Id>,
        from: Id,
        request: ClientRequest,
    ) -> std::result::Result<(), RaftError<Id>> {
        if matches!(self.current_role, Role::Leader) {
            self.log.push(LogEntry {
                term: self.current_term,
                from,
                request,
            });
            self.acked_length.insert(self.id.clone(), self.log.len());
            for follower in &self.nodes {
                if *follower != self.id {
                    self.replicate_log(transport, follower.clone()).await?;
                }
            }
        } else {
            return Err(RaftError::NotLeader {
                leader: self.current_leader.clone(),
            });
        }

        Ok(())
    }

    pub async fn tick_periodically(
        &mut self,
        transport: &mut impl RaftTransport<Id>,
    ) -> Result<()> {
        self.broadcast_replicate_log(transport).await?;

        if let Some(deadline) = self.election_deadline {
            if Instant::now() >= deadline {
                // timeout fired!
                self.start_election(transport).await?;
            }
        }

        Ok(())
    }

    pub async fn broadcast_replicate_log(
        &mut self,
        transport: &mut impl RaftTransport<Id>,
    ) -> Result<()> {
        if matches!(self.current_role, Role::Leader) {
            for follower in &self.nodes {
                if *follower != self.id {
                    self.replicate_log(transport, follower.clone()).await?;
                }
            }
        }
        Ok(())
    }

    async fn replicate_log(&self, transport: &mut impl RaftTransport<Id>, node: Id) -> Result<()> {
        let prefix_length = self.sent_length.get(&node).copied().unwrap_or(0);
        let suffix = self.log[prefix_length..].to_vec();

        let prefix_term = if prefix_length > 0 {
            self.log[prefix_length - 1].term
        } else {
            0
        };

        let log_request_message = RaftMessage::LogRequest(LogRequestMessage {
            leader_id: self.id.clone(),
            current_term: self.current_term,
            prefix_length,
            prefix_term,
            commit_length: self.commit_length,
            suffix,
        });

        self.send_message(transport, node, log_request_message)
            .await
    }

    pub async fn handle_log_request(
        &mut self,
        transport: &mut impl RaftTransport<Id>,
        log_request: LogRequestMessage<Id>,
    ) -> Result<Option<RaftEvent<Id>>> {
        if log_request.current_term > self.current_term {
            self.current_term = log_request.current_term;
            self.voted_for = None;
            self.cancel_election_timer()
        }

        let mut event = None;
        if log_request.current_term == self.current_term {
            self.current_role = Role::Follower;
            self.current_leader = Some(log_request.leader_id.clone());
            self.start_election_timer();

            event = Some(RaftEvent::LeaderElected {
                leader: log_request.leader_id.clone(),
            });
        }

        let log_ok = (self.log.len() >= log_request.prefix_length)
            && (log_request.prefix_length == 0
                || self.log[log_request.prefix_length - 1].term == log_request.prefix_term);

        let message = if log_request.current_term == self.current_term && log_ok {
            let ack = log_request.prefix_length + log_request.suffix.len();
            self.append_entries(
                transport,
                log_request.prefix_length,
                log_request.commit_length,
                log_request.suffix,
            )
            .await?;

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

        let message = RaftMessage::LogResponse(message);
        self.send_message(transport, log_request.leader_id, message)
            .await?;

        Ok(event)
    }

    async fn append_entries(
        &mut self,
        transport: &mut impl RaftTransport<Id>,
        prefix_length: usize,
        leader_commit: usize,
        suffix: Vec<LogEntry<Id>>,
    ) -> Result<()> {
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
            // deliver the entries from commit_length to leader_commit - 1 to the application
            for entry_index in self.commit_length..min(leader_commit - 1, self.log.len()) {
                let entry = self.log[entry_index].clone();
                self.deliver_log(transport, entry.clone()).await?;
            }
            self.commit_length = leader_commit;
        }

        Ok(())
    }

    pub async fn handle_log_response(
        &mut self,
        transport: &mut impl RaftTransport<Id>,
        log_response: LogResponseMessage<Id>,
    ) -> Result<Option<RaftEvent<Id>>> {
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
                self.commit_log_entries(transport).await?;
            } else if follower_sent_length > 0 {
                self.sent_length
                    .insert(log_response.node_id.clone(), follower_sent_length - 1);
                self.replicate_log(transport, log_response.node_id).await?;
            }
        } else if log_response.current_term > self.current_term {
            self.current_term = log_response.current_term;
            self.current_role = Role::Follower;
            self.voted_for = None;
            self.cancel_election_timer();
        }

        Ok(None)
    }

    async fn commit_log_entries(&mut self, transport: &mut impl RaftTransport<Id>) -> Result<()> {
        while self.commit_length < self.log.len() {
            let acks = self
                .nodes
                .iter()
                .filter(|&node| {
                    self.acked_length.get(node).copied().unwrap_or(0) > self.commit_length
                })
                .count();

            if acks >= self.quorum() {
                // deliver log[commit_length] to the application
                self.deliver_log(transport, self.log[self.commit_length].clone())
                    .await?;
                self.commit_length += 1;
            } else {
                break;
            }
        }

        Ok(())
    }

    pub async fn handle_raft_message(
        &mut self,
        transport: &mut impl RaftTransport<Id>,
        message: RaftMessage<Id>,
    ) -> Result<Option<RaftEvent<Id>>> {
        match message {
            RaftMessage::VoteRequest(req) => self.handle_vote_request(transport, req).await,
            RaftMessage::VoteResponse(res) => self.handle_vote_response(transport, res).await,
            RaftMessage::LogRequest(req) => self.handle_log_request(transport, req).await,
            RaftMessage::LogResponse(res) => self.handle_log_response(transport, res).await,
        }
    }

    async fn deliver_log(
        &mut self,
        transport: &mut impl RaftTransport<Id>,
        log_entry: LogEntry<Id>,
    ) -> Result<()> {
        #[rustfmt::skip]
        let log_entry_result = match log_entry.request {
            ClientRequest::Read { key, msg_id } => {
                let value = self.commited_log.get(&key).cloned();
                ClientResponse::ReadOk { in_reply_to: msg_id, value }
            }
            ClientRequest::Write { key, value, msg_id } => {
                self.commited_log.insert(key, value);
                ClientResponse::WriteOk { in_reply_to: msg_id }
            }
            ClientRequest::Cas { key, from, to, msg_id } => {
                let written = self.commited_log.get(&key) == Some(&from);
                if written {
                    self.commited_log.insert(key, to);
                }
                ClientResponse::CasOk { in_reply_to: msg_id, written }
            }
        };

        transport
            .send_client_response(self.id.clone(), log_entry.from, log_entry_result)
            .await
    }

    async fn send_message(
        &self,
        transport: &mut impl RaftTransport<Id>,
        to: Id,
        message: RaftMessage<Id>,
    ) -> Result<()> {
        transport
            .send_raft_message(self.id.clone(), to, message)
            .await
    }

    pub fn current_term(&self) -> TermId {
        self.current_term
    }

    pub fn current_role(&self) -> Role {
        self.current_role
    }

    pub fn commit_length(&self) -> usize {
        self.commit_length
    }

    pub fn commit_length_mut_ref(&mut self) -> &mut usize {
        &mut self.commit_length
    }

    pub fn get_id(&self) -> &Id {
        &self.id
    }

    pub fn log(&self) -> &Vec<LogEntry<Id>> {
        &self.log
    }

    pub fn nodes(&self) -> &Vec<Id> {
        &self.nodes
    }

    pub fn get_votes_received(&self) -> &Vec<Id> {
        &self.votes_received
    }

    fn quorum(&self) -> usize {
        (self.nodes.len() + 1).div_ceil(2)
    }
}

impl<Id: Clone + Default + Eq + Hash + Ord> Hash for Node<Id> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.current_term.hash(state);
        self.voted_for.hash(state);
        self.log.hash(state);
        self.commit_length.hash(state);
        self.current_role.hash(state);
        self.current_leader.hash(state);
        self.votes_received.hash(state);
        hash_hashmap(&self.sent_length, state);
        hash_hashmap(&self.acked_length, state);
    }
}

pub fn hash_hashmap<H, K, V>(map: &HashMap<K, V>, state: &mut H)
where
    H: Hasher,
    K: Clone + Hash + Ord,
    V: Clone + Hash + Ord,
{
    let mut cloned: Vec<(K, V)> = map.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    cloned.sort_by_key(|(k, _v)| k.clone());
    cloned.hash(state);
}

impl<Id: Clone + Default + Hash + PartialEq + Eq> PartialEq for Node<Id> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.current_term == other.current_term
            && self.voted_for == other.voted_for
            && self.log == other.log
            && self.commit_length == other.commit_length
            && self.current_role == other.current_role
            && self.current_leader == other.current_leader
            && self.votes_received == other.votes_received
            && self.sent_length == other.sent_length
            && self.acked_length == other.acked_length
    }
}
