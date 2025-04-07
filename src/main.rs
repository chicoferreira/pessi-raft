use std::cmp::min;
use std::collections::{HashMap, HashSet};

type NodeId = usize;
type TermId = usize;

#[derive(Debug, Default)]
struct Node {
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
}

#[derive(Debug, Clone, Copy, Default)]
enum Role {
    #[default]
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone)]
struct LogEntry {
    term: TermId,
    command: String,
}

struct VoteRequestMessage {
    node_id: NodeId,
    current_term: TermId,
    log_length: usize,
    last_log_term: TermId,
}

struct VoteResponseMessage {
    node_id: NodeId,
    current_term: TermId,
    vote_granted: bool,
}

struct LogRequestMessage {
    leader_id: NodeId,
    current_term: TermId,
    prefix_length: usize,
    prefix_term: TermId,
    commit_length: usize,
    suffix: Vec<LogEntry>,
}

struct LogResponseMessage {
    node_id: NodeId,
    current_term: TermId,
    ack: usize,
    success: bool,
}

impl Node {
    pub fn start_election(&mut self) {
        self.current_role = Role::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.id);
        self.votes_received.insert(self.id);

        let last_term = self.log.last().map(|e| e.term).unwrap_or(0);

        let vote_request_message = VoteRequestMessage {
            node_id: self.id,
            current_term: self.current_term,
            log_length: self.log.len(),
            last_log_term: last_term,
        };

        for node in &self.nodes {
            if *node != self.id {
                // TODO: Send vote request message to the node
                // send_packet(*node, vote_request_message);
            }
        }

        self.start_election_timer();
    }

    pub fn start_election_timer(&mut self) {
        // TODO: Start the election timer logic here
    }

    pub fn cancel_election_timer(&mut self) {
        // TODO: Cancel the election timer logic here
    }

    pub fn handle_vote_request(&mut self, candidate_vote_request: VoteRequestMessage) {
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
                .is_none_or(|n| n == candidate_vote_request.node_id);

        if vote_granted {
            self.voted_for = Some(candidate_vote_request.node_id);
        }

        let vote_response_message = VoteResponseMessage {
            node_id: self.id,
            current_term: self.current_term,
            vote_granted,
        };

        // TODO: Send vote response message to the candidate
        // send_packet(candidate_vote_request.node_id, vote_response_message);
    }

    pub fn handle_vote_response(&mut self, vote_response: VoteResponseMessage) {
        if matches!(self.current_role, Role::Candidate)
            && vote_response.current_term == self.current_term
            && vote_response.vote_granted
        {
            self.votes_received.insert(vote_response.node_id);
            // ceil((self.nodes.len() + 1) / 2)
            if self.votes_received.len() >= (self.nodes.len() + 1).div_ceil(2) {
                self.current_role = Role::Leader;
                self.current_leader = Some(self.id);
                self.cancel_election_timer();
                for &follower in &self.nodes {
                    if follower != self.id {
                        self.sent_length.insert(follower, self.log.len());
                        self.acked_length.insert(follower, 0);
                        self.replicate_log(follower);
                    }
                }
            }
        } else if vote_response.current_term > self.current_term {
            self.current_term = vote_response.current_term;
            self.current_role = Role::Follower;
            self.voted_for = None;
            self.cancel_election_timer();
        }
    }

    pub fn append_to_log(&mut self, message: String) {
        if matches!(self.current_role, Role::Leader) {
            self.log.push(LogEntry {
                term: self.current_term,
                command: message,
            });
            self.acked_length.insert(self.id, self.log.len());
            for &follower in &self.nodes {
                if follower != self.id {
                    self.replicate_log(follower);
                }
            }
        } else {
            // TODO: forward the request to the currentLeader
        }
    }

    // TODO: run this periodically
    pub fn tick_periodically(&self) {
        if matches!(self.current_role, Role::Leader) {
            for &follower in &self.nodes {
                if follower != self.id {
                    self.replicate_log(follower);
                }
            }
        }
    }

    fn replicate_log(&self, node: NodeId) {
        let prefix_length = self.sent_length.get(&node).copied().unwrap_or(0);
        let suffix = self.log[prefix_length..].to_vec();

        let prefix_term = if prefix_length > 0 {
            self.log[prefix_length - 1].term
        } else {
            0
        };

        let log_request_message = LogRequestMessage {
            leader_id: self.id,
            current_term: self.current_term,
            prefix_length,
            prefix_term,
            commit_length: self.commit_length,
            suffix,
        };

        // TODO: Send log request message to the follower
    }

    pub fn handle_log_request(&mut self, log_request: LogRequestMessage) {
        if log_request.current_term > self.current_term {
            self.current_term = log_request.current_term;
            self.voted_for = None;
            self.cancel_election_timer()
        }
        if log_request.current_term == self.current_term {
            self.current_role = Role::Follower;
            self.current_leader = Some(log_request.leader_id);
        }

        let log_ok = (self.log.len() >= log_request.prefix_length)
            && (log_request.prefix_length == 0
                || self.log[log_request.prefix_length - 1].term == log_request.prefix_term);

        if log_request.current_term == self.current_term && log_ok {
            let ack = log_request.prefix_length + log_request.suffix.len();
            self.append_entries(
                log_request.prefix_length,
                log_request.commit_length,
                log_request.suffix,
            );

            let log_response_message = LogResponseMessage {
                node_id: self.id,
                current_term: self.current_term,
                ack,
                success: true,
            };

            // TODO: Send log response message
        } else {
            let log_response_message = LogResponseMessage {
                node_id: self.id,
                current_term: self.current_term,
                ack: 0,
                success: false,
            };

            // TODO: Send log response message
        }
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

    pub fn handle_log_response(&mut self, log_response: LogResponseMessage) {
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
                    .insert(log_response.node_id, log_response.ack);
                self.acked_length
                    .insert(log_response.node_id, log_response.ack);
                self.commit_log_entries();
            } else if follower_sent_length > 0 {
                self.sent_length
                    .insert(log_response.node_id, follower_sent_length - 1);
                self.replicate_log(log_response.node_id);
            }
        } else if log_response.current_term > self.current_term {
            self.current_term = log_response.current_term;
            self.current_role = Role::Follower;
            self.voted_for = None;
            self.cancel_election_timer();
        }
    }

    fn commit_log_entries(&mut self) {
        while self.commit_length < self.log.len() {
            let acks = self
                .nodes
                .iter()
                .filter(|node| {
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
}

fn main() {
    println!("Hello, world!");
}
