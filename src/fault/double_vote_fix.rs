use crate::fault::actor::{RaftActor, StaterightMessage};
use crate::fault::double_vote_fix::DoubleVoteFixMessage::ElectedBy;
use crate::fault::injector::FaultInjector;
use crate::raft::{Node, RaftEvent, RaftMessage, TermId};
use stateright::actor::{Id, Out};
use std::collections::HashMap;
use std::ops::ControlFlow;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum DoubleVoteFixMessage {
    ElectedBy {
        leader: Id,
        term: TermId,
        by: Vec<Id>,
    },
}

#[derive(Clone, Debug, Default, Hash, PartialEq)]
struct DoubleVoteFixState {
    votes: Vec<Vote>,
    blacklist: Vec<Id>,
}

#[derive(Clone, Debug, Default, Hash, PartialEq)]
struct Vote {
    term: TermId,
    from: Id,
    to: Id,
}

struct DoubleVoteFixInjector;

impl FaultInjector<DoubleVoteFixMessage, DoubleVoteFixState> for DoubleVoteFixInjector {
    fn inject_on_msg(
        &self,
        state: &mut Node<Id>,
        other_state: &mut DoubleVoteFixState,
        msg: StaterightMessage<DoubleVoteFixMessage>,
        _out: &mut Out<RaftActor<DoubleVoteFixMessage, DoubleVoteFixState>>,
    ) -> ControlFlow<(), StaterightMessage<DoubleVoteFixMessage>> {
        // Check if the message is from a blacklisted node
        if let StaterightMessage::Raft(msg) = &msg {
            let from = match msg {
                RaftMessage::VoteRequest(msg) => msg.node_id,
                RaftMessage::VoteResponse(msg) => msg.node_id,
                RaftMessage::LogRequest(msg) => msg.leader_id,
                RaftMessage::LogResponse(msg) => msg.node_id,
            };

            if other_state.blacklist.iter().any(|&id| id == from) {
                // The node is blacklisted, ignore the message
                return ControlFlow::Break(());
            }
        }

        // When the node receives a vote response, and it is positive,
        // register the vote in the `other_state`
        if let StaterightMessage::Other(ElectedBy { leader, term, by }) = &msg {
            for id in by {
                other_state.votes.push(Vote {
                    term: *term,
                    from: *id,
                    to: *leader,
                })
            }

            // find duplicate votes and blacklist the nodes
            let mut seen = HashMap::new();
            for vote in &other_state.votes {
                if vote.term == state.current_term() {
                    if let Some(other_vote) = seen.get(&vote.from) {
                        if other_vote != &vote.to {
                            // Duplicate vote found, blacklist the node
                            other_state.blacklist.push(vote.from);
                        }
                    } else {
                        seen.insert(vote.from, vote.to);
                    }
                }
            }
        }

        ControlFlow::Continue(msg)
    }

    fn inject_on_event(
        &self,
        state: &mut Node<Id>,
        _other_state: &mut DoubleVoteFixState,
        out: &mut Out<RaftActor<DoubleVoteFixMessage, DoubleVoteFixState>>,
        event: RaftEvent<Id>,
    ) -> ControlFlow<(), RaftEvent<Id>> {
        match event {
            RaftEvent::LeaderElected { leader } => {
                if leader == *state.get_id() {
                    for other_id in state.nodes() {
                        // if other_id != state.get_id() {
                        // Send the elected message to all nodes
                        out.send(
                            *other_id,
                            StaterightMessage::Other(ElectedBy {
                                leader,
                                term: state.current_term(),
                                by: state.get_votes_received().iter().cloned().collect(),
                            }),
                        );
                        // }
                    }
                }
            }
            _ => {}
        }
        ControlFlow::Continue(event)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fault::actor::RaftActor;
    use crate::fault::double_vote::DoubleVoteFaultInjector;
    use stateright::Checker;
    use stateright::Expectation::Sometimes;
    use stateright::Model;
    use stateright::actor::{ActorModel, Network};
    use std::ops::Deref;

    #[test]
    fn test_double_vote_fix() {
        let peers: Vec<Id> = Id::vec_from(0..3);

        let actors = vec![
            RaftActor::new(peers.clone(), DoubleVoteFaultInjector),
            RaftActor::new(peers.clone(), DoubleVoteFixInjector),
            RaftActor::new(peers.clone(), DoubleVoteFixInjector),
        ];

        ActorModel::new((), ())
            .actors(actors)
            .init_network(Network::new_ordered(vec![]))
            .property(Sometimes, "Double Vote Banned", |_actor, state| {
                // check if someone banned node 1, which is the double vote fault injector
                for states in &state.actor_states {
                    let (_, _, state) = states.deref();
                    if state.blacklist.contains(&Id::from(0)) {
                        return true;
                    }
                }
                false
            })
            .checker()
            .target_max_depth(14)
            .threads(num_cpus::get())
            // .serve("localhost:3000");
            .spawn_dfs()
            .join()
            .assert_properties()
    }
}
