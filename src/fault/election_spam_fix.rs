use crate::fault::actor::{RaftActor, StaterightMessage};
use crate::raft;
use crate::raft::{Node, RaftMessage, TermId};
use stateright::actor::{Id, Out};
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::ops::ControlFlow;

use super::injector::FaultInjector;

struct ElectionSpamFix;

#[derive(Default, PartialEq, Eq, Clone, Debug)]
struct ElectionSpamFixState {
    last_election_proposals: HashMap<TermId, Vec<Id>>,
    /// Nodes that have been blocked from sending election requests
    /// only used for property checking
    blocked_nodes: Vec<Id>,
}

impl<OtherMsg> FaultInjector<OtherMsg, ElectionSpamFixState> for ElectionSpamFix
where
    OtherMsg: Clone + Debug + Eq + Hash,
{
    fn inject_on_msg(
        &self,
        _node_state: &mut Node<Id>,
        other_state: &mut ElectionSpamFixState,
        msg: StaterightMessage<OtherMsg>,
        _out: &mut Out<RaftActor<OtherMsg, ElectionSpamFixState>>,
    ) -> ControlFlow<(), StaterightMessage<OtherMsg>> {
        // if in the last 2 terms we have seen an election from the same node
        // ignore the next VoteRequest packets from that node
        if let StaterightMessage::Raft(RaftMessage::VoteRequest(ref request)) = msg {
            let node_id = request.node_id;
            let term = request.current_term;

            other_state
                .last_election_proposals
                .entry(term)
                .or_default()
                .push(node_id);

            const MAX_CONSECUTIVE_ELECTIONS: usize = 3;
            let consecutive = other_state
                .last_election_proposals
                .iter()
                .filter(|(t, _)| *t + MAX_CONSECUTIVE_ELECTIONS > term)
                .filter(|(_, v)| v.contains(&node_id))
                .count();

            if consecutive >= MAX_CONSECUTIVE_ELECTIONS {
                other_state.blocked_nodes.push(node_id);

                return ControlFlow::Break(());
            }
        }
        ControlFlow::Continue(msg)
    }
}

impl Hash for ElectionSpamFixState {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        raft::hash_hashmap(&self.last_election_proposals, state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fault::actor::{RaftActor, create_raft_actor_model};
    use crate::fault::election_spam::ElectionSpamFault;
    use stateright::actor::Id;
    use stateright::{Checker, Expectation, Model};

    #[test]
    fn test_election_spam_fix() {
        let peers: Vec<Id> = Id::vec_from(0..3);

        let actors = vec![
            RaftActor::<(), ElectionSpamFixState>::new(peers.clone(), ElectionSpamFault),
            RaftActor::new(peers.clone(), ElectionSpamFix),
            RaftActor::new(peers.clone(), ElectionSpamFix),
        ];

        create_raft_actor_model()
            .actors(actors)
            .property(
                Expectation::Eventually,
                "First Node Is Banned",
                |_, state| {
                    state.actor_states.iter().any(|state| {
                        let (_, _, state) = state.as_ref();
                        state.blocked_nodes.contains(&Id::from(0))
                    })
                },
            )
            .checker()
            .target_max_depth(16)
            .threads(num_cpus::get())
            .spawn_bfs()
            .join()
            .assert_properties()
    }
}
