use crate::fault::actor::{RaftActor, StaterightMessage};
use crate::fault::injector::FaultInjector;
use crate::raft::{Node, TermId};
use pollster::FutureExt;
use stateright::actor::{Id, Out};
use std::ops::ControlFlow;
use DoubleVoteFixMessage::ElectedBy;

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
        out: &mut Out<RaftActor<DoubleVoteFixMessage, DoubleVoteFixState>>,
    ) -> ControlFlow<(), StaterightMessage<DoubleVoteFixMessage>> {
        if let StaterightMessage::Other(ElectedBy { leader, term, by }) = msg {
            for id in by {
                if other_state
                    .votes
                    .iter()
                    .find(|vote| vote.from == id && vote.term == term && vote.to != leader)
                    .is_some()
                {
                    // the node already voted for someone else in this term
                    // add to blacklist
                    other_state.blacklist.push(id.clone());

                    // start a new election
                    state.start_election(out).block_on().unwrap();
                }
                other_state.votes.push(Vote {
                    term,
                    from: id,
                    to: leader,
                });
            }
            return ControlFlow::Break(());
        }

        ControlFlow::Continue(msg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fault::actor::{create_raft_actor_model, RaftActor};
    use crate::fault::double_vote::DoubleVoteFaultInjector;
    use stateright::Checker;
    use stateright::Model;

    #[test]
    fn test_double_vote_fix() {
        let peers: Vec<Id> = Id::vec_from(0..5);

        let actors = vec![
            RaftActor::new(peers.clone(), DoubleVoteFaultInjector),
            RaftActor::new(peers.clone(), DoubleVoteFixInjector),
            RaftActor::new(peers.clone(), DoubleVoteFixInjector),
            RaftActor::new(peers.clone(), DoubleVoteFixInjector),
            RaftActor::new(peers.clone(), DoubleVoteFixInjector),
        ];

        create_raft_actor_model()
            .actors(actors)
            .checker()
            .target_max_depth(11)
            .threads(num_cpus::get())
            .spawn_dfs()
            .join()
            .assert_properties();
    }
}
