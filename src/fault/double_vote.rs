use crate::fault::actor::{RaftActor, StaterightMessage};
use crate::fault::injector::FaultInjector;
use crate::raft::{Node, RaftMessage, VoteResponseMessage};
use stateright::actor::{Id, Out};
use std::fmt::Debug;
use std::hash::Hash;
use std::ops::ControlFlow;

pub struct DoubleVoteFaultInjector;

impl<OtherMsg, OtherState> FaultInjector<OtherMsg, OtherState> for DoubleVoteFaultInjector
where
    OtherMsg: Clone + Debug + Eq + Hash,
    OtherState: Default + Clone + Debug + Hash + PartialEq,
{
    fn inject_on_msg(
        &self,
        node_state: &mut Node<Id>,
        _other_state: &mut OtherState,
        msg: StaterightMessage<OtherMsg>,
        out: &mut Out<RaftActor<OtherMsg, OtherState>>,
    ) -> ControlFlow<(), StaterightMessage<OtherMsg>> {
        if let StaterightMessage::Raft(RaftMessage::VoteRequest(vote_request_message)) = msg {
            // if the message is a vote request, always accept
            let vote_response_message = RaftMessage::VoteResponse(VoteResponseMessage {
                node_id: node_state.get_id().clone(),
                current_term: node_state.current_term(),
                vote_granted: true,
            });
            out.send(
                vote_request_message.node_id,
                StaterightMessage::Raft(vote_response_message),
            );
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(msg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fault::actor::{create_raft_actor_model, RaftActor};
    use crate::fault::injector::NoFaultInjector;
    use crate::fault::property;
    use stateright::Checker;
    use stateright::DiscoveryClassification::Counterexample;
    use stateright::Model;

    #[test]
    fn test_double_vote_fault() {
        let peers: Vec<Id> = Id::vec_from(0..3);

        let actors = vec![
            RaftActor::<(), ()>::new(peers.clone(), NoFaultInjector),
            RaftActor::new(peers.clone(), NoFaultInjector),
            RaftActor::new(peers.clone(), DoubleVoteFaultInjector),
        ];

        let discovery = create_raft_actor_model()
            .actors(actors)
            .checker()
            .target_max_depth(10)
            .threads(num_cpus::get())
            .spawn_bfs()
            .join()
            .discovery_classification(property::ELECTION_SAFETY);

        assert!(matches!(discovery, Counterexample));
    }
}
