use crate::fault::actor::{RaftActor, StaterightMessage};
use crate::fault::injector::FaultInjector;
use crate::raft::{Node, RaftMessage, VoteResponseMessage};
use stateright::actor::{Id, Out};
use std::ops::ControlFlow;

struct DoubleVoteFaultInjector;

impl FaultInjector for DoubleVoteFaultInjector {
    fn inject_on_msg(
        &self,
        state: &mut Node<Id>,
        msg: StaterightMessage,
        out: &mut Out<RaftActor>,
    ) -> ControlFlow<(), StaterightMessage> {
        if let StaterightMessage::Raft(RaftMessage::VoteRequest(vote_request_message)) = msg {
            // if the message is a vote request, always accept
            let vote_response_message = RaftMessage::VoteResponse(VoteResponseMessage {
                node_id: state.get_id().clone(),
                current_term: state.current_term(),
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
    use stateright::Checker;
    use stateright::DiscoveryClassification::Counterexample;
    use stateright::Model;
    use std::sync::Arc;

    #[test]
    fn test_double_vote_fault() {
        let peers: Vec<Id> = Id::vec_from(0..3);

        let actors = vec![
            RaftActor {
                fault_injector: Arc::new(NoFaultInjector),
                node_ids: peers.clone(),
            },
            RaftActor {
                fault_injector: Arc::new(NoFaultInjector),
                node_ids: peers.clone(),
            },
            RaftActor {
                fault_injector: Arc::new(DoubleVoteFaultInjector),
                node_ids: peers.clone(),
            },
        ];

        let discovery = create_raft_actor_model()
            .actors(actors)
            .checker()
            .target_max_depth(10)
            .threads(num_cpus::get())
            .spawn_bfs()
            .join()
            .discovery_classification("Election Safety");

        assert!(matches!(discovery, Counterexample));
    }
}
