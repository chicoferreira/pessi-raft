use crate::fault::actor::{RaftActor, StaterightMessage};
use crate::fault::injector::FaultInjector;
use crate::raft::{Node, RaftEvent, RaftMessage, VoteResponseMessage};
use stateright::actor::{Id, Out};
use std::ops::ControlFlow;

struct MessageForgeryFault;

impl FaultInjector<(), ()> for MessageForgeryFault {
    fn inject_on_event(
        &self,
        node_state: &mut Node<Id>,
        _other_state: &mut (),
        out: &mut Out<RaftActor<(), ()>>,
        event: RaftEvent<Id>,
    ) -> ControlFlow<(), RaftEvent<Id>> {
        match event {
            RaftEvent::ElectionStarted => {
                let my_id = node_state.get_id();
                for id in node_state.nodes().iter().filter(|id| *id != my_id) {
                    let forged_msg =
                        StaterightMessage::Raft(RaftMessage::VoteResponse(VoteResponseMessage {
                            node_id: *id,
                            current_term: node_state.current_term(),
                            vote_granted: true,
                        }));

                    out.send(*my_id, forged_msg);
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
    use crate::fault::actor::{RaftActor, create_raft_actor_model};
    use crate::fault::injector::NoFaultInjector;
    use crate::fault::property;
    use DiscoveryClassification::Counterexample;
    use stateright::Checker;
    use stateright::{DiscoveryClassification, Model};

    #[test]
    fn test_election_spam() {
        let peers: Vec<Id> = Id::vec_from(0..3);

        let actors = vec![
            RaftActor::<(), ()>::new(peers.clone(), MessageForgeryFault),
            RaftActor::new(peers.clone(), NoFaultInjector),
            RaftActor::new(peers.clone(), NoFaultInjector),
        ];

        let classification = create_raft_actor_model()
            .actors(actors)
            .checker()
            .target_max_depth(15)
            .threads(num_cpus::get())
            // .serve("localhost:3000");
            .spawn_dfs()
            .join()
            .discovery_classification(property::ELECTION_SAFETY);

        // TODO: check why assert_properties is working

        assert!(matches!(classification, Counterexample));
    }
}
