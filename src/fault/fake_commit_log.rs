use crate::fault::actor::RaftActor;
use crate::fault::injector::FaultInjector;
use crate::raft::Node;
use crate::raft::Role::Leader;
use stateright::actor::{Id, Out};

struct FakeLogCommitFault;

impl FaultInjector<(), ()> for FakeLogCommitFault {
    fn inject_on_post_msg(
        &self,
        node_state: &mut Node<Id>,
        _: &mut (),
        _: &mut Out<RaftActor<(), ()>>,
    ) {
        if node_state.current_role() == Leader {
            // if node_state.log().len() == 0 {
            //     return ControlFlow::Continue(());
            // }
            //
            // node_state.log_mut_ref()[0] = LogEntry {
            //     term: 0,
            //     from: Id::from(0),
            //     request: ClientRequest::Read { key: 0, msg_id: 0 },
            // };

            *node_state.commit_length_mut_ref() = node_state.log().len();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fault::actor::{RaftActor, StaterightMessage};
    use crate::fault::injector::NoFaultInjector;
    use crate::fault::property;
    use crate::transport::ClientRequest;
    use stateright::Checker;
    use stateright::Model;
    use stateright::actor::{ActorModel, Envelope, Network};

    #[test]
    fn test_fake_log_commit() {
        let peers: Vec<Id> = Id::vec_from(0..5);

        let _actors = vec![
            RaftActor::<(), ()>::new(peers.clone(), FakeLogCommitFault),
            RaftActor::new(peers.clone(), NoFaultInjector),
            RaftActor::new(peers.clone(), NoFaultInjector),
            RaftActor::new(peers.clone(), NoFaultInjector),
            RaftActor::new(peers.clone(), NoFaultInjector),
        ];

        let _network = Network::new_unordered_duplicating(vec![
            Envelope {
                src: Id::from(1),
                dst: Id::from(0),
                msg: StaterightMessage::<()>::ClientRequest(ClientRequest::Write {
                    key: 1,
                    value: 42,
                    msg_id: 1,
                }),
            },
            Envelope {
                src: Id::from(1),
                dst: Id::from(0),
                msg: StaterightMessage::<()>::ClientRequest(ClientRequest::Write {
                    key: 2,
                    value: 43,
                    msg_id: 2,
                }),
            },
        ]);

        // let classification = property::add_raft_properties(ActorModel::new((), ()))
        //     .init_network(network)
        //     .actors(actors)
        //     .max_crashes(1)
        //     .checker()
        //     .target_max_depth(11)
        //     .threads(num_cpus::get())
        //     // .serve("localhost:3000");
        //     .spawn_dfs()
        //     .join()
        //     .discovery_classification(property::LOG_SAFETY);
        // //
        // assert!(matches!(
        //     classification,
        //     stateright::DiscoveryClassification::Counterexample
        // ));
    }
}
