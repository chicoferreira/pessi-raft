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
    use crate::fault::actor::{RaftActor, create_raft_actor_model};
    use crate::fault::injector::NoFaultInjector;
    use stateright::Model;

    #[test]
    fn test_election_spam() {
        let peers: Vec<Id> = Id::vec_from(0..3);

        let actors = vec![
            RaftActor::<(), ()>::new(peers.clone(), FakeLogCommitFault),
            RaftActor::new(peers.clone(), NoFaultInjector),
            RaftActor::new(peers.clone(), NoFaultInjector),
        ];

        create_raft_actor_model()
            .actors(actors)
            .checker()
            // .target_max_depth(12)
            .threads(num_cpus::get())
            .serve("localhost:3000");
        // .spawn_dfs()
        // .join()
        // .discovery
    }
}
