use crate::fault::actor::RaftActor;
use crate::raft::{Node, RaftEvent};
use pollster::FutureExt;
use stateright::actor::{Id, Out};

use super::injector::FaultInjector;

struct ElectionSpamFault;

impl FaultInjector<(), ()> for ElectionSpamFault {
    fn inject_on_event(
        &self,
        node_state: &mut Node<Id>,
        _other_state: &mut (),
        out: &mut Out<RaftActor<(), ()>>,
        event: RaftEvent<Id>,
    ) -> std::ops::ControlFlow<(), RaftEvent<Id>> {
        match event {
            RaftEvent::LeaderElected { .. } => {
                node_state.start_election(out).block_on().unwrap();
            }
        }
        std::ops::ControlFlow::Continue(event)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fault::actor::{create_raft_actor_model, RaftActor};
    use crate::fault::injector::NoFaultInjector;
    use crate::fault::property;
    use stateright::{Checker, Model};

    #[test]
    fn test_election_spam() {
        let peers: Vec<Id> = Id::vec_from(0..3);

        let actors = vec![
            RaftActor::<(), ()>::new(peers.clone(), NoFaultInjector),
            RaftActor::new(peers.clone(), NoFaultInjector),
            RaftActor::new(peers.clone(), NoFaultInjector),
        ];

        create_raft_actor_model()
            .actors(actors)
            .checker()
            .target_max_depth(12)
            .threads(num_cpus::get())
            .spawn_dfs()
            .join()
            .assert_no_discovery(property::LOG_LIVENESS)
    }
}
