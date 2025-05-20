use super::injector::FaultInjector;
use crate::fault::actor::{RaftActor, StaterightMessage};
use crate::raft::{Node, RaftEvent};
use pollster::FutureExt;
use stateright::actor::{Id, Out};
use std::fmt::Debug;
use std::hash::Hash;
use std::ops::ControlFlow;

pub struct ElectionSpamFault;

impl<OtherMsg, OtherState> FaultInjector<OtherMsg, OtherState> for ElectionSpamFault
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
        node_state.start_election(out).block_on().unwrap();
        ControlFlow::Continue(msg)
    }

    fn inject_on_event(
        &self,
        node_state: &mut Node<Id>,
        _other_state: &mut OtherState,
        out: &mut Out<RaftActor<OtherMsg, OtherState>>,
        event: RaftEvent<Id>,
    ) -> ControlFlow<(), RaftEvent<Id>> {
        match event {
            RaftEvent::LeaderElected { .. } => {
                node_state.start_election(out).block_on().unwrap();
            }
        }
        ControlFlow::Continue(event)
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
            RaftActor::<(), ()>::new(peers.clone(), ElectionSpamFault),
            RaftActor::new(peers.clone(), ElectionSpamFault),
            RaftActor::new(peers.clone(), NoFaultInjector),
        ];

        create_raft_actor_model()
            .actors(actors)
            .checker()
            .target_max_depth(12)
            .threads(num_cpus::get())
            // .serve("localhost:3000");
            .spawn_dfs()
            .join()
            .assert_no_discovery(property::LOG_LIVENESS)
    }
}
