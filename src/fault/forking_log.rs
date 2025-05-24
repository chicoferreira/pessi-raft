use crate::fault::actor::{RaftActor, StaterightMessage};
use crate::fault::injector::FaultInjector;
use crate::raft::{Node, RaftMessage};
use crate::transport::ClientRequest;
use stateright::actor::{Id, Out};
use std::fmt::Debug;
use std::hash::Hash;
use std::ops::ControlFlow;

/// A fault injector that causes the leader to equivocate by forking the log:
/// followers receive a mutated AppendEntries (LogRequest) suffix.
pub struct ForkingLogFaultInjector;

impl<OtherMsg, OtherState> FaultInjector<OtherMsg, OtherState> for ForkingLogFaultInjector
where
    OtherMsg: Clone + Debug + Eq + Hash,
    OtherState: Default + Clone + Debug + Hash + PartialEq,
{
    fn inject_on_msg(
        &self,
        _node_state: &mut Node<Id>,
        _other_state: &mut OtherState,
        msg: StaterightMessage<OtherMsg>,
        _out: &mut Out<RaftActor<OtherMsg, OtherState>>,
    ) -> ControlFlow<(), StaterightMessage<OtherMsg>> {
        match msg {
            StaterightMessage::Raft(RaftMessage::LogRequest(mut req)) => {
                for entry in req.suffix.iter_mut() {
                    if let ClientRequest::Write { value, .. } = &mut entry.request {
                        *value = value.wrapping_add(1);
                    }
                }
                ControlFlow::Continue(StaterightMessage::Raft(RaftMessage::LogRequest(req)))
            }
            other => ControlFlow::Continue(other),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fault::actor::{RaftActor, create_raft_actor_model};
    use crate::fault::injector::NoFaultInjector;
    use crate::fault::property;
    use stateright::Checker;
    use stateright::DiscoveryClassification::Counterexample;
    use stateright::Model;

    #[test]
    fn test_forking_log_fault() {
        let peers: Vec<Id> = Id::vec_from(0..3);
        let actors = vec![
            RaftActor::new(peers.clone(), ForkingLogFaultInjector),
            RaftActor::<(), ()>::new(peers.clone(), NoFaultInjector),
            RaftActor::new(peers.clone(), NoFaultInjector),
        ];

        let classification = create_raft_actor_model()
            .actors(actors)
            .checker()
            .target_max_depth(12)
            .threads(num_cpus::get())
            .spawn_bfs()
            .join()
            .discovery_classification(property::LOG_SAFETY);

        assert!(matches!(classification, Counterexample));
    }
}
