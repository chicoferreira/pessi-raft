use crate::fault::actor::{RaftActor, StaterightMessage};
use crate::fault::injector::FaultInjector;
use crate::raft;
use crate::raft::LogEntry;
use stateright::actor::{Id, Out};
use std::collections::HashMap;
use std::hash::Hash;
use std::ops::ControlFlow;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum ForkingLogFixMessage {
    Gossip {
        leader: Id,
        index: usize,
        suffix: Vec<LogEntry<Id>>,
    },
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct ForkingLogFixState {
    seen: HashMap<(Id, usize), Vec<LogEntry<Id>>>,
    blacklist: Vec<Id>,
}

impl Hash for ForkingLogFixState {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        raft::hash_hashmap(&self.seen, state);
        self.blacklist.hash(state);
    }
}

pub struct ForkingLogFixInjector;

impl FaultInjector<ForkingLogFixMessage, ForkingLogFixState> for ForkingLogFixInjector {
    fn inject_on_msg(
        &self,
        node_state: &mut raft::Node<Id>,
        other_state: &mut ForkingLogFixState,
        msg: StaterightMessage<ForkingLogFixMessage>,
        out: &mut Out<RaftActor<ForkingLogFixMessage, ForkingLogFixState>>,
    ) -> ControlFlow<(), StaterightMessage<ForkingLogFixMessage>> {
        let (_, _, _, _) = (node_state, other_state, msg, out);
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fault::actor::RaftActor;
    use crate::fault::forking_log::ForkingLogFaultInjector;
    use crate::transport::ClientRequest;
    use stateright::Expectation::{Always, Sometimes};
    use stateright::actor::{ActorModel, Envelope, Network};
    use stateright::{Checker, Model};

    #[test]
    fn test_forking_log_fix() {
        let peers: Vec<Id> = Id::vec_from(0..3);
        let actors = vec![
            RaftActor::new(peers.clone(), ForkingLogFaultInjector),
            RaftActor::new(peers.clone(), ForkingLogFixInjector),
            RaftActor::new(peers.clone(), ForkingLogFixInjector),
        ];

        let network = Network::new_ordered(vec![Envelope {
            src: Id::from(0),
            dst: Id::from(1),
            msg: StaterightMessage::ClientRequest(ClientRequest::Write {
                key: 1,
                value: 42,
                msg_id: 1,
            }),
        }]);

        ActorModel::new((), ())
            .init_network(network)
            .actors(actors)
            .property(Always, "Only Faulty Node Gets Blacklisted", |_, state| {
                state.actor_states.iter().all(|state| {
                    let (_, _, state) = state.as_ref();
                    // Either is empty or only have id 0 blacklisted
                    state.blacklist.iter().all(|id| id == &Id::from(0))
                })
            })
            .property(Sometimes, "Faulty Node Blacklisted", |_, state| {
                state.actor_states.iter().any(|state| {
                    let (_, _, state) = state.as_ref();
                    // At least one node has id 0 blacklisted
                    state.blacklist.contains(&Id::from(0))
                })
            })
            .checker()
            .target_max_depth(18)
            .threads(num_cpus::get())
            .spawn_bfs()
            .join()
            .assert_properties();
    }
}
