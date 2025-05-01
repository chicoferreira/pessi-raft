use crate::raft;
use crate::raft::{RaftError, RaftMessage, Role};
use crate::transport::{ClientRequest, ClientResponse, RaftTransport};
use pollster::FutureExt;
use stateright::actor::{model_timeout, Actor, Id, Out};
use std::borrow::Cow;

struct RaftActor {
    node_ids: Vec<Id>,
}

#[derive(Clone, Hash, PartialEq, Eq, Debug)]
enum RaftTicker {
    ReplicationTimeout,
    ElectionTimeout,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
enum StaterightMessage {
    Raft(RaftMessage<Id>),
    ClientResponse(ClientResponse),
    ClientRequest(ClientRequest),
}

impl<A> RaftTransport<Id> for Out<A>
where
    A: Actor<Msg = StaterightMessage>,
{
    async fn send_raft_message(
        &mut self,
        _from: Id,
        to: Id,
        message: RaftMessage<Id>,
    ) -> anyhow::Result<()> {
        self.send(to, StaterightMessage::Raft(message));
        Ok(())
    }

    async fn send_client_response(
        &mut self,
        _from: Id,
        to: Id,
        response: ClientResponse,
    ) -> anyhow::Result<()> {
        self.send(to, StaterightMessage::ClientResponse(response));
        Ok(())
    }
}

impl Actor for RaftActor {
    type Msg = StaterightMessage;
    type Timer = RaftTicker;
    type State = raft::Node<Id>;

    fn on_start(&self, id: Id, o: &mut Out<Self>) -> Self::State {
        o.set_timer(RaftTicker::ReplicationTimeout, model_timeout());
        o.set_timer(RaftTicker::ElectionTimeout, model_timeout());
        let node_ids = self.node_ids.clone();
        raft::Node::new(id, node_ids)
    }

    fn on_msg(
        &self,
        _id: Id,
        state: &mut Cow<Self::State>,
        src: Id,
        msg: Self::Msg,
        o: &mut Out<Self>,
    ) {
        let state = state.to_mut();

        match msg {
            StaterightMessage::Raft(raft_msg) => {
                state.handle_raft_message(o, raft_msg).block_on().unwrap();
            }
            StaterightMessage::ClientRequest(request_msg) => {
                let request_msg_clone = request_msg.clone();
                match state.append_to_log(o, src, request_msg_clone).block_on() {
                    Err(RaftError::NotLeader {
                        leader: Some(leader),
                    }) => {
                        o.send(leader, StaterightMessage::ClientRequest(request_msg));
                    }
                    Err(RaftError::NotLeader { leader: None }) => {}
                    Err(e) => {
                        panic!("Error appending to log: {:?}", e);
                    }
                    Ok(()) => {}
                }
            }
            StaterightMessage::ClientResponse(_) => {}
        }
    }

    fn on_timeout(
        &self,
        _id: Id,
        state: &mut Cow<Self::State>,
        timer: &Self::Timer,
        o: &mut Out<Self>,
    ) {
        let state = state.to_mut();
        match timer {
            RaftTicker::ReplicationTimeout => {
                state.broadcast_replicate_log(o).block_on().unwrap();
            }
            RaftTicker::ElectionTimeout => {
                if state.current_role() != Role::Leader {
                    state.start_election(o).block_on().unwrap();
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::raft::Role;
    use crate::stateright::{RaftActor, StaterightMessage};
    use crate::transport::ClientRequest;
    use stateright::actor::{Actor, ActorModel, Envelope, Id, Network};
    use stateright::Expectation;
    use stateright::Model;
    use std::collections::HashSet;

    #[derive(Clone)]
    struct RaftModelCfg {
        server_count: usize,
        network: Network<<RaftActor as Actor>::Msg>,
    }

    impl RaftModelCfg {
        fn into_model(self) -> ActorModel<RaftActor, Self> {
            let peers: Vec<Id> = Id::vec_from(0..self.server_count);
            ActorModel::new(self.clone(), ())
                // .max_crashes((self.server_count - 1) / 2)
                .actors((0..self.server_count).map(|_| RaftActor {
                    node_ids: peers.clone(),
                }))
                .init_network(self.network)
                .property(Expectation::Sometimes, "Election Liveness", |_, state| {
                    state
                        .actor_states
                        .iter()
                        .any(|s| s.current_role() == Role::Leader)
                })
                .property(Expectation::Sometimes, "Log Liveness", |_, state| {
                    state.actor_states.iter().any(|s| s.commit_length() > 0)
                })
                .property(Expectation::Always, "Election Safety", |_, state| {
                    // at most one leader can be elected in a given term

                    let mut leaders_term = HashSet::new();
                    for s in &state.actor_states {
                        if s.current_role() == Role::Leader
                            && !leaders_term.insert(s.current_term())
                        {
                            return false;
                        }
                    }
                    true
                })
                .property(Expectation::Always, "State Machine Safety", |_, state| {
                    // if a server has applied a log entry at a given index to its state machine, no other server will
                    // ever apply a different log entry for the same index.

                    let mut max_commit_length = 0;
                    let mut max_commit_length_actor_id = 0;
                    for (i, s) in state.actor_states.iter().enumerate() {
                        if s.committed_log().len() > max_commit_length {
                            max_commit_length = s.committed_log().len();
                            max_commit_length_actor_id = i;
                        }
                    }
                    if max_commit_length == 0 {
                        return true;
                    }

                    for i in 0..max_commit_length {
                        let ref_log = state.actor_states[max_commit_length_actor_id]
                            .committed_log()
                            .get(&i)
                            .unwrap();
                        for s in &state.actor_states {
                            if let Some(log) = s.committed_log().get(&i) {
                                if log != ref_log {
                                    return false;
                                }
                            }
                        }
                    }
                    true
                })
        }
    }

    #[test]
    fn test_linearizability() {
        RaftModelCfg {
            server_count: 5,
            network: Network::new_ordered(vec![
                Envelope {
                    src: Id::from(0),
                    dst: Id::from(1),
                    msg: StaterightMessage::ClientRequest(ClientRequest::Write {
                        key: 1,
                        value: 42,
                        msg_id: 1,
                    }),
                },
            ]),
        }
        .into_model()
        .checker()
        // .threads(num_cpus::get())
        .serve("localhost:3000");
        // .spawn_dfs()
        // .join()
        // .assert_properties()
    }
}
