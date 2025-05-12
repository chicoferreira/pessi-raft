use crate::fault::injector::FaultInjector;
use crate::fault::property;
use crate::raft;
use crate::raft::RaftEvent::LeaderElected;
use crate::raft::{RaftError, RaftMessage, Role};
use crate::transport::{ClientRequest, ClientResponse, RaftTransport};
use pollster::FutureExt;
use stateright::actor::{model_timeout, Actor, ActorModel, Id, Out};
use std::borrow::Cow;
use std::fmt::Debug;
use std::hash::Hash;
use std::mem;
use std::ops::ControlFlow;
use std::sync::Arc;

pub struct RaftActor {
    pub fault_injector: Arc<dyn FaultInjector + Send + Sync>,
    pub node_ids: Vec<Id>,
}

#[derive(Clone, Hash, PartialEq, Eq, Debug)]
pub enum RaftTicker {
    ReplicationTimeout,
    ElectionTimeout,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum StaterightMessage {
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
    type State = (raft::Node<Id>, Vec<StaterightMessage>);

    fn on_start(&self, id: Id, o: &mut Out<Self>) -> Self::State {
        o.set_timer(RaftTicker::ReplicationTimeout, model_timeout());
        o.set_timer(RaftTicker::ElectionTimeout, model_timeout());
        let node_ids = self.node_ids.clone();
        (raft::Node::new(id, node_ids), Vec::new())
    }

    fn on_msg(
        &self,
        _id: Id,
        state: &mut Cow<Self::State>,
        src: Id,
        msg: Self::Msg,
        out: &mut Out<Self>,
    ) {
        let (state, pending_requests) = state.to_mut();

        let msg = match self.fault_injector.inject_on_msg(state, msg, out) {
            ControlFlow::Continue(msg) => msg,
            ControlFlow::Break(()) => return,
        };

        match msg {
            StaterightMessage::Raft(raft_msg) => {
                let event = state.handle_raft_message(out, raft_msg).block_on().unwrap();

                if let Some(LeaderElected { leader }) = event {
                    let pending_requests = mem::take(pending_requests);
                    for pending_request in pending_requests {
                        out.send(leader, pending_request);
                    }
                }
            }
            StaterightMessage::ClientRequest(request_msg) => {
                let request_msg_clone = request_msg.clone();
                match state.append_to_log(out, src, request_msg_clone).block_on() {
                    Err(RaftError::NotLeader { leader }) => {
                        if let Some(leader) = leader {
                            out.send(leader, StaterightMessage::ClientRequest(request_msg));
                        } else {
                            // No leader elected yet, so we need to store the request
                            pending_requests.push(StaterightMessage::ClientRequest(request_msg));
                        }
                    }
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
        let (state, _pending_requests) = state.to_mut();

        if self.fault_injector.inject_on_timeout(state).is_break() {
            return;
        }

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

pub fn create_raft_actor_model() -> ActorModel<RaftActor> {
    property::add_raft_properties(ActorModel::new((), ()))
}

#[cfg(test)]
mod tests {
    use crate::fault::actor::{create_raft_actor_model, RaftActor, StaterightMessage};
    use crate::fault::injector::NoFaultInjector;
    use crate::transport::ClientRequest;
    use stateright::actor::{Envelope, Id, Network};
    use stateright::Checker;
    use stateright::Model;
    use std::sync::Arc;

    #[test]
    fn test_linearizability() {
        let network = Network::new_ordered(vec![Envelope {
            src: Id::from(0),
            dst: Id::from(1),
            msg: StaterightMessage::ClientRequest(ClientRequest::Write {
                key: 1,
                value: 42,
                msg_id: 1,
            }),
        }]);

        let server_count = 3;
        let peers: Vec<Id> = Id::vec_from(0..server_count);

        create_raft_actor_model()
            // .max_crashes((self.server_count - 1) / 2)
            .actors((0..server_count).map(|_| RaftActor {
                fault_injector: Arc::new(NoFaultInjector),
                node_ids: peers.clone(),
            }))
            .init_network(network)
            .checker()
            .target_max_depth(15)
            .threads(num_cpus::get())
            // .serve("localhost:3000");
            .spawn_bfs()
            .join()
            .assert_properties()
    }
}
