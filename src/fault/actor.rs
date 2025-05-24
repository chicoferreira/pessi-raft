use crate::fault::injector::FaultInjector;
use crate::fault::property;
use crate::raft;
use crate::raft::RaftEvent::LeaderElected;
use crate::raft::{RaftError, RaftMessage, Role};
use crate::transport::{ClientRequest, ClientResponse, RaftTransport};
use pollster::FutureExt;
use stateright::actor::{Actor, ActorModel, Envelope, Id, Network, Out, model_timeout};
use std::borrow::Cow;
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;
use std::mem;
use std::ops::ControlFlow;
use std::sync::Arc;

pub struct RaftActor<OtherMsg, OtherState> {
    pub fault_injector: Arc<dyn FaultInjector<OtherMsg, OtherState> + Send + Sync>,
    pub node_ids: Vec<Id>,
    _pd: PhantomData<(OtherMsg, OtherState)>,
}

impl<OtherMsg, OtherState> RaftActor<OtherMsg, OtherState>
where
    OtherMsg: Clone + Debug + Eq + Hash,
    OtherState: Default + Clone + Debug + Hash + PartialEq,
{
    pub fn new(
        node_ids: Vec<Id>,
        fault_injector: impl FaultInjector<OtherMsg, OtherState> + Send + Sync + 'static,
    ) -> Self {
        Self {
            fault_injector: Arc::new(fault_injector),
            node_ids,
            _pd: PhantomData,
        }
    }
}

#[derive(Clone, Hash, PartialEq, Eq, Debug)]
pub enum RaftTicker {
    ReplicationTimeout,
    ElectionTimeout,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum StaterightMessage<Other> {
    Raft(RaftMessage<Id>),
    ClientResponse(ClientResponse),
    ClientRequest(ClientRequest),
    // Other message to be able to fix implementations be able to add their own messages
    Other(Other),
}

impl<A, Other> RaftTransport<Id> for Out<A>
where
    A: Actor<Msg = StaterightMessage<Other>>,
{
    async fn send_raft_message(
        &mut self,
        _: Id,
        to: Id,
        message: RaftMessage<Id>,
    ) -> anyhow::Result<()> {
        self.send(to, StaterightMessage::Raft(message));
        Ok(())
    }

    async fn send_client_response(
        &mut self,
        _: Id,
        to: Id,
        res: ClientResponse,
    ) -> anyhow::Result<()> {
        self.send(to, StaterightMessage::ClientResponse(res));
        Ok(())
    }
}

impl<OtherMsg: Clone + Debug + Eq + Hash, OtherState: Clone + Debug + Hash + PartialEq + Default>
    Actor for RaftActor<OtherMsg, OtherState>
{
    type Msg = StaterightMessage<OtherMsg>;
    type Timer = RaftTicker;
    type State = (raft::Node<Id>, Vec<StaterightMessage<OtherMsg>>, OtherState);

    fn on_start(&self, id: Id, o: &mut Out<Self>) -> Self::State {
        o.set_timer(RaftTicker::ReplicationTimeout, model_timeout());
        o.set_timer(RaftTicker::ElectionTimeout, model_timeout());
        let node_ids = self.node_ids.clone();
        (
            raft::Node::new(id, node_ids),
            Vec::new(),
            OtherState::default(),
        )
    }

    fn on_msg(
        &self,
        _id: Id,
        state: &mut Cow<Self::State>,
        src: Id,
        msg: Self::Msg,
        out: &mut Out<Self>,
    ) {
        let (state, pending_requests, other_state) = state.to_mut();

        let msg = match self
            .fault_injector
            .inject_on_msg(state, other_state, msg, out)
        {
            ControlFlow::Continue(msg) => msg,
            ControlFlow::Break(()) => return,
        };

        match msg {
            StaterightMessage::Raft(raft_msg) => {
                let event = state.handle_raft_message(out, raft_msg).block_on().unwrap();

                if let Some(event) = event {
                    let event =
                        match self
                            .fault_injector
                            .inject_on_event(state, other_state, out, event)
                        {
                            ControlFlow::Continue(event) => event,
                            ControlFlow::Break(()) => return,
                        };

                    if let LeaderElected { leader } = event {
                        let pending_requests = mem::take(pending_requests);
                        for pending_request in pending_requests {
                            out.send(leader, pending_request);
                        }
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
            _ => {}
        }
        self.fault_injector
            .inject_on_post_msg(state, other_state, out);
    }

    fn on_timeout(
        &self,
        _id: Id,
        state: &mut Cow<Self::State>,
        timer: &Self::Timer,
        out: &mut Out<Self>,
    ) {
        let (state, pending_requests, other_state) = state.to_mut();

        if self
            .fault_injector
            .inject_on_timeout(state, other_state)
            .is_break()
        {
            return;
        }

        match timer {
            RaftTicker::ReplicationTimeout => {
                state.broadcast_replicate_log(out).block_on().unwrap();
            }
            RaftTicker::ElectionTimeout => {
                if state.current_role() != Role::Leader {
                    let event = state.start_election(out).block_on().unwrap();
                    let event = self
                        .fault_injector
                        .inject_on_event(state, other_state, out, event);
                    let ControlFlow::Continue(event) = event else {
                        return;
                    };

                    if let LeaderElected { leader } = event {
                        let pending_requests = mem::take(pending_requests);
                        for pending_request in pending_requests {
                            out.send(leader, pending_request);
                        }
                    }
                }
            }
        }
    }
}

pub fn create_raft_actor_model<OtherMsg, OtherState>() -> ActorModel<RaftActor<OtherMsg, OtherState>>
where
    OtherMsg: Clone + Debug + Eq + Hash,
    OtherState: Clone + Debug + Hash + PartialEq + Default,
{
    let network = Network::new_ordered(vec![Envelope {
        src: Id::from(0),
        dst: Id::from(1),
        msg: StaterightMessage::<OtherMsg>::ClientRequest(ClientRequest::Write {
            key: 1,
            value: 42,
            msg_id: 1,
        }),
    }]);

    property::add_raft_properties(ActorModel::new((), ())).init_network(network)
}

#[cfg(test)]
mod tests {
    use crate::fault::actor::{RaftActor, create_raft_actor_model};
    use crate::fault::injector::NoFaultInjector;
    use stateright::actor::Id;
    use stateright::{Checker, Model};

    #[test]
    fn test_linearizability() {
        let server_count = 3;
        let peers: Vec<Id> = Id::vec_from(0..server_count);

        create_raft_actor_model::<(), ()>()
            // .max_crashes((self.server_count - 1) / 2)
            .actors((0..server_count).map(|_| RaftActor::new(peers.clone(), NoFaultInjector)))
            .checker()
            .target_max_depth(15)
            .threads(num_cpus::get())
            // .serve("localhost:3000");
            .spawn_bfs()
            .join()
            .assert_properties()
    }
}
