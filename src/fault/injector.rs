use crate::fault::actor::{RaftActor, StaterightMessage};
use crate::raft::{Node, RaftEvent};
use stateright::actor::{Id, Out};
use std::fmt::Debug;
use std::hash::Hash;
use std::ops::ControlFlow;

pub trait FaultInjector<OtherMsg, OtherState>
where
    OtherMsg: Clone + Debug + Eq + Hash,
    OtherState: Default + Clone + Debug + Hash + PartialEq,
{
    fn inject_on_msg(
        &self,
        node_state: &mut Node<Id>,
        other_state: &mut OtherState,
        msg: StaterightMessage<OtherMsg>,
        out: &mut Out<RaftActor<OtherMsg, OtherState>>,
    ) -> ControlFlow<(), StaterightMessage<OtherMsg>> {
        let _ = (node_state, other_state, out);
        ControlFlow::Continue(msg)
    }

    fn inject_on_post_msg(
        &self,
        node_state: &mut Node<Id>,
        other_state: &mut OtherState,
        out: &mut Out<RaftActor<OtherMsg, OtherState>>,
    ) {
        let _ = (node_state, other_state, out);
    }

    fn inject_on_timeout(
        &self,
        node_state: &mut Node<Id>,
        other_state: &mut OtherState,
    ) -> ControlFlow<()> {
        let _ = (node_state, other_state);
        ControlFlow::Continue(())
    }

    fn inject_on_event(
        &self,
        node_state: &mut Node<Id>,
        other_state: &mut OtherState,
        out: &mut Out<RaftActor<OtherMsg, OtherState>>,
        event: RaftEvent<Id>,
    ) -> ControlFlow<(), RaftEvent<Id>> {
        let _ = (node_state, other_state, out);
        ControlFlow::Continue(event)
    }
}

pub struct NoFaultInjector;

impl<OtherMsg, OtherState> FaultInjector<OtherMsg, OtherState> for NoFaultInjector
where
    OtherMsg: Clone + Debug + Eq + Hash,
    OtherState: Default + Clone + Debug + Hash + PartialEq,
{
}
