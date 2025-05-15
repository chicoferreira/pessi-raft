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
        _node_state: &mut Node<Id>,
        _other_state: &mut OtherState,
        msg: StaterightMessage<OtherMsg>,
        _out: &mut Out<RaftActor<OtherMsg, OtherState>>,
    ) -> ControlFlow<(), StaterightMessage<OtherMsg>> {
        ControlFlow::Continue(msg)
    }

    fn inject_on_timeout(
        &self,
        _node_state: &mut Node<Id>,
        _other_state: &mut OtherState,
    ) -> ControlFlow<()> {
        ControlFlow::Continue(())
    }

    fn inject_on_event(
        &self,
        _node_state: &mut Node<Id>,
        _other_state: &mut OtherState,
        event: RaftEvent<Id>,
    ) -> ControlFlow<(), RaftEvent<Id>> {
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
