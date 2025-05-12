use crate::fault::actor::{RaftActor, StaterightMessage};
use crate::raft::Node;
use stateright::actor::{Id, Out};
use std::ops::ControlFlow;

pub trait FaultInjector {
    fn inject_on_msg(
        &self,
        _state: &mut Node<Id>,
        msg: StaterightMessage,
        _out: &mut Out<RaftActor>,
    ) -> ControlFlow<(), StaterightMessage> {
        ControlFlow::Continue(msg)
    }

    fn inject_on_timeout(&self, _state: &mut Node<Id>) -> ControlFlow<()> {
        ControlFlow::Continue(())
    }
}

pub struct NoFaultInjector;

impl FaultInjector for NoFaultInjector {}
