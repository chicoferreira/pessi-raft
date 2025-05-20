use crate::fault::actor::RaftActor;
use crate::raft::Role;
use stateright::actor::{ActorModel, ActorModelState};
use stateright::Expectation::{Always, Sometimes};
use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;

pub fn election_safety_property<Cfg, OtherMsg, OtherState>(
    _actor: &ActorModel<RaftActor<OtherMsg, OtherState>, Cfg>,
    state: &ActorModelState<RaftActor<OtherMsg, OtherState>>,
) -> bool
where
    OtherMsg: Clone + Debug + Eq + Hash,
    OtherState: Default + Clone + Debug + Hash + PartialEq,
{
    // at most one leader can be elected in a given term

    let mut leaders_term = HashSet::new();
    for s in &state.actor_states {
        if s.0.current_role() == Role::Leader && !leaders_term.insert(s.0.current_term()) {
            return false;
        }
    }
    true
}

pub fn log_safety_property<Cfg, OtherMsg, OtherState>(
    _actor: &ActorModel<RaftActor<OtherMsg, OtherState>, Cfg>,
    state: &ActorModelState<RaftActor<OtherMsg, OtherState>>,
) -> bool
where
    OtherMsg: Clone + Debug + Eq + Hash,
    OtherState: Default + Clone + Debug + Hash + PartialEq,
{
    // if a server has applied a log entry at a given index to its state machine, no other server will
    // ever apply a different log entry for the same index.

    let mut max_commit_length = 0;
    let mut max_commit_length_actor_id = 0;
    for (i, s) in state.actor_states.iter().enumerate() {
        if s.0.log().len() > max_commit_length {
            max_commit_length = s.0.log().len();
            max_commit_length_actor_id = i;
        }
    }
    if max_commit_length == 0 {
        return true;
    }

    for i in 0..max_commit_length {
        let ref_log = state.actor_states[max_commit_length_actor_id]
            .0
            .log()
            .get(i)
            .unwrap();
        for s in &state.actor_states {
            if let Some(log) = s.0.log().get(i) {
                if log != ref_log {
                    return false;
                }
            }
        }
    }
    true
}

pub fn log_liveness_property<Cfg, OtherMsg, OtherState>(
    _actor: &ActorModel<RaftActor<OtherMsg, OtherState>, Cfg>,
    state: &ActorModelState<RaftActor<OtherMsg, OtherState>>,
) -> bool
where
    OtherMsg: Clone + Debug + Eq + Hash,
    OtherState: Default + Clone + Debug + Hash + PartialEq,
{
    state.actor_states.iter().any(|s| s.0.commit_length() > 0)
}

pub fn election_liveness_property<Cfg, OtherMsg, OtherState>(
    _actor: &ActorModel<RaftActor<OtherMsg, OtherState>, Cfg>,
    state: &ActorModelState<RaftActor<OtherMsg, OtherState>>,
) -> bool
where
    OtherMsg: Clone + Debug + Eq + Hash,
    OtherState: Default + Clone + Debug + Hash + PartialEq,
{
    state
        .actor_states
        .iter()
        .any(|s| s.0.current_role() == Role::Leader)
}

pub const ELECTION_LIVENESS: &str = "Election Liveness";
pub const LOG_LIVENESS: &str = "Log Liveness";
pub const ELECTION_SAFETY: &str = "Election Safety";
pub const LOG_SAFETY: &str = "Log Safety";

pub fn add_raft_properties<Cfg, OtherMsg, OtherState>(
    target: ActorModel<RaftActor<OtherMsg, OtherState>, Cfg, ()>,
) -> ActorModel<RaftActor<OtherMsg, OtherState>, Cfg>
where
    OtherMsg: Clone + Debug + Eq + Hash,
    OtherState: Default + Clone + Debug + Hash + PartialEq,
{
    target
        .property(Sometimes, ELECTION_LIVENESS, election_liveness_property)
        .property(Sometimes, LOG_LIVENESS, log_liveness_property)
        .property(Always, ELECTION_SAFETY, election_safety_property)
        .property(Always, LOG_SAFETY, log_safety_property)
}
