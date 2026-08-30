// Copyright 2026 Yuriy Krasilnikov
// SPDX-License-Identifier: Apache-2.0

//! Evaluates the total, effect-free exact-session decision algebra.

use crate::contract::control::{
    InitiatorBinding, OperatorError, SessionDecision, SessionDecisionEvidence,
    SessionDecisionRequest, SessionEvidence, SessionRefusalReason, StatePort,
};
use std::collections::BTreeSet;

use super::{project, session_inventory};

pub(crate) fn decide(
    state: &dyn StatePort,
    request: SessionDecisionRequest,
) -> Result<SessionDecision, OperatorError> {
    project::get(state, &request.project_id)?;
    match request.continuity {
        crate::contract::control::SessionContinuity::Independent => Ok(SessionDecision::New {
            evidence: SessionDecisionEvidence::Independent,
        }),
        crate::contract::control::SessionContinuity::ContinueBound => {
            continue_bound(state, request)
        }
    }
}

fn continue_bound(
    state: &dyn StatePort,
    request: SessionDecisionRequest,
) -> Result<SessionDecision, OperatorError> {
    match state.get_initiator_binding(&request.project_id, &request.identity)? {
        Some(binding) => decide_binding(state, binding),
        None => decide_unbound(state, request),
    }
}

fn decide_binding(
    state: &dyn StatePort,
    binding: InitiatorBinding,
) -> Result<SessionDecision, OperatorError> {
    let evidence = session_inventory::for_exact_session(
        state,
        &binding.project_id,
        binding.target_session_id,
    )?;
    if evidence.is_empty() {
        return Err(OperatorError::BoundSessionEvidenceMissing {
            target_session_id: binding.target_session_id,
            binding: Box::new(binding),
        });
    }
    Ok(SessionDecision::ResumeExact {
        target_session_id: binding.target_session_id,
        evidence_operation_ids: operation_ids(evidence),
    })
}

fn decide_unbound(
    state: &dyn StatePort,
    request: SessionDecisionRequest,
) -> Result<SessionDecision, OperatorError> {
    let initiator_bindings = state.list_initiator_bindings_for_initiator(
        &request.project_id,
        &request.identity.initiator_session_id,
        &request.identity.initiator_agent_id,
    )?;
    if !initiator_bindings.is_empty() {
        return Ok(SessionDecision::Refuse {
            reason: SessionRefusalReason::IdentityMismatch,
            evidence: SessionDecisionEvidence::IdentityBindings {
                bindings: initiator_bindings,
            },
        });
    }
    let evidence = session_inventory::for_project(state, &request.project_id)?;
    let sessions = unique_sessions(evidence);
    if sessions.len() > 1 {
        return Ok(SessionDecision::Refuse {
            reason: SessionRefusalReason::AmbiguousSessions,
            evidence: SessionDecisionEvidence::CandidateSessions {
                target_session_ids: sessions,
            },
        });
    }
    Ok(SessionDecision::Refuse {
        reason: SessionRefusalReason::BindingRequired,
        evidence: SessionDecisionEvidence::CandidateSessions {
            target_session_ids: sessions,
        },
    })
}

fn operation_ids(evidence: Vec<SessionEvidence>) -> Vec<crate::contract::control::OperationId> {
    evidence.into_iter().map(|item| item.operation_id).collect()
}

fn unique_sessions(evidence: Vec<SessionEvidence>) -> Vec<crate::contract::control::SessionId> {
    evidence
        .into_iter()
        .map(|item| item.target_session_id.value())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(crate::contract::control::SessionId::new_exact)
        .collect()
}
