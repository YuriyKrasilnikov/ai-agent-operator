// Copyright 2026 Yuriy Krasilnikov
// SPDX-License-Identifier: Apache-2.0

//! Derives continuation evidence solely from successful operator operations.

use crate::contract::control::{
    OperatorError, SessionEvidence, SessionInspectRequest, SessionInventoryRequest, StatePort,
};

use super::project;

pub(crate) fn inventory(
    state: &dyn StatePort,
    request: SessionInventoryRequest,
) -> Result<Vec<SessionEvidence>, OperatorError> {
    project::get(state, &request.project_id)?;
    for_project(state, &request.project_id)
}

pub(crate) fn inspect(
    state: &dyn StatePort,
    request: SessionInspectRequest,
) -> Result<Vec<SessionEvidence>, OperatorError> {
    project::get(state, &request.project_id)?;
    let evidence = for_exact_session(state, &request.project_id, request.target_session_id)?;
    if evidence.is_empty() {
        return Err(OperatorError::UnknownSession {
            project_id: request.project_id,
            target_session_id: request.target_session_id,
        });
    }
    Ok(evidence)
}

pub(crate) fn for_project(
    state: &dyn StatePort,
    project_id: &crate::contract::control::ProjectId,
) -> Result<Vec<SessionEvidence>, OperatorError> {
    state.list_session_evidence(project_id)
}

pub(crate) fn for_exact_session(
    state: &dyn StatePort,
    project_id: &crate::contract::control::ProjectId,
    target_session_id: crate::contract::control::SessionId,
) -> Result<Vec<SessionEvidence>, OperatorError> {
    state.inspect_session_evidence(project_id, target_session_id)
}
