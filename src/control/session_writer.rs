// Copyright 2026 Yuriy Krasilnikov
// SPDX-License-Identifier: Apache-2.0

//! Interprets atomic persistence facts for one current session writer.

use crate::contract::control::{
    Operation, OperationAdmission, OperationIntent, OperationStart, OperationState, OperatorError,
    SessionId, StatePort,
};

pub fn admit(
    state: &dyn StatePort,
    request: &OperationStart,
    fingerprint: &str,
) -> Result<(Operation, bool), OperatorError> {
    let session = session_for(&request.intent);
    match state.persist_operation_admission(request, session, fingerprint)? {
        OperationAdmission::Existing {
            operation,
            fingerprint: existing_fingerprint,
        } if existing_fingerprint == fingerprint => Ok((operation, false)),
        OperationAdmission::Existing { .. } => Err(OperatorError::Conflict(
            "request_id was already used with a different complete request".to_owned(),
        )),
        OperationAdmission::MissingProject => Err(OperatorError::UnknownProject(
            request.project_id.as_str().to_owned(),
        )),
        OperationAdmission::ActiveSession { operation } => active_writer_error(operation),
        OperationAdmission::Inserted(operation) => Ok((operation, true)),
    }
}

fn session_for(intent: &OperationIntent) -> SessionId {
    match intent {
        OperationIntent::New => SessionId::new(),
        OperationIntent::ResumeExact { session_id } => *session_id,
    }
}

fn active_writer_error(operation: Operation) -> Result<(Operation, bool), OperatorError> {
    match operation.state {
        OperationState::Accepted | OperationState::Running => Err(OperatorError::Conflict(
            "session already has an active writer".to_owned(),
        )),
        OperationState::Indeterminate => Err(OperatorError::UnclassifiedSession(
            "session was held by an operation interrupted by daemon restart".to_owned(),
        )),
        OperationState::Succeeded | OperationState::Failed | OperationState::Cancelled => Err(
            OperatorError::State("terminal operation retained an active writer claim".to_owned()),
        ),
    }
}
