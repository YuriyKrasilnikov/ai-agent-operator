// Copyright 2026 Yuriy Krasilnikov
// SPDX-License-Identifier: Apache-2.0

//! SS03: operation, idempotency, transition, and recovery persistence.
use super::{
    project, session_claim,
    sqlite::{Adapter, decode, encode, sql_error},
};
use crate::contract::control::{
    Operation, OperationAdmission, OperationId, OperationStart, OperationState, OperatorError,
    SessionId, TerminalOutcome,
};
use ::sqlite::{ConnectionThreadSafe, State};
pub(crate) fn get(a: &Adapter, id: OperationId) -> Result<Operation, OperatorError> {
    a.read(|c| by_id(c, id)?.ok_or_else(|| OperatorError::UnknownOperation(id.value().to_string())))
}
pub(crate) fn list_session_evidence(
    a: &Adapter,
    project_id: &crate::contract::control::ProjectId,
) -> Result<Vec<crate::contract::control::SessionEvidence>, OperatorError> {
    a.read(|connection| evidence_for_project(connection, project_id, None))
}
pub(crate) fn inspect_session_evidence(
    a: &Adapter,
    project_id: &crate::contract::control::ProjectId,
    target_session_id: SessionId,
) -> Result<Vec<crate::contract::control::SessionEvidence>, OperatorError> {
    a.read(|connection| evidence_for_project(connection, project_id, Some(target_session_id)))
}
pub(crate) fn persist_admission(
    a: &Adapter,
    r: &OperationStart,
    sid: SessionId,
    f: &str,
) -> Result<OperationAdmission, OperatorError> {
    a.transaction(|connection| {
        let request_key = r.request_id.value().to_string();
        if let Some((operation, existing_fingerprint)) =
            by_request(connection, &request_key)?
        {
            return Ok(OperationAdmission::Existing {
                operation,
                fingerprint: existing_fingerprint,
            });
        }

        if project::find(connection, &r.project_id)?.is_none() {
            return Ok(OperationAdmission::MissingProject);
        }

        let session_key = sid.value().to_string();
        if let Some(claimed_operation_id) = session_claim::active(connection, &session_key)? {
            let claimed_operation = by_id(connection, claimed_operation_id)?.ok_or_else(|| {
                OperatorError::State("active session references no operation".to_owned())
            })?;
            return Ok(OperationAdmission::ActiveSession {
                operation: claimed_operation,
            });
        }

        let operation = Operation {
            operation_id: OperationId::new(),
            request_id: r.request_id,
            project_id: r.project_id.clone(),
            intent: r.intent.clone(),
            session_id: sid,
            state: OperationState::Accepted,
            observed_session_id: None,
            observed_model: None,
            observed_claude_version: None,
            terminal_outcome: None,
        };
        let record = encode(&operation)?;
        let operation_key = operation.operation_id.value().to_string();
        let mut statement = connection
            .prepare(
                "INSERT INTO operations (request_id, operation_id, session_id, fingerprint, record_json) VALUES (?, ?, ?, ?, ?)",
            )
            .map_err(sql_error)?;
        statement
            .bind(
                &[
                    (1, request_key.as_str()),
                    (2, operation_key.as_str()),
                    (3, session_key.as_str()),
                    (4, f),
                    (5, record.as_str()),
                ][..],
            )
            .map_err(sql_error)?;
        statement.next().map_err(sql_error)?;
        session_claim::claim(connection, &session_key, &operation_key)?;
        Ok(OperationAdmission::Inserted(operation))
    })
}
pub(crate) fn transition(
    a: &Adapter,
    id: OperationId,
    next: OperationState,
    terminal: Option<TerminalOutcome>,
    session: Option<SessionId>,
    model: Option<String>,
    version: Option<String>,
) -> Result<Operation, OperatorError> {
    a.transaction(|c| {
        let mut o =
            by_id(c, id)?.ok_or_else(|| OperatorError::UnknownOperation(id.value().to_string()))?;
        if !allowed(o.state, next) {
            return Err(OperatorError::Conflict(
                "operation transition is not permitted".to_owned(),
            ));
        }
        if next.terminal() != terminal.is_some() {
            return Err(OperatorError::State(
                "terminal outcome must exactly match terminal state".to_owned(),
            ));
        }
        o.state = next;
        o.terminal_outcome = terminal;
        if session.is_some() {
            o.observed_session_id = session
        }
        if model.is_some() {
            o.observed_model = model
        }
        if version.is_some() {
            o.observed_claude_version = version
        }
        write(c, &o)?;
        if next.terminal() {
            session_claim::release(c, &o.operation_id.value().to_string())?
        }
        Ok(o)
    })
}
pub(crate) fn recover(a: &Adapter) -> Result<(), OperatorError> {
    a.transaction(|c| {
        let mut s = c
            .prepare("SELECT record_json FROM operations")
            .map_err(sql_error)?;
        let mut items = Vec::new();
        while let State::Row = s.next().map_err(sql_error)? {
            let o: Operation = decode(s.read::<String, _>(0).map_err(sql_error)?)?;
            if !o.state.terminal() {
                items.push(o)
            }
        }
        for mut o in items {
            o.state = OperationState::Indeterminate;
            o.terminal_outcome = Some(TerminalOutcome::Indeterminate(
                "daemon restarted before direct child was classified".to_owned(),
            ));
            write(c, &o)?
        }
        Ok(())
    })
}
fn by_request(
    c: &ConnectionThreadSafe,
    k: &str,
) -> Result<Option<(Operation, String)>, OperatorError> {
    let mut s = c
        .prepare("SELECT record_json, fingerprint FROM operations WHERE request_id = ?")
        .map_err(sql_error)?;
    s.bind((1, k)).map_err(sql_error)?;
    match s.next().map_err(sql_error)? {
        State::Row => Ok(Some((
            decode(s.read::<String, _>(0).map_err(sql_error)?)?,
            s.read::<String, _>(1).map_err(sql_error)?,
        ))),
        State::Done => Ok(None),
    }
}
fn by_id(c: &ConnectionThreadSafe, id: OperationId) -> Result<Option<Operation>, OperatorError> {
    let k = id.value().to_string();
    let mut s = c
        .prepare("SELECT record_json FROM operations WHERE operation_id = ?")
        .map_err(sql_error)?;
    s.bind((1, k.as_str())).map_err(sql_error)?;
    match s.next().map_err(sql_error)? {
        State::Row => Ok(Some(decode(s.read::<String, _>(0).map_err(sql_error)?)?)),
        State::Done => Ok(None),
    }
}
fn write(c: &ConnectionThreadSafe, o: &Operation) -> Result<(), OperatorError> {
    let r = encode(o)?;
    let k = o.operation_id.value().to_string();
    let mut s = c
        .prepare("UPDATE operations SET record_json = ? WHERE operation_id = ?")
        .map_err(sql_error)?;
    s.bind(&[(1, r.as_str()), (2, k.as_str())][..])
        .map_err(sql_error)?;
    s.next().map_err(sql_error)?;
    Ok(())
}
fn allowed(c: OperationState, n: OperationState) -> bool {
    match c {
        OperationState::Accepted => matches!(
            n,
            OperationState::Running
                | OperationState::Failed
                | OperationState::Cancelled
                | OperationState::Indeterminate
        ),
        OperationState::Running => matches!(
            n,
            OperationState::Succeeded
                | OperationState::Failed
                | OperationState::Cancelled
                | OperationState::Indeterminate
        ),
        OperationState::Succeeded
        | OperationState::Failed
        | OperationState::Cancelled
        | OperationState::Indeterminate => false,
    }
}

fn evidence_for_project(
    connection: &ConnectionThreadSafe,
    project_id: &crate::contract::control::ProjectId,
    target_session_id: Option<SessionId>,
) -> Result<Vec<crate::contract::control::SessionEvidence>, OperatorError> {
    let mut statement = connection
        .prepare("SELECT record_json FROM operations")
        .map_err(sql_error)?;
    let mut evidence = Vec::new();
    while let State::Row = statement.next().map_err(sql_error)? {
        let operation: Operation = decode(statement.read::<String, _>(0).map_err(sql_error)?)?;
        if operation.project_id != *project_id || operation.state != OperationState::Succeeded {
            continue;
        }
        let (Some(observed_session_id), Some(observed_model), Some(TerminalOutcome::Succeeded(_))) = (
            operation.observed_session_id,
            operation.observed_model,
            operation.terminal_outcome,
        ) else {
            continue;
        };
        if observed_session_id != operation.session_id {
            continue;
        }
        if let Some(required_session_id) = target_session_id
            && observed_session_id != required_session_id
        {
            continue;
        }
        evidence.push(crate::contract::control::SessionEvidence {
            operation_id: operation.operation_id,
            target_session_id: observed_session_id,
            observed_model,
            observed_claude_version: operation.observed_claude_version,
        });
    }
    evidence.sort_by_key(|item| item.operation_id.value());
    Ok(evidence)
}
