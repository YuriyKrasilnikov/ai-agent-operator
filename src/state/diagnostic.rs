// Copyright 2026 Yuriy Krasilnikov
// SPDX-License-Identifier: Apache-2.0

//! SS09: durable, provider-neutral diagnostics for covered one-shot operations.

use ::sqlite::{ConnectionThreadSafe, State};

use super::{
    conversation,
    operation::by_id,
    sqlite::{Adapter, decode, encode, sql_error},
};
use crate::contract::control::{
    OperationDiagnostic, OperationDiagnosticPayload, OperationDiagnostics, OperationId,
    OperatorError,
};

pub(crate) fn create_coverage(
    connection: &ConnectionThreadSafe,
    operation_id: OperationId,
) -> Result<(), OperatorError> {
    let operation_key = operation_id.value().to_string();
    let mut statement = connection
        .prepare("INSERT INTO operation_diagnostic_coverage (operation_id) VALUES (?)")
        .map_err(sql_error)?;
    statement
        .bind((1, operation_key.as_str()))
        .map_err(sql_error)?;
    statement.next().map_err(sql_error)?;
    Ok(())
}

pub(crate) fn record(
    adapter: &Adapter,
    operation_id: OperationId,
    payload: OperationDiagnosticPayload,
) -> Result<OperationDiagnostic, OperatorError> {
    adapter.transaction(|connection| {
        payload.validate()?;
        let operation = by_id(connection, operation_id)?.ok_or_else(|| {
            OperatorError::UnknownOperation(operation_id.value().to_string())
        })?;
        if operation.state != crate::contract::control::OperationState::Running {
            return Err(OperatorError::Conflict(
                "operation diagnostics can be appended only while the operation is running"
                    .to_owned(),
            ));
        }
        require_coverage(connection, operation_id)?;
        let operation_key = operation_id.value().to_string();
        let mut sequence_statement = connection
            .prepare(
                "SELECT COALESCE(MAX(diagnostic_sequence), 0) FROM operation_diagnostics WHERE operation_id = ?",
            )
            .map_err(sql_error)?;
        sequence_statement
            .bind((1, operation_key.as_str()))
            .map_err(sql_error)?;
        let sequence = match sequence_statement.next().map_err(sql_error)? {
            State::Row => sequence_statement.read::<i64, _>(0).map_err(sql_error)?,
            State::Done => {
                return Err(OperatorError::State(
                    "diagnostic sequence query returned no aggregate row".to_owned(),
                ));
            }
        };
        let sequence = sequence.checked_add(1).ok_or_else(|| {
            OperatorError::State("diagnostic sequence exhausted durable range".to_owned())
        })?;
        let diagnostic_sequence = u64::try_from(sequence).map_err(|_| {
            OperatorError::State("diagnostic sequence is outside unsigned range".to_owned())
        })?;
        let diagnostic = OperationDiagnostic {
            operation_id,
            diagnostic_sequence,
            payload,
        };
        let record = encode(&diagnostic)?;
        let mut statement = connection
            .prepare(
                "INSERT INTO operation_diagnostics (operation_id, diagnostic_sequence, record_json) VALUES (?, ?, ?)",
            )
            .map_err(sql_error)?;
        statement
            .bind((1, operation_key.as_str()))
            .map_err(sql_error)?;
        statement.bind((2, sequence)).map_err(sql_error)?;
        statement.bind((3, record.as_str())).map_err(sql_error)?;
        statement.next().map_err(sql_error)?;
        Ok(diagnostic)
    })
}

pub(crate) fn snapshot(
    adapter: &Adapter,
    operation_id: OperationId,
    after_diagnostic_sequence: u64,
) -> Result<OperationDiagnostics, OperatorError> {
    let cursor = i64::try_from(after_diagnostic_sequence).map_err(|_| {
        OperatorError::InvalidRequest(
            "diagnostic cursor exceeds the supported durable range".to_owned(),
        )
    })?;
    adapter.read(|connection| {
        let operation = by_id(connection, operation_id)?.ok_or_else(|| {
            OperatorError::UnknownOperation(operation_id.value().to_string())
        })?;
        if conversation::exists(connection, operation_id)? {
            return Err(OperatorError::Conflict(
                "operation diagnostics are unavailable for live conversations".to_owned(),
            ));
        }
        require_coverage(connection, operation_id)?;
        let operation_key = operation_id.value().to_string();
        let mut statement = connection
            .prepare(
                "SELECT record_json FROM operation_diagnostics WHERE operation_id = ? AND diagnostic_sequence > ? ORDER BY diagnostic_sequence ASC",
            )
            .map_err(sql_error)?;
        statement
            .bind((1, operation_key.as_str()))
            .map_err(sql_error)?;
        statement.bind((2, cursor)).map_err(sql_error)?;
        let mut diagnostics = Vec::new();
        while let State::Row = statement.next().map_err(sql_error)? {
            diagnostics.push(decode(statement.read::<String, _>(0).map_err(sql_error)?)?);
        }
        Ok(OperationDiagnostics {
            operation,
            diagnostics,
        })
    })
}

fn require_coverage(
    connection: &ConnectionThreadSafe,
    operation_id: OperationId,
) -> Result<(), OperatorError> {
    let operation_key = operation_id.value().to_string();
    let mut statement = connection
        .prepare("SELECT operation_id FROM operation_diagnostic_coverage WHERE operation_id = ?")
        .map_err(sql_error)?;
    statement
        .bind((1, operation_key.as_str()))
        .map_err(sql_error)?;
    match statement.next().map_err(sql_error)? {
        State::Row => Ok(()),
        State::Done => Err(OperatorError::DiagnosticsUnavailable(operation_key)),
    }
}
