// Copyright 2026 Yuriy Krasilnikov
// SPDX-License-Identifier: Apache-2.0

//! Decodes one durable session-writer claim reference.

use uuid::Uuid;

use super::sqlite::sql_error;
use crate::contract::control::{OperationId, OperatorError};
use ::sqlite::{ConnectionThreadSafe, State};

pub fn operation_id(value: String) -> Result<OperationId, OperatorError> {
    value
        .parse::<Uuid>()
        .map(OperationId::new_exact)
        .map_err(|error| {
            OperatorError::State(format!(
                "active session operation id was not a UUID: {error}"
            ))
        })
}
pub(crate) fn active(
    connection: &ConnectionThreadSafe,
    session: &str,
) -> Result<Option<OperationId>, OperatorError> {
    let mut s = connection
        .prepare("SELECT operation_id FROM active_sessions WHERE session_id = ?")
        .map_err(sql_error)?;
    s.bind((1, session)).map_err(sql_error)?;
    match s.next().map_err(sql_error)? {
        State::Row => Ok(Some(operation_id(
            s.read::<String, _>(0).map_err(sql_error)?,
        )?)),
        State::Done => Ok(None),
    }
}
pub(crate) fn claim(
    connection: &ConnectionThreadSafe,
    session: &str,
    operation: &str,
) -> Result<(), OperatorError> {
    let mut s = connection
        .prepare("INSERT INTO active_sessions (session_id, operation_id) VALUES (?, ?)")
        .map_err(sql_error)?;
    s.bind(&[(1, session), (2, operation)][..])
        .map_err(sql_error)?;
    s.next().map_err(sql_error)?;
    Ok(())
}
pub(crate) fn release(
    connection: &ConnectionThreadSafe,
    operation: &str,
) -> Result<(), OperatorError> {
    let mut s = connection
        .prepare("DELETE FROM active_sessions WHERE operation_id = ?")
        .map_err(sql_error)?;
    s.bind((1, operation)).map_err(sql_error)?;
    s.next().map_err(sql_error)?;
    Ok(())
}
