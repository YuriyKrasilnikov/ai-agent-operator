// Copyright 2026 Yuriy Krasilnikov
// SPDX-License-Identifier: Apache-2.0

//! Persists the ordered event timeline of one durable conversation.

use super::sqlite::{decode, encode, sql_error};
use crate::contract::control::{
    ConversationEvent, ConversationEventPayload, ConversationId, OperatorError,
};
use ::sqlite::{ConnectionThreadSafe, State};

const FIRST_EVENT_SEQUENCE: u64 = 1;

pub(crate) fn append(
    connection: &ConnectionThreadSafe,
    conversation_id: ConversationId,
    payload: ConversationEventPayload,
) -> Result<ConversationEvent, OperatorError> {
    let event = ConversationEvent {
        conversation_id,
        sequence: next_sequence(connection, conversation_id)?,
        payload,
    };
    insert(connection, &event)?;
    Ok(event)
}

pub(crate) fn list_after(
    connection: &ConnectionThreadSafe,
    conversation_id: ConversationId,
    after_sequence: u64,
) -> Result<Vec<ConversationEvent>, OperatorError> {
    let after = i64::try_from(after_sequence).map_err(|error| {
        OperatorError::State(format!(
            "conversation event sequence does not fit SQLite: {error}"
        ))
    })?;
    let conversation_key = conversation_id.operation_id().value().to_string();
    let mut statement = connection
        .prepare(
            "SELECT record_json FROM conversation_events WHERE conversation_id = ? AND sequence > ? ORDER BY sequence",
        )
        .map_err(sql_error)?;
    statement
        .bind((1, conversation_key.as_str()))
        .map_err(sql_error)?;
    statement.bind((2, after)).map_err(sql_error)?;
    let mut events = Vec::new();
    while let State::Row = statement.next().map_err(sql_error)? {
        events.push(decode(statement.read::<String, _>(0).map_err(sql_error)?)?);
    }
    Ok(events)
}

fn insert(
    connection: &ConnectionThreadSafe,
    event: &ConversationEvent,
) -> Result<(), OperatorError> {
    let record = encode(event)?;
    let conversation_key = event.conversation_id.operation_id().value().to_string();
    let sequence = i64::try_from(event.sequence).map_err(|error| {
        OperatorError::State(format!("event sequence does not fit SQLite: {error}"))
    })?;
    let mut statement = connection
        .prepare(
            "INSERT INTO conversation_events (conversation_id, sequence, record_json) VALUES (?, ?, ?)",
        )
        .map_err(sql_error)?;
    statement
        .bind((1, conversation_key.as_str()))
        .map_err(sql_error)?;
    statement.bind((2, sequence)).map_err(sql_error)?;
    statement.bind((3, record.as_str())).map_err(sql_error)?;
    statement.next().map_err(sql_error)?;
    Ok(())
}

fn next_sequence(
    connection: &ConnectionThreadSafe,
    conversation_id: ConversationId,
) -> Result<u64, OperatorError> {
    let conversation_key = conversation_id.operation_id().value().to_string();
    let mut statement = connection
        .prepare(
            "SELECT COALESCE(MAX(sequence), 0) FROM conversation_events WHERE conversation_id = ?",
        )
        .map_err(sql_error)?;
    statement
        .bind((1, conversation_key.as_str()))
        .map_err(sql_error)?;
    match statement.next().map_err(sql_error)? {
        State::Row => {
            let maximum = statement.read::<i64, _>(0).map_err(sql_error)?;
            let sequence = u64::try_from(maximum).map_err(|error| {
                OperatorError::State(format!("stored event sequence is invalid: {error}"))
            })?;
            if sequence == 0 {
                Ok(FIRST_EVENT_SEQUENCE)
            } else {
                sequence.checked_add(1).ok_or_else(|| {
                    OperatorError::State("conversation event sequence is exhausted".to_owned())
                })
            }
        }
        State::Done => Err(OperatorError::State(
            "SQLite did not return an event sequence".to_owned(),
        )),
    }
}
