// Copyright 2026 Yuriy Krasilnikov
// SPDX-License-Identifier: Apache-2.0

//! Owns durable conversation-turn admission, ordering, lifecycle transitions, and event eligibility.

use crate::contract::control::{
    ConversationEventPayload, ConversationId, ConversationState, ConversationTurn, OperatorError,
    TurnId, TurnState,
};
use ::sqlite::{ConnectionThreadSafe, State};

use super::conversation_timeline;
use super::sqlite::{decode, encode, sql_error};

pub(crate) fn first(
    connection: &ConnectionThreadSafe,
    conversation_id: ConversationId,
) -> Result<Option<ConversationTurn>, OperatorError> {
    let conversation_key = conversation_id.operation_id().value().to_string();
    let mut statement = connection
        .prepare(
            "SELECT record_json FROM conversation_turns WHERE conversation_id = ? ORDER BY position LIMIT 1",
        )
        .map_err(sql_error)?;
    statement
        .bind((1, conversation_key.as_str()))
        .map_err(sql_error)?;
    match statement.next().map_err(sql_error)? {
        State::Row => Ok(Some(decode(
            statement.read::<String, _>(0).map_err(sql_error)?,
        )?)),
        State::Done => Ok(None),
    }
}

pub(crate) fn list(
    connection: &ConnectionThreadSafe,
    conversation_id: ConversationId,
) -> Result<Vec<ConversationTurn>, OperatorError> {
    let conversation_key = conversation_id.operation_id().value().to_string();
    let mut statement = connection
        .prepare(
            "SELECT record_json FROM conversation_turns WHERE conversation_id = ? ORDER BY position",
        )
        .map_err(sql_error)?;
    statement
        .bind((1, conversation_key.as_str()))
        .map_err(sql_error)?;
    let mut turns = Vec::new();
    while let State::Row = statement.next().map_err(sql_error)? {
        turns.push(decode(statement.read::<String, _>(0).map_err(sql_error)?)?);
    }
    Ok(turns)
}

pub(crate) fn find(
    connection: &ConnectionThreadSafe,
    conversation_id: ConversationId,
    turn_id: TurnId,
) -> Result<Option<ConversationTurn>, OperatorError> {
    Ok(find_with_fingerprint(connection, conversation_id, turn_id)?.map(|(turn, _)| turn))
}

pub(crate) fn find_with_fingerprint(
    connection: &ConnectionThreadSafe,
    conversation_id: ConversationId,
    turn_id: TurnId,
) -> Result<Option<(ConversationTurn, String)>, OperatorError> {
    let conversation_key = conversation_id.operation_id().value().to_string();
    let turn_key = turn_id.value().to_string();
    let mut statement = connection
        .prepare(
            "SELECT record_json, fingerprint FROM conversation_turns WHERE conversation_id = ? AND turn_id = ?",
        )
        .map_err(sql_error)?;
    statement
        .bind(&[(1, conversation_key.as_str()), (2, turn_key.as_str())][..])
        .map_err(sql_error)?;
    match statement.next().map_err(sql_error)? {
        State::Row => Ok(Some((
            decode(statement.read::<String, _>(0).map_err(sql_error)?)?,
            statement.read::<String, _>(1).map_err(sql_error)?,
        ))),
        State::Done => Ok(None),
    }
}

pub(crate) fn insert(
    connection: &ConnectionThreadSafe,
    turn: &ConversationTurn,
    fingerprint: &str,
) -> Result<(), OperatorError> {
    let record = encode(turn)?;
    let conversation_key = turn.conversation_id.operation_id().value().to_string();
    let turn_key = turn.turn_id.value().to_string();
    let position = i64::try_from(turn.position).map_err(|error| {
        OperatorError::State(format!("turn position does not fit SQLite: {error}"))
    })?;
    let mut statement = connection
        .prepare(
            "INSERT INTO conversation_turns (conversation_id, turn_id, position, fingerprint, record_json) VALUES (?, ?, ?, ?, ?)",
        )
        .map_err(sql_error)?;
    statement
        .bind((1, conversation_key.as_str()))
        .map_err(sql_error)?;
    statement.bind((2, turn_key.as_str())).map_err(sql_error)?;
    statement.bind((3, position)).map_err(sql_error)?;
    statement.bind((4, fingerprint)).map_err(sql_error)?;
    statement.bind((5, record.as_str())).map_err(sql_error)?;
    statement.next().map_err(sql_error)?;
    Ok(())
}

pub(crate) fn record_transition(
    connection: &ConnectionThreadSafe,
    conversation_id: ConversationId,
    turn_id: TurnId,
    state: Option<TurnState>,
    result: Option<String>,
    payload: &ConversationEventPayload,
) -> Result<ConversationTurn, OperatorError> {
    if event_turn_id(payload)? != turn_id {
        return Err(OperatorError::State(
            "turn observation event refers to another turn".to_owned(),
        ));
    }
    if !event_matches_transition(state, result.as_deref(), payload) {
        return Err(OperatorError::State(
            "turn observation event does not match the durable transition".to_owned(),
        ));
    }
    match state {
        Some(next) => transition(connection, conversation_id, turn_id, next, result),
        None => {
            if result.is_some() {
                return Err(OperatorError::State(
                    "turn event without state transition cannot record a result".to_owned(),
                ));
            }
            let turn = find(connection, conversation_id, turn_id)?
                .ok_or_else(|| OperatorError::State("conversation turn is unknown".to_owned()))?;
            if !nontransition_event_allowed(&turn, payload) {
                return Err(OperatorError::Conflict(
                    "turn observation event is not admissible in the durable turn state".to_owned(),
                ));
            }
            Ok(turn)
        }
    }
}

fn nontransition_event_allowed(
    turn: &ConversationTurn,
    payload: &ConversationEventPayload,
) -> bool {
    match payload {
        ConversationEventPayload::TurnAcknowledged { .. } => {
            matches!(turn.state, TurnState::Queued | TurnState::Started)
        }
        ConversationEventPayload::AssistantTextDelta { .. } => turn.state == TurnState::Started,
        ConversationEventPayload::Initialized { .. }
        | ConversationEventPayload::TurnQueued { .. }
        | ConversationEventPayload::TurnStarted { .. }
        | ConversationEventPayload::TurnCompleted { .. }
        | ConversationEventPayload::TurnCancelled { .. }
        | ConversationEventPayload::TurnDiscarded { .. }
        | ConversationEventPayload::TurnFailed { .. }
        | ConversationEventPayload::TurnIndeterminate { .. }
        | ConversationEventPayload::ConversationTerminal { .. } => false,
    }
}

fn transition(
    connection: &ConnectionThreadSafe,
    conversation_id: ConversationId,
    turn_id: TurnId,
    state: TurnState,
    result: Option<String>,
) -> Result<ConversationTurn, OperatorError> {
    let mut turn = find(connection, conversation_id, turn_id)?
        .ok_or_else(|| OperatorError::State("conversation turn is unknown".to_owned()))?;
    if !turn_transition_allowed(turn.state, state) {
        return Err(OperatorError::Conflict(
            "conversation turn transition is not permitted".to_owned(),
        ));
    }
    if (state == TurnState::Completed) != result.is_some() {
        return Err(OperatorError::State(
            "completed turn must exactly match a result".to_owned(),
        ));
    }
    turn.state = state;
    turn.result = result;
    write(connection, &turn)?;
    Ok(turn)
}

fn write(connection: &ConnectionThreadSafe, turn: &ConversationTurn) -> Result<(), OperatorError> {
    let record = encode(turn)?;
    let conversation_key = turn.conversation_id.operation_id().value().to_string();
    let turn_key = turn.turn_id.value().to_string();
    let mut statement = connection
        .prepare(
            "UPDATE conversation_turns SET record_json = ? WHERE conversation_id = ? AND turn_id = ?",
        )
        .map_err(sql_error)?;
    statement
        .bind(
            &[
                (1, record.as_str()),
                (2, conversation_key.as_str()),
                (3, turn_key.as_str()),
            ][..],
        )
        .map_err(sql_error)?;
    statement.next().map_err(sql_error)?;
    Ok(())
}

fn event_turn_id(payload: &ConversationEventPayload) -> Result<TurnId, OperatorError> {
    match payload {
        ConversationEventPayload::Initialized { .. }
        | ConversationEventPayload::ConversationTerminal { .. } => Err(OperatorError::State(
            "conversation-level event cannot observe a turn".to_owned(),
        )),
        ConversationEventPayload::TurnQueued { turn_id }
        | ConversationEventPayload::TurnStarted { turn_id }
        | ConversationEventPayload::TurnAcknowledged { turn_id }
        | ConversationEventPayload::AssistantTextDelta { turn_id, .. }
        | ConversationEventPayload::TurnCompleted { turn_id, .. }
        | ConversationEventPayload::TurnCancelled { turn_id, .. }
        | ConversationEventPayload::TurnDiscarded { turn_id, .. }
        | ConversationEventPayload::TurnFailed { turn_id, .. }
        | ConversationEventPayload::TurnIndeterminate { turn_id, .. } => Ok(*turn_id),
    }
}

fn event_matches_transition(
    state: Option<TurnState>,
    result: Option<&str>,
    payload: &ConversationEventPayload,
) -> bool {
    match (state, result, payload) {
        (None, None, ConversationEventPayload::TurnAcknowledged { .. })
        | (None, None, ConversationEventPayload::AssistantTextDelta { .. }) => true,
        (Some(TurnState::Started), None, ConversationEventPayload::TurnStarted { .. }) => true,
        (
            Some(TurnState::Completed),
            Some(result),
            ConversationEventPayload::TurnCompleted {
                result: event_result,
                ..
            },
        ) => result == event_result,
        (Some(TurnState::Cancelled), None, ConversationEventPayload::TurnCancelled { .. })
        | (Some(TurnState::Discarded), None, ConversationEventPayload::TurnDiscarded { .. })
        | (Some(TurnState::Failed), None, ConversationEventPayload::TurnFailed { .. })
        | (
            Some(TurnState::Indeterminate),
            None,
            ConversationEventPayload::TurnIndeterminate { .. },
        ) => true,
        (None, Some(_), _)
        | (None, None, _)
        | (Some(TurnState::Queued), _, _)
        | (Some(TurnState::Started), _, _)
        | (Some(TurnState::Completed), _, _)
        | (Some(TurnState::Cancelled), _, _)
        | (Some(TurnState::Discarded), _, _)
        | (Some(TurnState::Failed), _, _)
        | (Some(TurnState::Indeterminate), _, _) => false,
    }
}

fn turn_transition_allowed(current: TurnState, next: TurnState) -> bool {
    match current {
        TurnState::Queued => matches!(
            next,
            TurnState::Started
                | TurnState::Cancelled
                | TurnState::Discarded
                | TurnState::Failed
                | TurnState::Indeterminate
        ),
        TurnState::Started => matches!(
            next,
            TurnState::Completed
                | TurnState::Cancelled
                | TurnState::Failed
                | TurnState::Indeterminate
        ),
        TurnState::Completed
        | TurnState::Cancelled
        | TurnState::Discarded
        | TurnState::Failed
        | TurnState::Indeterminate => false,
    }
}

pub(crate) fn resolve_unfinished(
    connection: &ConnectionThreadSafe,
    conversation_id: ConversationId,
    conversation_state: ConversationState,
) -> Result<(), OperatorError> {
    let turns = list(connection, conversation_id)?;
    let unresolved: Vec<ConversationTurn> = turns
        .into_iter()
        .filter(|turn| !turn.state.terminal())
        .collect();
    if unresolved.is_empty() {
        return Ok(());
    }
    let default_state = unresolved_state(conversation_state)?;
    for mut turn in unresolved {
        let next = match (conversation_state, turn.state) {
            (ConversationState::Cancelled, TurnState::Queued) => TurnState::Discarded,
            (ConversationState::Cancelled, TurnState::Started) => TurnState::Cancelled,
            (_, TurnState::Queued | TurnState::Started) => default_state,
            (
                _,
                TurnState::Completed
                | TurnState::Cancelled
                | TurnState::Discarded
                | TurnState::Failed
                | TurnState::Indeterminate,
            ) => {
                return Err(OperatorError::State(
                    "terminal resolution selected an already terminal turn".to_owned(),
                ));
            }
        };
        turn.state = next;
        write(connection, &turn)?;
        conversation_timeline::append(
            connection,
            conversation_id,
            unresolved_event(turn.turn_id, next, conversation_state)?,
        )?;
    }
    Ok(())
}

fn unresolved_event(
    turn_id: TurnId,
    state: TurnState,
    conversation_state: ConversationState,
) -> Result<ConversationEventPayload, OperatorError> {
    let message = match conversation_state {
        ConversationState::Cancelled => "conversation cancellation was observed".to_owned(),
        ConversationState::Failed => "conversation failed before this turn completed".to_owned(),
        ConversationState::Indeterminate => {
            "conversation became indeterminate before this turn completed".to_owned()
        }
        ConversationState::Open | ConversationState::Closing | ConversationState::Succeeded => {
            return Err(OperatorError::State(
                "non-failing terminal state cannot resolve unfinished turns".to_owned(),
            ));
        }
    };
    match state {
        TurnState::Cancelled => Ok(ConversationEventPayload::TurnCancelled { turn_id, message }),
        TurnState::Discarded => Ok(ConversationEventPayload::TurnDiscarded { turn_id, message }),
        TurnState::Failed => Ok(ConversationEventPayload::TurnFailed { turn_id, message }),
        TurnState::Indeterminate => {
            Ok(ConversationEventPayload::TurnIndeterminate { turn_id, message })
        }
        TurnState::Queued | TurnState::Started | TurnState::Completed => Err(OperatorError::State(
            "terminal resolution selected a nonterminal turn event".to_owned(),
        )),
    }
}

fn unresolved_state(conversation_state: ConversationState) -> Result<TurnState, OperatorError> {
    match conversation_state {
        ConversationState::Open | ConversationState::Closing => Err(OperatorError::State(
            "nonterminal conversation cannot resolve unfinished turns".to_owned(),
        )),
        ConversationState::Succeeded => Err(OperatorError::State(
            "succeeded conversation has unfinished turns".to_owned(),
        )),
        ConversationState::Cancelled => Ok(TurnState::Cancelled),
        ConversationState::Failed => Ok(TurnState::Failed),
        ConversationState::Indeterminate => Ok(TurnState::Indeterminate),
    }
}

pub(crate) fn highest_position(
    connection: &ConnectionThreadSafe,
    conversation_id: ConversationId,
) -> Result<u64, OperatorError> {
    let conversation_key = conversation_id.operation_id().value().to_string();
    let mut statement = connection
        .prepare("SELECT MAX(position) FROM conversation_turns WHERE conversation_id = ?")
        .map_err(sql_error)?;
    statement
        .bind((1, conversation_key.as_str()))
        .map_err(sql_error)?;
    match statement.next().map_err(sql_error)? {
        State::Row => {
            let maximum = statement.read::<Option<i64>, _>(0).map_err(sql_error)?;
            let stored_position = maximum.ok_or_else(|| {
                OperatorError::State("conversation has no durably admitted first turn".to_owned())
            })?;
            let position = u64::try_from(stored_position).map_err(|error| {
                OperatorError::State(format!("stored turn position is invalid: {error}"))
            })?;
            if position == 0 {
                return Err(OperatorError::State(
                    "stored turn position must start at one".to_owned(),
                ));
            }
            Ok(position)
        }
        State::Done => Err(OperatorError::State(
            "SQLite did not return a highest turn position".to_owned(),
        )),
    }
}

pub(crate) fn next_position(
    connection: &ConnectionThreadSafe,
    conversation_id: ConversationId,
) -> Result<u64, OperatorError> {
    let conversation_key = conversation_id.operation_id().value().to_string();
    let mut statement = connection
        .prepare(
            "SELECT COALESCE(MAX(position), 0) FROM conversation_turns WHERE conversation_id = ?",
        )
        .map_err(sql_error)?;
    statement
        .bind((1, conversation_key.as_str()))
        .map_err(sql_error)?;
    match statement.next().map_err(sql_error)? {
        State::Row => {
            let maximum = statement.read::<i64, _>(0).map_err(sql_error)?;
            let position = u64::try_from(maximum).map_err(|error| {
                OperatorError::State(format!("stored turn position is invalid: {error}"))
            })?;
            position.checked_add(1).ok_or_else(|| {
                OperatorError::State("conversation turn position is exhausted".to_owned())
            })
        }
        State::Done => Err(OperatorError::State(
            "SQLite did not return a turn position".to_owned(),
        )),
    }
}
