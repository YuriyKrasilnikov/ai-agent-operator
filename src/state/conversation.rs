// Copyright 2026 Yuriy Krasilnikov
// SPDX-License-Identifier: Apache-2.0

//! Persists one live conversation and its durable turn admission facts.

use super::{
    conversation_timeline, conversation_turn, operation, project, session_claim,
    sqlite::{Adapter, decode, encode, sql_error},
};
use crate::contract::control::{
    Conversation, ConversationCloseAdmission, ConversationEvent, ConversationEventPayload,
    ConversationId, ConversationSend, ConversationStart, ConversationStartAdmission,
    ConversationState, ConversationStopMode, ConversationTurn, ConversationTurnAdmission,
    Operation, OperationId, OperationState, OperatorError, SessionClaimDisposition, SessionId,
    TerminalOutcome, TurnId, TurnState,
};
use ::sqlite::{ConnectionThreadSafe, State};

const FIRST_TURN_POSITION: u64 = 1;

pub(crate) fn persist_start(
    adapter: &Adapter,
    request: &ConversationStart,
    session_id: SessionId,
    fingerprint: &str,
) -> Result<ConversationStartAdmission, OperatorError> {
    adapter.transaction(|connection| {
        let request_key = request.request_id.value().to_string();
        if let Some((operation, existing_fingerprint)) =
            operation::by_request(connection, &request_key)?
        {
            let conversation_id = ConversationId::new(operation.operation_id);
            return match find(connection, conversation_id)? {
                Some(conversation) => {
                    let first_turn = conversation_turn::first(connection, conversation_id)?
                        .ok_or_else(|| {
                            OperatorError::State(
                                "conversation has no durable first turn".to_owned(),
                            )
                        })?;
                    Ok(ConversationStartAdmission::Existing {
                        operation,
                        conversation,
                        first_turn,
                        fingerprint: existing_fingerprint,
                    })
                }
                None => Ok(ConversationStartAdmission::ExistingOperation {
                    operation,
                    fingerprint: existing_fingerprint,
                }),
            };
        }

        if project::find(connection, &request.project_id)?.is_none() {
            return Ok(ConversationStartAdmission::MissingProject);
        }

        let session_key = session_id.value().to_string();
        if let Some(claimed_operation_id) = session_claim::active(connection, &session_key)? {
            let claimed_operation = operation::by_id(connection, claimed_operation_id)?
                .ok_or_else(|| {
                    OperatorError::State("active session references no operation".to_owned())
                })?;
            return Ok(ConversationStartAdmission::ActiveSession {
                operation: claimed_operation,
            });
        }

        let operation = new_operation(request, session_id);
        let conversation = Conversation {
            conversation_id: ConversationId::new(operation.operation_id),
            project_id: request.project_id.clone(),
            intent: request.intent.clone(),
            session_id,
            state: ConversationState::Open,
            close_mode: None,
            terminal_outcome: None,
        };
        let first_turn = ConversationTurn {
            conversation_id: conversation.conversation_id,
            turn_id: request.turn_id,
            position: FIRST_TURN_POSITION,
            prompt: request.prompt.clone(),
            state: TurnState::Queued,
            result: None,
        };
        insert_operation(connection, &operation, fingerprint)?;
        session_claim::claim(
            connection,
            &session_key,
            &operation.operation_id.value().to_string(),
        )?;
        insert_conversation(connection, &conversation)?;
        conversation_turn::insert(connection, &first_turn, fingerprint)?;
        conversation_timeline::append(
            connection,
            conversation.conversation_id,
            ConversationEventPayload::TurnQueued {
                turn_id: first_turn.turn_id,
            },
        )?;
        Ok(ConversationStartAdmission::Inserted {
            operation,
            conversation,
            first_turn,
        })
    })
}

pub(crate) fn get(
    adapter: &Adapter,
    conversation_id: ConversationId,
) -> Result<Conversation, OperatorError> {
    adapter.read(|connection| {
        find(connection, conversation_id)?.ok_or_else(|| unknown(conversation_id))
    })
}

pub(crate) fn snapshot(
    adapter: &Adapter,
    conversation_id: ConversationId,
    after_sequence: u64,
) -> Result<crate::contract::control::ConversationSnapshot, OperatorError> {
    adapter.read(|connection| {
        let conversation =
            find(connection, conversation_id)?.ok_or_else(|| unknown(conversation_id))?;
        let turns = conversation_turn::list(connection, conversation_id)?;
        let events =
            conversation_timeline::list_after(connection, conversation_id, after_sequence)?;
        Ok(crate::contract::control::ConversationSnapshot {
            conversation,
            turns,
            events,
        })
    })
}

pub(crate) fn persist_turn(
    adapter: &Adapter,
    request: &ConversationSend,
    fingerprint: &str,
) -> Result<ConversationTurnAdmission, OperatorError> {
    adapter.transaction(|connection| {
        let conversation = match find(connection, request.conversation_id)? {
            Some(conversation) => conversation,
            None => return Ok(ConversationTurnAdmission::MissingConversation),
        };
        if let Some((turn, existing_fingerprint)) = conversation_turn::find_with_fingerprint(
            connection,
            request.conversation_id,
            request.turn_id,
        )? {
            return Ok(ConversationTurnAdmission::Existing {
                turn,
                fingerprint: existing_fingerprint,
            });
        }
        if conversation.state != ConversationState::Open {
            return Ok(ConversationTurnAdmission::Closed { conversation });
        }
        let turn = ConversationTurn {
            conversation_id: request.conversation_id,
            turn_id: request.turn_id,
            position: conversation_turn::next_position(connection, request.conversation_id)?,
            prompt: request.prompt.clone(),
            state: TurnState::Queued,
            result: None,
        };
        conversation_turn::insert(connection, &turn, fingerprint)?;
        conversation_timeline::append(
            connection,
            request.conversation_id,
            ConversationEventPayload::TurnQueued {
                turn_id: turn.turn_id,
            },
        )?;
        Ok(ConversationTurnAdmission::Inserted(turn))
    })
}

pub(crate) fn record_turn_observation(
    adapter: &Adapter,
    conversation_id: ConversationId,
    turn_id: TurnId,
    state: Option<TurnState>,
    result: Option<String>,
    payload: ConversationEventPayload,
) -> Result<crate::contract::control::ConversationTurnObservation, OperatorError> {
    adapter.transaction(|connection| {
        let conversation =
            find(connection, conversation_id)?.ok_or_else(|| unknown(conversation_id))?;
        if conversation.state.terminal() {
            return Err(OperatorError::Conflict(
                "terminal conversation cannot append a turn observation".to_owned(),
            ));
        }
        let turn = conversation_turn::record_transition(
            connection,
            conversation_id,
            turn_id,
            state,
            result,
            &payload,
        )?;
        let event = conversation_timeline::append(connection, conversation_id, payload)?;
        Ok(crate::contract::control::ConversationTurnObservation { turn, event })
    })
}

pub(crate) fn record_initialization(
    adapter: &Adapter,
    conversation_id: ConversationId,
    session_id: SessionId,
    model: String,
    claude_version: Option<String>,
) -> Result<ConversationEvent, OperatorError> {
    adapter.transaction(|connection| {
        let conversation =
            find(connection, conversation_id)?.ok_or_else(|| unknown(conversation_id))?;
        if conversation.state.terminal() {
            return Err(OperatorError::Conflict(
                "conversation initialization is no longer admissible".to_owned(),
            ));
        }
        if session_id != conversation.session_id {
            return Err(OperatorError::Conflict(
                "provider initialization session differs from the intended session".to_owned(),
            ));
        }
        let mut operation = operation::by_id(connection, conversation_id.operation_id())?
            .ok_or_else(|| {
                OperatorError::State("conversation references no operation".to_owned())
            })?;
        if operation.session_id != session_id {
            return Err(OperatorError::State(
                "conversation and operation session identities differ".to_owned(),
            ));
        }
        let events = conversation_timeline::list_after(connection, conversation_id, 0)?;
        let mut initialized_before = false;
        for event in events {
            match event.payload {
                ConversationEventPayload::Initialized {
                    session_id: observed_session,
                    model: observed_model,
                    claude_version: observed_version,
                } => {
                    initialized_before = true;
                    if observed_session != session_id
                        || observed_model != model
                        || observed_version != claude_version
                    {
                        return Err(OperatorError::Conflict(
                            "provider initialization contradicts prior conversation identity"
                                .to_owned(),
                        ));
                    }
                }
                ConversationEventPayload::TurnQueued { .. }
                | ConversationEventPayload::TurnStarted { .. }
                | ConversationEventPayload::TurnAcknowledged { .. }
                | ConversationEventPayload::AssistantTextDelta { .. }
                | ConversationEventPayload::TurnCompleted { .. }
                | ConversationEventPayload::TurnCancelled { .. }
                | ConversationEventPayload::TurnDiscarded { .. }
                | ConversationEventPayload::TurnFailed { .. }
                | ConversationEventPayload::TurnIndeterminate { .. }
                | ConversationEventPayload::ConversationTerminal { .. } => {}
            }
        }
        if initialized_before {
            if operation.observed_session_id != Some(session_id)
                || operation.observed_model.as_deref() != Some(model.as_str())
                || operation.observed_claude_version != claude_version
            {
                return Err(OperatorError::State(
                    "provider initialization event and operation identity disagree".to_owned(),
                ));
            }
        } else {
            if let Some(observed_session) = operation.observed_session_id
                && observed_session != session_id
            {
                return Err(OperatorError::Conflict(
                    "provider initialization session contradicts operation identity".to_owned(),
                ));
            }
            if let Some(observed_model) = operation.observed_model.as_deref()
                && observed_model != model
            {
                return Err(OperatorError::Conflict(
                    "provider initialization model contradicts operation identity".to_owned(),
                ));
            }
            if let Some(observed_version) = operation.observed_claude_version.as_ref()
                && Some(observed_version) != claude_version.as_ref()
            {
                return Err(OperatorError::Conflict(
                    "provider initialization version contradicts operation identity".to_owned(),
                ));
            }
            operation.observed_session_id = Some(session_id);
            operation.observed_model = Some(model.clone());
            operation.observed_claude_version = claude_version.clone();
            operation::write(connection, &operation)?;
        }
        conversation_timeline::append(
            connection,
            conversation_id,
            ConversationEventPayload::Initialized {
                session_id,
                model,
                claude_version,
            },
        )
    })
}

pub(crate) fn close(
    adapter: &Adapter,
    conversation_id: ConversationId,
    requested_mode: ConversationStopMode,
) -> Result<ConversationCloseAdmission, OperatorError> {
    adapter.transaction(|connection| {
        let mut conversation =
            find(connection, conversation_id)?.ok_or_else(|| unknown(conversation_id))?;
        match conversation.state {
            ConversationState::Open => {
                let through_position =
                    conversation_turn::highest_position(connection, conversation_id)?;
                conversation.state = ConversationState::Closing;
                conversation.close_mode = Some(requested_mode);
                write_conversation(connection, &conversation)?;
                Ok(ConversationCloseAdmission::ClosedNow {
                    conversation,
                    through_position,
                })
            }
            ConversationState::Closing => match conversation.close_mode {
                Some(ConversationStopMode::Graceful)
                    if requested_mode == ConversationStopMode::Cancel =>
                {
                    conversation.close_mode = Some(ConversationStopMode::Cancel);
                    write_conversation(connection, &conversation)?;
                    Ok(ConversationCloseAdmission::EscalatedToCancel(conversation))
                }
                Some(ConversationStopMode::Graceful) | Some(ConversationStopMode::Cancel) => {
                    Ok(ConversationCloseAdmission::AlreadyClosing(conversation))
                }
                None => Err(OperatorError::State(
                    "closing conversation has no durable close mode".to_owned(),
                )),
            },
            ConversationState::Succeeded
            | ConversationState::Cancelled
            | ConversationState::Failed
            | ConversationState::Indeterminate => {
                Ok(ConversationCloseAdmission::Terminal(conversation))
            }
        }
    })
}

pub(crate) fn terminalize(
    adapter: &Adapter,
    conversation_id: ConversationId,
    conversation_state: ConversationState,
    operation_state: OperationState,
    terminal: TerminalOutcome,
    claim_disposition: SessionClaimDisposition,
) -> Result<Conversation, OperatorError> {
    adapter.transaction(|connection| {
        if !conversation_state.terminal() || !operation_state.terminal() {
            return Err(OperatorError::State(
                "conversation terminal transition requires terminal states".to_owned(),
            ));
        }
        if claim_disposition == SessionClaimDisposition::RetainUnclassifiedWriter
            && conversation_state != ConversationState::Indeterminate
        {
            return Err(OperatorError::State(
                "only an indeterminate conversation may retain an unclassified writer claim"
                    .to_owned(),
            ));
        }
        if !terminal_states_agree(conversation_state, operation_state, &terminal) {
            return Err(OperatorError::State(
                "conversation and operation terminal states disagree".to_owned(),
            ));
        }
        let mut conversation =
            find(connection, conversation_id)?.ok_or_else(|| unknown(conversation_id))?;
        if conversation.state.terminal() {
            if conversation.state == conversation_state
                && conversation.terminal_outcome.as_ref() == Some(&terminal)
            {
                return Ok(conversation);
            }
            return Err(OperatorError::Conflict(
                "conversation already has a different terminal outcome".to_owned(),
            ));
        }
        let mut operation = operation::by_id(connection, conversation_id.operation_id())?
            .ok_or_else(|| {
                OperatorError::State("conversation references no operation".to_owned())
            })?;
        if operation.state.terminal() {
            return Err(OperatorError::Conflict(
                "operation already has a terminal outcome".to_owned(),
            ));
        }
        conversation_turn::resolve_unfinished(connection, conversation_id, conversation_state)?;
        conversation.state = conversation_state;
        conversation.terminal_outcome = Some(terminal.clone());
        operation.state = operation_state;
        operation.terminal_outcome = Some(terminal);
        write_conversation(connection, &conversation)?;
        operation::write(connection, &operation)?;
        conversation_timeline::append(
            connection,
            conversation_id,
            ConversationEventPayload::ConversationTerminal {
                outcome: conversation.terminal_outcome.clone().ok_or_else(|| {
                    OperatorError::State("terminal outcome was absent".to_owned())
                })?,
            },
        )?;
        match claim_disposition {
            SessionClaimDisposition::ReleaseProvenWriter => {
                session_claim::release(connection, &operation.operation_id.value().to_string())?;
            }
            SessionClaimDisposition::RetainUnclassifiedWriter => {}
        }
        Ok(conversation)
    })
}

pub(crate) fn recover(adapter: &Adapter) -> Result<(), OperatorError> {
    adapter.transaction(|connection| {
        let mut statement = connection
            .prepare("SELECT record_json FROM conversations")
            .map_err(sql_error)?;
        let mut conversations = Vec::new();
        while let State::Row = statement.next().map_err(sql_error)? {
            let conversation: Conversation =
                decode(statement.read::<String, _>(0).map_err(sql_error)?)?;
            if !conversation.state.terminal() {
                conversations.push(conversation);
            }
        }
        for mut conversation in conversations {
            let terminal = TerminalOutcome::Indeterminate(
                "daemon restarted before persistent conversation was classified".to_owned(),
            );
            let mut operation =
                operation::by_id(connection, conversation.conversation_id.operation_id())?
                    .ok_or_else(|| {
                        OperatorError::State("conversation references no operation".to_owned())
                    })?;
            if operation.state.terminal() {
                return Err(OperatorError::State(
                    "live conversation references a terminal operation during recovery".to_owned(),
                ));
            }
            conversation_turn::resolve_unfinished(
                connection,
                conversation.conversation_id,
                ConversationState::Indeterminate,
            )?;
            conversation.state = ConversationState::Indeterminate;
            conversation.terminal_outcome = Some(terminal.clone());
            operation.state = OperationState::Indeterminate;
            operation.terminal_outcome = Some(terminal);
            write_conversation(connection, &conversation)?;
            operation::write(connection, &operation)?;
            conversation_timeline::append(
                connection,
                conversation.conversation_id,
                ConversationEventPayload::ConversationTerminal {
                    outcome: conversation.terminal_outcome.clone().ok_or_else(|| {
                        OperatorError::State("terminal outcome was absent".to_owned())
                    })?,
                },
            )?;
        }
        operation::recover_in_connection(connection)?;
        Ok(())
    })
}

fn new_operation(request: &ConversationStart, session_id: SessionId) -> Operation {
    Operation {
        operation_id: OperationId::new(),
        request_id: request.request_id,
        project_id: request.project_id.clone(),
        intent: request.intent.clone(),
        session_id,
        state: OperationState::Accepted,
        observed_session_id: None,
        observed_model: None,
        observed_claude_version: None,
        terminal_outcome: None,
    }
}

fn insert_operation(
    connection: &ConnectionThreadSafe,
    operation: &Operation,
    fingerprint: &str,
) -> Result<(), OperatorError> {
    let record = encode(operation)?;
    let request_key = operation.request_id.value().to_string();
    let operation_key = operation.operation_id.value().to_string();
    let session_key = operation.session_id.value().to_string();
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
                (4, fingerprint),
                (5, record.as_str()),
            ][..],
        )
        .map_err(sql_error)?;
    statement.next().map_err(sql_error)?;
    Ok(())
}

pub(super) fn find(
    connection: &ConnectionThreadSafe,
    conversation_id: ConversationId,
) -> Result<Option<Conversation>, OperatorError> {
    let conversation_key = conversation_id.operation_id().value().to_string();
    let mut statement = connection
        .prepare("SELECT record_json FROM conversations WHERE conversation_id = ?")
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

fn insert_conversation(
    connection: &ConnectionThreadSafe,
    conversation: &Conversation,
) -> Result<(), OperatorError> {
    let record = encode(conversation)?;
    let conversation_key = conversation
        .conversation_id
        .operation_id()
        .value()
        .to_string();
    let operation_key = conversation
        .conversation_id
        .operation_id()
        .value()
        .to_string();
    let mut statement = connection
        .prepare(
            "INSERT INTO conversations (conversation_id, operation_id, record_json) VALUES (?, ?, ?)",
        )
        .map_err(sql_error)?;
    statement
        .bind(
            &[
                (1, conversation_key.as_str()),
                (2, operation_key.as_str()),
                (3, record.as_str()),
            ][..],
        )
        .map_err(sql_error)?;
    statement.next().map_err(sql_error)?;
    Ok(())
}

fn write_conversation(
    connection: &ConnectionThreadSafe,
    conversation: &Conversation,
) -> Result<(), OperatorError> {
    let record = encode(conversation)?;
    let conversation_key = conversation
        .conversation_id
        .operation_id()
        .value()
        .to_string();
    let mut statement = connection
        .prepare("UPDATE conversations SET record_json = ? WHERE conversation_id = ?")
        .map_err(sql_error)?;
    statement
        .bind(&[(1, record.as_str()), (2, conversation_key.as_str())][..])
        .map_err(sql_error)?;
    statement.next().map_err(sql_error)?;
    Ok(())
}

fn terminal_states_agree(
    conversation_state: ConversationState,
    operation_state: OperationState,
    terminal: &TerminalOutcome,
) -> bool {
    match (conversation_state, operation_state, terminal) {
        (
            ConversationState::Succeeded,
            OperationState::Succeeded,
            TerminalOutcome::Succeeded(_),
        )
        | (
            ConversationState::Cancelled,
            OperationState::Cancelled,
            TerminalOutcome::Cancelled(_),
        )
        | (ConversationState::Failed, OperationState::Failed, TerminalOutcome::Failed(_))
        | (
            ConversationState::Indeterminate,
            OperationState::Indeterminate,
            TerminalOutcome::Indeterminate(_),
        ) => true,
        (ConversationState::Open, _, _)
        | (ConversationState::Closing, _, _)
        | (_, OperationState::Accepted, _)
        | (_, OperationState::Running, _)
        | (_, _, TerminalOutcome::Succeeded(_))
        | (_, _, TerminalOutcome::Cancelled(_))
        | (_, _, TerminalOutcome::Failed(_))
        | (_, _, TerminalOutcome::Indeterminate(_)) => false,
    }
}

pub(super) fn unknown(conversation_id: ConversationId) -> OperatorError {
    OperatorError::UnknownOperation(conversation_id.operation_id().value().to_string())
}
