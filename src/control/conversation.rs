// Copyright 2026 Yuriy Krasilnikov
// SPDX-License-Identifier: Apache-2.0

//! Orchestrates one durable live conversation through the single Target boundary.

use std::{
    sync::{Arc, mpsc},
    thread,
    time::{Duration, Instant},
};

use sha2::{Digest, Sha256};

use crate::contract::{
    control::{
        ConversationEventPayload, ConversationId, ConversationSend, ConversationSnapshot,
        ConversationStart, ConversationStartAdmission, ConversationState, ConversationStopMode,
        ConversationTurnAdmission, ConversationWait, OperationId, OperationState, OperatorError,
        SessionId, TerminalOutcome, TurnId, TurnState,
    },
    target::{
        TargetIntent, TargetLiveObservation, TargetLiveStart, TargetLiveStop, TargetLiveTurn,
        TargetOperationId, TargetSessionId, TargetTurnId,
    },
};

use super::{OperationControl, project};

const CURSOR_POLL: Duration = Duration::from_millis(10);

pub(super) fn start(
    control: &OperationControl,
    request: ConversationStart,
) -> Result<ConversationSnapshot, OperatorError> {
    request.validate()?;
    let project = project::resolve(control.state.as_ref(), &request.project_id)?;
    let fingerprint = fingerprint(&request)?;
    let session_id = session_for(&request);
    let (operation_id, first_turn) =
        match control
            .state
            .persist_conversation_start(&request, session_id, &fingerprint)?
        {
            ConversationStartAdmission::Existing {
                operation: _,
                conversation,
                first_turn: _,
                fingerprint: existing,
            } => {
                ensure_fingerprint(&fingerprint, &existing)?;
                return control
                    .state
                    .get_conversation_snapshot(conversation.conversation_id, 0);
            }
            ConversationStartAdmission::ExistingOperation {
                fingerprint: existing,
                ..
            } => {
                ensure_fingerprint(&fingerprint, &existing)?;
                return Err(OperatorError::Conflict(
                    "request UUID is already occupied by a one-shot operation".to_owned(),
                ));
            }
            ConversationStartAdmission::MissingProject => {
                return Err(OperatorError::UnknownProject(
                    request.project_id.to_string(),
                ));
            }
            ConversationStartAdmission::ActiveSession { operation } => {
                return if operation.state == OperationState::Indeterminate {
                    Err(OperatorError::UnclassifiedSession(
                        operation.session_id.to_string(),
                    ))
                } else {
                    Err(OperatorError::Conflict(
                        "target session already has a current writer".to_owned(),
                    ))
                };
            }
            ConversationStartAdmission::Inserted {
                operation,
                conversation: _,
                first_turn,
            } => (operation.operation_id, first_turn),
        };
    let gate = match control.runtime.admit(operation_id) {
        Ok(gate) => gate,
        Err(error) => {
            terminalize(
                control,
                ConversationId::new(operation_id),
                ConversationState::Indeterminate,
                TerminalOutcome::Indeterminate(error.to_string()),
                crate::contract::control::SessionClaimDisposition::ReleaseProvenWriter,
            )?;
            return Err(error);
        }
    };
    let (permission_sender, permission_receiver) = mpsc::channel();
    let (observation_sender, observation_receiver) = mpsc::channel();
    let target_start = TargetLiveStart {
        operation_id: TargetOperationId(operation_id.value()),
        working_directory: project.working_directory,
        executable: project.claude_executable,
        expected_model: project.expected_opus_model,
        intent: target_intent(&request),
        session_id: TargetSessionId(session_id.value()),
        first_turn: target_turn(&first_turn),
        running_permission: permission_receiver,
    };
    if let Err(error) = control.target.start_live(target_start, observation_sender) {
        return fail_before_live_worker(control, operation_id, error);
    }
    if let Err(error) = control.state.transition(
        operation_id,
        OperationState::Running,
        None,
        None,
        None,
        None,
    ) {
        return stop_after_live_start(control, operation_id, error);
    }
    let worker = control.clone();
    let spawn = thread::Builder::new()
        .name(format!("aiop-live-observations-{}", operation_id.value()))
        .spawn(move || observe(worker, operation_id, observation_receiver, gate));
    if let Err(error) = spawn {
        return stop_after_live_start(
            control,
            operation_id,
            OperatorError::State(format!("live observation worker could not start: {error}")),
        );
    }
    if permission_sender.send(Ok(())).is_err() {
        let error = OperatorError::Indeterminate(
            "live target stopped before Control granted durable Running permission".to_owned(),
        );
        control
            .target
            .stop_live(
                TargetOperationId(operation_id.value()),
                TargetLiveStop::Cancel,
            )
            .map_err(|termination| {
                OperatorError::Indeterminate(format!(
                    "{error}; direct live child termination failed: {termination}"
                ))
            })?;
        return Err(error);
    }
    control
        .state
        .get_conversation_snapshot(ConversationId::new(operation_id), 0)
}

pub(super) fn send(
    control: &OperationControl,
    request: ConversationSend,
) -> Result<ConversationSnapshot, OperatorError> {
    request.validate()?;
    let fingerprint = fingerprint(&request)?;
    match control
        .state
        .persist_conversation_turn(&request, &fingerprint)?
    {
        ConversationTurnAdmission::Existing {
            turn: _,
            fingerprint: existing,
        } => {
            ensure_fingerprint(&fingerprint, &existing)?;
            control
                .state
                .get_conversation_snapshot(request.conversation_id, 0)
        }
        ConversationTurnAdmission::MissingConversation => Err(OperatorError::UnknownOperation(
            request.conversation_id.operation_id().value().to_string(),
        )),
        ConversationTurnAdmission::Closed { .. } => Err(OperatorError::Conflict(
            "conversation no longer admits turns".to_owned(),
        )),
        ConversationTurnAdmission::Inserted(turn) => {
            let target_turn = target_turn(&turn);
            if let Err(error) = control.target.send_live(
                TargetOperationId(request.conversation_id.operation_id().value()),
                target_turn,
            ) {
                let snapshot = control
                    .state
                    .get_conversation_snapshot(request.conversation_id, 0)?;
                if snapshot.conversation.state.terminal()
                    || snapshot.conversation.close_mode == Some(ConversationStopMode::Cancel)
                {
                    return Ok(snapshot);
                }
                control
                    .state
                    .close_conversation(request.conversation_id, ConversationStopMode::Cancel)?;
                let stop = control.target.stop_live(
                    TargetOperationId(request.conversation_id.operation_id().value()),
                    TargetLiveStop::Cancel,
                );
                if let Err(stop_error) = stop {
                    return Err(OperatorError::Indeterminate(format!(
                        "{error}; direct live child termination failed: {stop_error}"
                    )));
                }
                return Err(OperatorError::Indeterminate(error));
            }
            control
                .state
                .get_conversation_snapshot(request.conversation_id, 0)
        }
    }
}

pub(super) fn wait(
    control: &OperationControl,
    request: ConversationWait,
) -> Result<ConversationSnapshot, OperatorError> {
    request.validate()?;
    let deadline = Instant::now()
        .checked_add(Duration::from_millis(request.wait_millis))
        .ok_or_else(|| {
            OperatorError::InvalidRequest("wait duration cannot be represented".to_owned())
        })?;
    loop {
        control.refusal()?;
        let snapshot = control
            .state
            .get_conversation_snapshot(request.conversation_id, request.after_sequence)?;
        if !snapshot.events.is_empty()
            || snapshot.conversation.state.terminal()
            || Instant::now() >= deadline
        {
            return Ok(snapshot);
        }
        thread::sleep(CURSOR_POLL);
    }
}

pub(super) fn stop(
    control: &OperationControl,
    conversation_id: ConversationId,
    mode: ConversationStopMode,
) -> Result<ConversationSnapshot, OperatorError> {
    let close = control.state.close_conversation(conversation_id, mode)?;
    let target_mode = match close {
        crate::contract::control::ConversationCloseAdmission::ClosedNow {
            through_position,
            ..
        } => match mode {
            ConversationStopMode::Graceful => TargetLiveStop::Graceful { through_position },
            ConversationStopMode::Cancel => TargetLiveStop::Cancel,
        },
        crate::contract::control::ConversationCloseAdmission::EscalatedToCancel(_) => {
            TargetLiveStop::Cancel
        }
        crate::contract::control::ConversationCloseAdmission::AlreadyClosing(_)
        | crate::contract::control::ConversationCloseAdmission::Terminal(_) => {
            return control.state.get_conversation_snapshot(conversation_id, 0);
        }
    };
    if let Err(error) = control.target.stop_live(
        TargetOperationId(conversation_id.operation_id().value()),
        target_mode,
    ) {
        let snapshot = control
            .state
            .get_conversation_snapshot(conversation_id, 0)?;
        if snapshot.conversation.state.terminal() {
            return Ok(snapshot);
        }
        return Err(OperatorError::Indeterminate(format!(
            "live target stop request could not be observed: {error}"
        )));
    }
    control.state.get_conversation_snapshot(conversation_id, 0)
}

fn observe(
    control: OperationControl,
    operation_id: OperationId,
    observations: mpsc::Receiver<TargetLiveObservation>,
    _gate: Arc<std::sync::atomic::AtomicBool>,
) {
    let conversation_id = ConversationId::new(operation_id);
    for observation in observations {
        match control.state.get_conversation(conversation_id) {
            Ok(conversation) if conversation.state.terminal() => break,
            Ok(_) => {}
            Err(error) => {
                record_state_refusal(&control, &error);
                break;
            }
        }
        let result = apply_observation(&control, conversation_id, observation);
        if let Err(error) = result {
            match control.state.get_conversation(conversation_id) {
                Ok(conversation) if conversation.state.terminal() => break,
                Ok(_) => {}
                Err(state_error) => {
                    record_state_refusal(&control, &state_error);
                    break;
                }
            }
            let stop = control.target.stop_live(
                TargetOperationId(operation_id.value()),
                TargetLiveStop::Cancel,
            );
            let terminal = terminalize(
                &control,
                conversation_id,
                ConversationState::Indeterminate,
                TerminalOutcome::Indeterminate(error.to_string()),
                crate::contract::control::SessionClaimDisposition::RetainUnclassifiedWriter,
            );
            let causal = compose_observation_failure(error, stop, terminal);
            record_state_refusal(&control, &causal);
            break;
        }
        let conversation = match control.state.get_conversation(conversation_id) {
            Ok(conversation) => conversation,
            Err(error) => {
                record_state_refusal(&control, &error);
                break;
            }
        };
        if conversation.state.terminal() {
            break;
        }
    }
    match control.state.get_conversation(conversation_id) {
        Ok(conversation) if !conversation.state.terminal() => {
            let error = terminalize(
                &control,
                conversation_id,
                ConversationState::Indeterminate,
                TerminalOutcome::Indeterminate(
                    "live target observation channel ended without a terminal outcome".to_owned(),
                ),
                crate::contract::control::SessionClaimDisposition::RetainUnclassifiedWriter,
            );
            if let Err(error) = error {
                record_state_refusal(&control, &error);
            }
        }
        Ok(_) => {}
        Err(error) => record_state_refusal(&control, &error),
    }
    if let Err(error) = control.runtime.release(operation_id) {
        record_state_refusal(&control, &error);
    }
}

fn record_state_refusal(control: &OperationControl, error: &OperatorError) {
    if matches!(error, OperatorError::State(_)) {
        control.record_refusal(error.clone());
    }
}

fn compose_observation_failure(
    root: OperatorError,
    stop: Result<(), String>,
    terminal: Result<(), OperatorError>,
) -> OperatorError {
    match (stop, terminal) {
        (Ok(()), Ok(())) => root,
        (Err(stop_error), Ok(())) => OperatorError::Indeterminate(format!(
            "{root}; direct live child termination failed: {stop_error}"
        )),
        (Ok(()), Err(terminal_error)) => OperatorError::State(format!(
            "{root}; durable indeterminate transition failed: {terminal_error}"
        )),
        (Err(stop_error), Err(terminal_error)) => OperatorError::State(format!(
            "{root}; direct live child termination failed: {stop_error}; durable indeterminate transition failed: {terminal_error}"
        )),
    }
}

fn apply_observation(
    control: &OperationControl,
    conversation_id: ConversationId,
    observation: TargetLiveObservation,
) -> Result<(), OperatorError> {
    match observation {
        TargetLiveObservation::Initialized {
            session_id,
            model,
            version,
        } => {
            control.state.record_conversation_initialization(
                conversation_id,
                SessionId::new_exact(session_id.0),
                model,
                version,
            )?;
        }
        TargetLiveObservation::TurnQueued { .. } => {}
        TargetLiveObservation::TurnStarted { turn_id } => record_turn(
            control,
            conversation_id,
            turn_id,
            TurnState::Started,
            None,
            ConversationEventPayload::TurnStarted {
                turn_id: TurnId::new(turn_id.0),
            },
        )?,
        TargetLiveObservation::TurnAcknowledged { turn_id } => {
            control.state.record_conversation_turn_observation(
                conversation_id,
                TurnId::new(turn_id.0),
                None,
                None,
                ConversationEventPayload::TurnAcknowledged {
                    turn_id: TurnId::new(turn_id.0),
                },
            )?;
        }
        TargetLiveObservation::AssistantTextDelta { turn_id, text } => {
            control.state.record_conversation_turn_observation(
                conversation_id,
                TurnId::new(turn_id.0),
                None,
                None,
                ConversationEventPayload::AssistantTextDelta {
                    turn_id: TurnId::new(turn_id.0),
                    text,
                },
            )?;
        }
        TargetLiveObservation::TurnCompleted { turn_id, result } => record_turn(
            control,
            conversation_id,
            turn_id,
            TurnState::Completed,
            Some(result.clone()),
            ConversationEventPayload::TurnCompleted {
                turn_id: TurnId::new(turn_id.0),
                result,
            },
        )?,
        TargetLiveObservation::TurnFailed { turn_id, message } => record_turn(
            control,
            conversation_id,
            turn_id,
            TurnState::Failed,
            None,
            ConversationEventPayload::TurnFailed {
                turn_id: TurnId::new(turn_id.0),
                message,
            },
        )?,
        TargetLiveObservation::Failed(message) => terminalize(
            control,
            conversation_id,
            ConversationState::Failed,
            TerminalOutcome::Failed(message),
            crate::contract::control::SessionClaimDisposition::ReleaseProvenWriter,
        )?,
        TargetLiveObservation::Indeterminate(message) => terminalize(
            control,
            conversation_id,
            ConversationState::Indeterminate,
            TerminalOutcome::Indeterminate(message),
            crate::contract::control::SessionClaimDisposition::ReleaseProvenWriter,
        )?,
        TargetLiveObservation::UnclassifiedWriter(message) => terminalize(
            control,
            conversation_id,
            ConversationState::Indeterminate,
            TerminalOutcome::Indeterminate(message),
            crate::contract::control::SessionClaimDisposition::RetainUnclassifiedWriter,
        )?,
        TargetLiveObservation::Cancelled => terminalize(
            control,
            conversation_id,
            ConversationState::Cancelled,
            TerminalOutcome::Cancelled("direct live child termination was observed".to_owned()),
            crate::contract::control::SessionClaimDisposition::ReleaseProvenWriter,
        )?,
        TargetLiveObservation::Exited => {
            let snapshot = control
                .state
                .get_conversation_snapshot(conversation_id, 0)?;
            let result = snapshot
                .turns
                .iter()
                .rev()
                .find_map(|turn| turn.result.clone())
                .ok_or_else(|| {
                    OperatorError::State("successful live exit had no turn result".to_owned())
                })?;
            terminalize(
                control,
                conversation_id,
                ConversationState::Succeeded,
                TerminalOutcome::Succeeded(result),
                crate::contract::control::SessionClaimDisposition::ReleaseProvenWriter,
            )?;
        }
    }
    Ok(())
}

fn record_turn(
    control: &OperationControl,
    conversation_id: ConversationId,
    turn_id: TargetTurnId,
    state: TurnState,
    result: Option<String>,
    payload: ConversationEventPayload,
) -> Result<(), OperatorError> {
    control.state.record_conversation_turn_observation(
        conversation_id,
        TurnId::new(turn_id.0),
        Some(state),
        result,
        payload,
    )?;
    Ok(())
}

fn terminalize(
    control: &OperationControl,
    conversation_id: ConversationId,
    state: ConversationState,
    outcome: TerminalOutcome,
    claim_disposition: crate::contract::control::SessionClaimDisposition,
) -> Result<(), OperatorError> {
    let operation_state = match state {
        ConversationState::Succeeded => OperationState::Succeeded,
        ConversationState::Cancelled => OperationState::Cancelled,
        ConversationState::Failed => OperationState::Failed,
        ConversationState::Indeterminate => OperationState::Indeterminate,
        ConversationState::Open | ConversationState::Closing => {
            return Err(OperatorError::State(
                "live terminal state was not terminal".to_owned(),
            ));
        }
    };
    control.state.terminalize_conversation(
        conversation_id,
        state,
        operation_state,
        outcome,
        claim_disposition,
    )?;
    Ok(())
}

fn fail_before_live_worker(
    control: &OperationControl,
    operation_id: OperationId,
    error: crate::contract::target::TargetLiveStartError,
) -> Result<ConversationSnapshot, OperatorError> {
    match error {
        crate::contract::target::TargetLiveStartError::NoWriter(message)
        | crate::contract::target::TargetLiveStartError::CleanupProvenExited(message) => {
            finish_without_worker(
                control,
                operation_id,
                ConversationState::Failed,
                TerminalOutcome::Failed(format!("live target could not start: {message}")),
                crate::contract::control::SessionClaimDisposition::ReleaseProvenWriter,
                None,
            )
        }
        crate::contract::target::TargetLiveStartError::CleanupUnproven(message) => {
            finish_without_worker(
                control,
                operation_id,
                ConversationState::Indeterminate,
                TerminalOutcome::Indeterminate(format!(
                    "live target startup did not prove direct-child exit: {message}"
                )),
                crate::contract::control::SessionClaimDisposition::RetainUnclassifiedWriter,
                None,
            )
        }
    }
}

fn stop_after_live_start(
    control: &OperationControl,
    operation_id: OperationId,
    error: OperatorError,
) -> Result<ConversationSnapshot, OperatorError> {
    let stop = control.target.stop_live(
        TargetOperationId(operation_id.value()),
        TargetLiveStop::Cancel,
    );
    let outcome = TerminalOutcome::Indeterminate(error.to_string());
    finish_without_worker(
        control,
        operation_id,
        ConversationState::Indeterminate,
        outcome,
        crate::contract::control::SessionClaimDisposition::RetainUnclassifiedWriter,
        stop.err(),
    )
}

fn finish_without_worker(
    control: &OperationControl,
    operation_id: OperationId,
    state: ConversationState,
    outcome: TerminalOutcome,
    claim_disposition: crate::contract::control::SessionClaimDisposition,
    termination_error: Option<String>,
) -> Result<ConversationSnapshot, OperatorError> {
    let terminal = terminalize(
        control,
        ConversationId::new(operation_id),
        state,
        outcome,
        claim_disposition,
    );
    let release = control.runtime.release(operation_id);
    match (terminal, release, termination_error) {
        (Ok(()), Ok(()), None) => control
            .state
            .get_conversation_snapshot(ConversationId::new(operation_id), 0),
        (terminal, release, termination) => Err(OperatorError::State(format!(
            "live startup cleanup: terminal={terminal:?}; runtime_release={release:?}; direct_child_termination={termination:?}"
        ))),
    }
}

fn session_for(request: &ConversationStart) -> SessionId {
    match request.intent {
        crate::contract::control::OperationIntent::New => SessionId::new(),
        crate::contract::control::OperationIntent::ResumeExact { session_id } => session_id,
    }
}

fn target_intent(request: &ConversationStart) -> TargetIntent {
    match request.intent {
        crate::contract::control::OperationIntent::New => TargetIntent::New,
        crate::contract::control::OperationIntent::ResumeExact { session_id } => {
            TargetIntent::ResumeExact {
                session_id: TargetSessionId(session_id.value()),
            }
        }
    }
}

fn target_turn(turn: &crate::contract::control::ConversationTurn) -> TargetLiveTurn {
    TargetLiveTurn {
        turn_id: TargetTurnId(turn.turn_id.value()),
        position: turn.position,
        prompt: turn.prompt.clone(),
    }
}

fn ensure_fingerprint(request: &str, existing: &str) -> Result<(), OperatorError> {
    if request == existing {
        Ok(())
    } else {
        Err(OperatorError::Conflict(
            "request UUID was reused with different conversation content".to_owned(),
        ))
    }
}

fn fingerprint<T: serde::Serialize>(request: &T) -> Result<String, OperatorError> {
    let encoded =
        serde_json::to_vec(request).map_err(|error| OperatorError::State(error.to_string()))?;
    Ok(format!("{:x}", Sha256::digest(encoded)))
}
