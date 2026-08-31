// Copyright 2026 Yuriy Krasilnikov
// SPDX-License-Identifier: Apache-2.0

mod support;

use std::{
    fs,
    io::BufReader,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    time::{Duration, Instant},
};

use aiop::{
    contract::control::{
        ConversationEventPayload, ConversationId, ConversationSend, ConversationStart,
        ConversationStartAdmission, ConversationState, ConversationTurnAdmission,
        OperationAdmission, OperationIntent, OperationStart, OperationState, OperatorError,
        ProjectId, ProjectRegistration, RequestId, ReviewProfile, SessionId, StatePort,
        TerminalOutcome, TurnId, TurnState,
    },
    contract::target::{
        TargetCommand, TargetIntent, TargetLiveObservation, TargetLiveStart, TargetLiveStartError,
        TargetLiveStop, TargetLiveTurn, TargetOperationId, TargetOutcome, TargetPort,
        TargetSessionId, TargetTurnId,
    },
    control::OperationControl,
    state::SqliteState,
    target::ClaudeTarget,
};
use tempfile::TempDir;
use uuid::Uuid;

use support::{call_tool, initialize_mcp, start_daemon, start_mcp};

fn project(directory: &TempDir) -> ProjectRegistration {
    ProjectRegistration {
        project_id: ProjectId::new("v03-state-project".to_owned())
            .expect("fixture project id is valid"),
        working_directory: directory.path().to_path_buf(),
        claude_executable: PathBuf::from("fixture-target"),
        expected_opus_model: "opus".to_owned(),
    }
}

fn start(project: &ProjectRegistration, turn_id: TurnId) -> ConversationStart {
    ConversationStart {
        request_id: RequestId::new(Uuid::new_v4()),
        project_id: project.project_id.clone(),
        intent: OperationIntent::New,
        turn_id,
        prompt: "first prompt".to_owned(),
        review_profile: ReviewProfile::OpusReadOnly,
    }
}

#[derive(Default)]
struct AmbiguousLiveTarget {
    send_attempts: AtomicUsize,
    cancel_requested: AtomicBool,
    observations: Mutex<Option<mpsc::Sender<TargetLiveObservation>>>,
}

impl TargetPort for AmbiguousLiveTarget {
    fn execute(&self, _: TargetCommand) -> TargetOutcome {
        TargetOutcome::Failed("scripted target does not execute one-shot work".to_owned())
    }

    fn cancel(&self, _: TargetOperationId) -> Result<(), String> {
        Ok(())
    }

    fn start_live(
        &self,
        start: TargetLiveStart,
        observations: mpsc::Sender<TargetLiveObservation>,
    ) -> Result<(), TargetLiveStartError> {
        let stored = self.observations.lock().map_err(|_| {
            TargetLiveStartError::NoWriter("scripted observation storage poisoned".to_owned())
        })?;
        if stored.is_some() {
            return Err(TargetLiveStartError::NoWriter(
                "scripted target received a second live start".to_owned(),
            ));
        }
        drop(stored);
        let permission = start.running_permission;
        std::thread::Builder::new()
            .name("scripted-live-permission".to_owned())
            .spawn(move || {
                let permission_result = permission.recv();
                if permission_result.is_err() {
                    eprintln!("scripted target did not receive live running permission");
                }
            })
            .map_err(|error| {
                TargetLiveStartError::NoWriter(format!(
                    "scripted permission thread could not start: {error}"
                ))
            })?;
        let mut stored = self.observations.lock().map_err(|_| {
            TargetLiveStartError::NoWriter("scripted observation storage poisoned".to_owned())
        })?;
        *stored = Some(observations);
        Ok(())
    }

    fn send_live(&self, _: TargetOperationId, _: TargetLiveTurn) -> Result<(), String> {
        self.send_attempts.fetch_add(1, Ordering::SeqCst);
        Err("scripted input delivery is ambiguous".to_owned())
    }

    fn stop_live(&self, _: TargetOperationId, stop: TargetLiveStop) -> Result<(), String> {
        match stop {
            TargetLiveStop::Graceful { .. } => Err("scripted target expected cancel".to_owned()),
            TargetLiveStop::Cancel => {
                self.cancel_requested.store(true, Ordering::SeqCst);
                let mut stored = self
                    .observations
                    .lock()
                    .map_err(|_| "scripted observation storage poisoned".to_owned())?;
                *stored = None;
                Ok(())
            }
        }
    }
}

fn assert_live_protocol_failure(prompt: &str) {
    let directory = TempDir::new().expect("temporary test directory is available");
    let target = ClaudeTarget::default();
    let operation_id = TargetOperationId(Uuid::new_v4());
    let session_id = TargetSessionId(Uuid::new_v4());
    let (observations, receiver) = mpsc::channel();
    let (grant, permission) = mpsc::channel();
    target
        .start_live(
            TargetLiveStart {
                operation_id,
                working_directory: directory.path().to_path_buf(),
                executable: PathBuf::from(env!("CARGO_BIN_EXE_aiop-fake-claude")),
                expected_model: "opus".to_owned(),
                intent: TargetIntent::New,
                session_id,
                first_turn: TargetLiveTurn {
                    turn_id: TargetTurnId(Uuid::new_v4()),
                    position: 1,
                    prompt: prompt.to_owned(),
                },
                running_permission: permission,
            },
            observations,
        )
        .expect("live target starts");
    grant.send(Ok(())).expect("Control grants durable Running");
    loop {
        match receiver
            .recv_timeout(Duration::from_secs(5))
            .expect("protocol failure produces a terminal observation")
        {
            TargetLiveObservation::Indeterminate(message) => {
                assert!(!message.contains("fixture-live-stderr-marker"));
                return;
            }
            TargetLiveObservation::UnclassifiedWriter(_) => {
                panic!("deterministic fake exit must prove direct-child cleanup")
            }
            TargetLiveObservation::Initialized { .. }
            | TargetLiveObservation::TurnQueued { .. }
            | TargetLiveObservation::TurnStarted { .. }
            | TargetLiveObservation::TurnAcknowledged { .. }
            | TargetLiveObservation::AssistantTextDelta { .. }
            | TargetLiveObservation::TurnCompleted { .. }
            | TargetLiveObservation::TurnFailed { .. } => {}
            TargetLiveObservation::Cancelled
            | TargetLiveObservation::Exited
            | TargetLiveObservation::Failed(_) => {
                panic!("protocol failure must be indeterminate")
            }
        }
    }
}

#[test]
fn live_protocol_identity_and_stream_failures_are_indeterminate_without_stderr_leakage() {
    for prompt in [
        "__fixture_live_session_mismatch__",
        "__fixture_live_model_mismatch__",
        "__fixture_live_uuid_mismatch__",
        "__fixture_live_content_mismatch__",
        "__fixture_live_invalid_json__",
        "__fixture_live_unknown_lifecycle__",
        "__fixture_live_lifecycle_order_mismatch__",
        "__fixture_live_malformed_tool_result__",
        "__fixture_live_result_without_started__",
        "__fixture_live_unexpected_exit__",
    ] {
        assert_live_protocol_failure(prompt);
    }
}

#[test]
fn durable_conversation_admission_orders_turns_events_and_recovers_active_claim() {
    let directory = TempDir::new().expect("temporary test directory is available");
    let database = directory.path().join("operator.sqlite");
    let project = project(&directory);
    let state = SqliteState::open(&database).expect("SQLite state opens");
    state
        .register_project(project.clone())
        .expect("project registers");
    let session_id = SessionId::new_exact(Uuid::new_v4());
    let first_turn_id = TurnId::new(Uuid::new_v4());
    let request = start(&project, first_turn_id);
    let (operation, conversation) = match state
        .persist_conversation_start(&request, session_id, "first request")
        .expect("conversation start persists")
    {
        ConversationStartAdmission::Inserted {
            operation,
            conversation,
            first_turn,
        } => {
            assert_eq!(first_turn.position, 1);
            (operation, conversation)
        }
        ConversationStartAdmission::Existing { .. }
        | ConversationStartAdmission::ExistingOperation { .. }
        | ConversationStartAdmission::MissingProject
        | ConversationStartAdmission::ActiveSession { .. } => {
            panic!("first conversation admission must insert")
        }
    };
    match state
        .persist_conversation_start(&request, session_id, "first request")
        .expect("same conversation start reads durable fact")
    {
        ConversationStartAdmission::Existing {
            operation: existing,
            conversation: existing_conversation,
            first_turn,
            fingerprint,
        } => {
            assert_eq!(existing, operation);
            assert_eq!(existing_conversation, conversation);
            assert_eq!(first_turn.position, 1);
            assert_eq!(fingerprint, "first request");
        }
        ConversationStartAdmission::MissingProject
        | ConversationStartAdmission::ExistingOperation { .. }
        | ConversationStartAdmission::ActiveSession { .. }
        | ConversationStartAdmission::Inserted { .. } => {
            panic!("same conversation start must not create another operation")
        }
    }
    let first_initialization = state
        .record_conversation_initialization(
            conversation.conversation_id,
            session_id,
            "opus".to_owned(),
            Some("fixture-1".to_owned()),
        )
        .expect("first provider initialization persists");
    assert_eq!(first_initialization.sequence, 2);
    let second_turn_id = TurnId::new(Uuid::new_v4());
    let second_turn = ConversationSend {
        conversation_id: conversation.conversation_id,
        turn_id: second_turn_id,
        prompt: "second prompt".to_owned(),
    };
    match state
        .persist_conversation_turn(&second_turn, "second request")
        .expect("second turn persists")
    {
        ConversationTurnAdmission::Inserted(turn) => assert_eq!(turn.position, 2),
        ConversationTurnAdmission::Existing { .. }
        | ConversationTurnAdmission::MissingConversation
        | ConversationTurnAdmission::Closed { .. } => {
            panic!("second turn must receive the next durable position")
        }
    }
    match state
        .persist_conversation_turn(&second_turn, "second request")
        .expect("same turn reads durable fact")
    {
        ConversationTurnAdmission::Existing { turn, fingerprint } => {
            assert_eq!(turn.position, 2);
            assert_eq!(fingerprint, "second request");
        }
        ConversationTurnAdmission::MissingConversation
        | ConversationTurnAdmission::Closed { .. }
        | ConversationTurnAdmission::Inserted(_) => {
            panic!("same turn must not receive another position")
        }
    }
    let second_initialization = state
        .record_conversation_initialization(
            conversation.conversation_id,
            session_id,
            "opus".to_owned(),
            Some("fixture-1".to_owned()),
        )
        .expect("repeated provider initialization persists");
    assert_eq!(second_initialization.sequence, 4);
    assert_eq!(
        state
            .get_conversation_snapshot(conversation.conversation_id, 1)
            .expect("production snapshot cursor reads")
            .events,
        vec![
            aiop::contract::control::ConversationEvent {
                conversation_id: conversation.conversation_id,
                sequence: 2,
                payload: ConversationEventPayload::Initialized {
                    session_id,
                    model: "opus".to_owned(),
                    claude_version: Some("fixture-1".to_owned()),
                },
            },
            aiop::contract::control::ConversationEvent {
                conversation_id: conversation.conversation_id,
                sequence: 3,
                payload: ConversationEventPayload::TurnQueued {
                    turn_id: second_turn_id,
                },
            },
            second_initialization,
        ]
    );
    match state
        .close_conversation(
            conversation.conversation_id,
            aiop::contract::control::ConversationStopMode::Graceful,
        )
        .expect("conversation closes admission")
    {
        aiop::contract::control::ConversationCloseAdmission::ClosedNow {
            conversation,
            through_position,
        } => {
            assert_eq!(conversation.state, ConversationState::Closing);
            assert_eq!(through_position, 2);
        }
        aiop::contract::control::ConversationCloseAdmission::AlreadyClosing(_)
        | aiop::contract::control::ConversationCloseAdmission::EscalatedToCancel(_)
        | aiop::contract::control::ConversationCloseAdmission::Terminal(_) => {
            panic!("open conversation must be atomically closed by the first caller")
        }
    }
    match state
        .persist_conversation_turn(
            &ConversationSend {
                conversation_id: conversation.conversation_id,
                turn_id: TurnId::new(Uuid::new_v4()),
                prompt: "rejected prompt".to_owned(),
            },
            "rejected request",
        )
        .expect("closed conversation returns a fact")
    {
        ConversationTurnAdmission::Closed {
            conversation: closed,
        } => assert_eq!(closed.state, ConversationState::Closing),
        ConversationTurnAdmission::Existing { .. }
        | ConversationTurnAdmission::MissingConversation
        | ConversationTurnAdmission::Inserted(_) => {
            panic!("closed conversation must not admit another turn")
        }
    }
    drop(state);

    let reopened = SqliteState::open(&database).expect("state reopens after daemon restart");
    assert_eq!(
        reopened
            .get_conversation(ConversationId::new(operation.operation_id))
            .expect("conversation remains durable")
            .state,
        ConversationState::Indeterminate
    );
    let recovered_operation = reopened
        .get_operation(operation.operation_id)
        .expect("recovered operation remains durable");
    assert_eq!(
        recovered_operation.terminal_outcome,
        reopened
            .get_conversation(ConversationId::new(operation.operation_id))
            .expect("recovered conversation remains durable")
            .terminal_outcome
    );
    match reopened
        .persist_conversation_start(
            &start(&project, TurnId::new(Uuid::new_v4())),
            session_id,
            "new request",
        )
        .expect("restart writer fact reads")
    {
        ConversationStartAdmission::ActiveSession { operation: claimed } => {
            assert_eq!(claimed.operation_id, operation.operation_id);
        }
        ConversationStartAdmission::Existing { .. }
        | ConversationStartAdmission::ExistingOperation { .. }
        | ConversationStartAdmission::MissingProject
        | ConversationStartAdmission::Inserted { .. } => {
            panic!("restart must retain the unclassified session claim")
        }
    }
}

#[test]
fn exact_session_claim_is_shared_by_live_and_one_shot_admission_in_both_directions() {
    let directory = TempDir::new().expect("temporary test directory is available");
    let project = project(&directory);
    let session_id = SessionId::new_exact(Uuid::new_v4());
    let state =
        SqliteState::open(&directory.path().join("operator.sqlite")).expect("SQLite state opens");
    state
        .register_project(project.clone())
        .expect("project registers");
    let live = start(&project, TurnId::new(Uuid::new_v4()));
    match state
        .persist_conversation_start(&live, session_id, "live writer")
        .expect("live writer admits")
    {
        ConversationStartAdmission::Inserted { .. } => {}
        ConversationStartAdmission::Existing { .. }
        | ConversationStartAdmission::ExistingOperation { .. }
        | ConversationStartAdmission::MissingProject
        | ConversationStartAdmission::ActiveSession { .. } => {
            panic!("live writer must acquire exact session claim")
        }
    }
    let one_shot = OperationStart {
        request_id: RequestId::new(Uuid::new_v4()),
        project_id: project.project_id.clone(),
        intent: OperationIntent::ResumeExact { session_id },
        prompt: "one-shot contender".to_owned(),
        review_profile: ReviewProfile::OpusReadOnly,
    };
    match state
        .persist_operation_admission(&one_shot, session_id, "one-shot contender")
        .expect("one-shot admission reads claim")
    {
        OperationAdmission::ActiveSession { .. } => {}
        other => panic!("one-shot contender must see the live writer claim: {other:?}"),
    }
    drop(state);

    let state = SqliteState::open(&directory.path().join("operator-second.sqlite"))
        .expect("independent SQLite state opens");
    state
        .register_project(project.clone())
        .expect("project registers");
    let one_shot = OperationStart {
        request_id: RequestId::new(Uuid::new_v4()),
        project_id: project.project_id.clone(),
        intent: OperationIntent::ResumeExact { session_id },
        prompt: "one-shot writer".to_owned(),
        review_profile: ReviewProfile::OpusReadOnly,
    };
    match state
        .persist_operation_admission(&one_shot, session_id, "one-shot writer")
        .expect("one-shot writer admits")
    {
        OperationAdmission::Inserted(_) => {}
        other => panic!("one-shot writer must acquire exact session claim: {other:?}"),
    }
    let live = ConversationStart {
        request_id: RequestId::new(Uuid::new_v4()),
        project_id: project.project_id.clone(),
        intent: OperationIntent::ResumeExact { session_id },
        turn_id: TurnId::new(Uuid::new_v4()),
        prompt: "live contender".to_owned(),
        review_profile: ReviewProfile::OpusReadOnly,
    };
    match state
        .persist_conversation_start(&live, session_id, "live contender")
        .expect("live admission reads claim")
    {
        ConversationStartAdmission::ActiveSession { .. } => {}
        other => panic!("live contender must see the one-shot writer claim: {other:?}"),
    }
}

#[test]
fn ambiguous_input_delivery_closes_admission_cancels_without_retry_and_becomes_indeterminate() {
    let directory = TempDir::new().expect("temporary test directory is available");
    let state = Arc::new(
        SqliteState::open(&directory.path().join("operator.sqlite")).expect("SQLite state opens"),
    );
    let project = project(&directory);
    state
        .register_project(project.clone())
        .expect("project registers");
    let target = Arc::new(AmbiguousLiveTarget::default());
    let control_state: Arc<dyn StatePort> = state.clone();
    let control_target: Arc<dyn TargetPort> = target.clone();
    let control = OperationControl::new(control_state, control_target);
    let started = match control
        .handle(aiop::contract::control::DaemonRequest::ConversationStart(
            start(&project, TurnId::new(Uuid::new_v4())),
        ))
        .expect("conversation starts")
    {
        aiop::contract::control::DaemonResponse::Conversation(snapshot) => snapshot,
        other => panic!("conversation start returned another payload: {other:?}"),
    };
    let conversation_id = started.conversation.conversation_id;
    let send = control.handle(aiop::contract::control::DaemonRequest::ConversationSend(
        ConversationSend {
            conversation_id,
            turn_id: TurnId::new(Uuid::new_v4()),
            prompt: "ambiguous delivery".to_owned(),
        },
    ));
    assert!(matches!(send, Err(OperatorError::Indeterminate(_))));
    assert_eq!(target.send_attempts.load(Ordering::SeqCst), 1);
    assert!(target.cancel_requested.load(Ordering::SeqCst));
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(5))
        .expect("fixture deadline is representable");
    let terminal = loop {
        let snapshot = state
            .get_conversation_snapshot(conversation_id, 0)
            .expect("conversation snapshot reads");
        if snapshot.conversation.state.terminal() {
            break snapshot;
        }
        assert!(
            Instant::now() < deadline,
            "ambiguous delivery did not terminalize"
        );
        std::thread::yield_now();
    };
    assert_eq!(
        terminal.conversation.state,
        ConversationState::Indeterminate
    );
    let later = control.handle(aiop::contract::control::DaemonRequest::ConversationSend(
        ConversationSend {
            conversation_id,
            turn_id: TurnId::new(Uuid::new_v4()),
            prompt: "must not be delivered".to_owned(),
        },
    ));
    assert!(matches!(later, Err(OperatorError::Conflict(_))));
    assert_eq!(target.send_attempts.load(Ordering::SeqCst), 1);
}

#[test]
fn completed_turn_can_terminalize_a_conversation_with_one_atomic_terminal_event() {
    let directory = TempDir::new().expect("temporary test directory is available");
    let database = directory.path().join("operator.sqlite");
    let project = project(&directory);
    let state = SqliteState::open(&database).expect("SQLite state opens");
    state
        .register_project(project.clone())
        .expect("project registers");
    let session_id = SessionId::new_exact(Uuid::new_v4());
    let turn_id = TurnId::new(Uuid::new_v4());
    let conversation = match state
        .persist_conversation_start(&start(&project, turn_id), session_id, "start")
        .expect("conversation start persists")
    {
        ConversationStartAdmission::Inserted { conversation, .. } => conversation,
        ConversationStartAdmission::Existing { .. }
        | ConversationStartAdmission::ExistingOperation { .. }
        | ConversationStartAdmission::MissingProject
        | ConversationStartAdmission::ActiveSession { .. } => {
            panic!("new conversation admission must insert")
        }
    };
    state
        .record_conversation_initialization(
            conversation.conversation_id,
            session_id,
            "opus".to_owned(),
            None,
        )
        .expect("provider initialization records qualifying operation identity");
    let started = state
        .record_conversation_turn_observation(
            conversation.conversation_id,
            turn_id,
            Some(TurnState::Started),
            None,
            ConversationEventPayload::TurnStarted { turn_id },
        )
        .expect("turn start and event persist atomically");
    assert_eq!(started.turn.state, TurnState::Started);
    assert_eq!(started.event.sequence, 3);
    let completed = state
        .record_conversation_turn_observation(
            conversation.conversation_id,
            turn_id,
            Some(TurnState::Completed),
            Some("review result".to_owned()),
            ConversationEventPayload::TurnCompleted {
                turn_id,
                result: "review result".to_owned(),
            },
        )
        .expect("turn completion and event persist atomically");
    assert_eq!(completed.turn.state, TurnState::Completed);
    assert_eq!(completed.event.sequence, 4);
    assert_eq!(
        state
            .terminalize_conversation(
                conversation.conversation_id,
                ConversationState::Succeeded,
                OperationState::Succeeded,
                TerminalOutcome::Succeeded("review result".to_owned()),
                aiop::contract::control::SessionClaimDisposition::ReleaseProvenWriter,
            )
            .expect("completed conversation terminalizes")
            .state,
        ConversationState::Succeeded
    );
    let qualifying_evidence = state
        .list_session_evidence(&project.project_id)
        .expect("successful live conversation is qualifying evidence");
    assert_eq!(qualifying_evidence.len(), 1);
    assert_eq!(
        qualifying_evidence[0].operation_id,
        conversation.conversation_id.operation_id()
    );
    assert_eq!(qualifying_evidence[0].target_session_id, session_id);
    assert_eq!(qualifying_evidence[0].observed_model, "opus");
    assert_eq!(qualifying_evidence[0].observed_claude_version, None);
    assert_eq!(
        state
            .inspect_session_evidence(&project.project_id, session_id)
            .expect("qualifying session evidence is inspectable"),
        qualifying_evidence
    );
    let snapshot = state
        .get_conversation_snapshot(conversation.conversation_id, completed.event.sequence)
        .expect("terminal snapshot reads");
    assert_eq!(snapshot.turns.len(), 1);
    assert_eq!(snapshot.turns[0].state, TurnState::Completed);
    assert_eq!(
        snapshot.events,
        vec![aiop::contract::control::ConversationEvent {
            conversation_id: conversation.conversation_id,
            sequence: 5,
            payload: ConversationEventPayload::ConversationTerminal {
                outcome: TerminalOutcome::Succeeded("review result".to_owned()),
            },
        }]
    );
    match state.record_conversation_turn_observation(
        conversation.conversation_id,
        turn_id,
        None,
        None,
        ConversationEventPayload::AssistantTextDelta {
            turn_id,
            text: "late delta".to_owned(),
        },
    ) {
        Err(OperatorError::Conflict(message)) => {
            assert_eq!(
                message,
                "terminal conversation cannot append a turn observation"
            );
        }
        other => panic!("terminal conversation must reject a late turn event: {other:?}"),
    }
    assert_eq!(
        state
            .get_conversation_snapshot(conversation.conversation_id, completed.event.sequence)
            .expect("terminal event remains the final timeline event")
            .events,
        snapshot.events
    );
}

#[test]
fn completed_turn_rejects_late_acknowledgement_and_delta_without_changing_open_timeline() {
    let directory = TempDir::new().expect("temporary test directory is available");
    let database = directory.path().join("operator.sqlite");
    let project = project(&directory);
    let state = SqliteState::open(&database).expect("SQLite state opens");
    state
        .register_project(project.clone())
        .expect("project registers");
    let session_id = SessionId::new_exact(Uuid::new_v4());
    let turn_id = TurnId::new(Uuid::new_v4());
    let conversation = match state
        .persist_conversation_start(&start(&project, turn_id), session_id, "start")
        .expect("conversation start persists")
    {
        ConversationStartAdmission::Inserted { conversation, .. } => conversation,
        ConversationStartAdmission::Existing { .. }
        | ConversationStartAdmission::ExistingOperation { .. }
        | ConversationStartAdmission::MissingProject
        | ConversationStartAdmission::ActiveSession { .. } => {
            panic!("new conversation admission must insert")
        }
    };
    state
        .record_conversation_turn_observation(
            conversation.conversation_id,
            turn_id,
            Some(TurnState::Started),
            None,
            ConversationEventPayload::TurnStarted { turn_id },
        )
        .expect("turn starts");
    let completed = state
        .record_conversation_turn_observation(
            conversation.conversation_id,
            turn_id,
            Some(TurnState::Completed),
            Some("completed result".to_owned()),
            ConversationEventPayload::TurnCompleted {
                turn_id,
                result: "completed result".to_owned(),
            },
        )
        .expect("turn completes while the conversation remains open");
    let before = state
        .get_conversation_snapshot(conversation.conversation_id, 0)
        .expect("open conversation snapshot reads");
    assert_eq!(before.conversation.state, ConversationState::Open);
    assert!(matches!(
        state.record_conversation_turn_observation(
            conversation.conversation_id,
            turn_id,
            None,
            None,
            ConversationEventPayload::TurnAcknowledged { turn_id },
        ),
        Err(OperatorError::Conflict(_))
    ));
    assert!(matches!(
        state.record_conversation_turn_observation(
            conversation.conversation_id,
            turn_id,
            None,
            None,
            ConversationEventPayload::AssistantTextDelta {
                turn_id,
                text: "late delta".to_owned(),
            },
        ),
        Err(OperatorError::Conflict(_))
    ));
    let after = state
        .get_conversation_snapshot(conversation.conversation_id, 0)
        .expect("open conversation snapshot remains readable");
    assert_eq!(after.conversation.state, ConversationState::Open);
    assert_eq!(after.turns, before.turns);
    assert_eq!(after.events, before.events);
    assert_eq!(
        after
            .events
            .last()
            .expect("completed event remains present")
            .sequence,
        completed.event.sequence
    );
}

#[test]
fn request_id_occupation_returns_existing_facts_without_creating_a_second_effect() {
    let directory = TempDir::new().expect("temporary test directory is available");
    let database = directory.path().join("operator.sqlite");
    let project = project(&directory);
    let state = SqliteState::open(&database).expect("SQLite state opens");
    state
        .register_project(project.clone())
        .expect("project registers");

    let session_id = SessionId::new_exact(Uuid::new_v4());
    let original_turn_id = TurnId::new(Uuid::new_v4());
    let request = start(&project, original_turn_id);
    let original_operation = match state
        .persist_conversation_start(&request, session_id, "conversation fingerprint")
        .expect("conversation start persists")
    {
        ConversationStartAdmission::Inserted { operation, .. } => operation,
        ConversationStartAdmission::Existing { .. }
        | ConversationStartAdmission::ExistingOperation { .. }
        | ConversationStartAdmission::MissingProject
        | ConversationStartAdmission::ActiveSession { .. } => {
            panic!("first conversation start must insert")
        }
    };
    let changed_turn_id = TurnId::new(Uuid::new_v4());
    let changed_turn_request = ConversationStart {
        turn_id: changed_turn_id,
        ..request.clone()
    };
    match state
        .persist_conversation_start(
            &changed_turn_request,
            session_id,
            "conversation fingerprint",
        )
        .expect("occupied conversation request returns durable fact")
    {
        ConversationStartAdmission::Existing {
            operation,
            first_turn,
            fingerprint,
            ..
        } => {
            assert_eq!(operation.operation_id, original_operation.operation_id);
            assert_eq!(first_turn.turn_id, original_turn_id);
            assert_eq!(fingerprint, "conversation fingerprint");
        }
        ConversationStartAdmission::ExistingOperation { .. }
        | ConversationStartAdmission::MissingProject
        | ConversationStartAdmission::ActiveSession { .. }
        | ConversationStartAdmission::Inserted { .. } => {
            panic!("changed first turn must expose the existing conversation fact")
        }
    }

    let one_shot_request_id = RequestId::new(Uuid::new_v4());
    let one_shot = OperationStart {
        request_id: one_shot_request_id,
        project_id: project.project_id.clone(),
        intent: OperationIntent::New,
        prompt: "one-shot prompt".to_owned(),
        review_profile: ReviewProfile::OpusReadOnly,
    };
    let one_shot_operation = match state
        .persist_operation_admission(
            &one_shot,
            SessionId::new_exact(Uuid::new_v4()),
            "one-shot fingerprint",
        )
        .expect("one-shot operation persists")
    {
        OperationAdmission::Inserted(operation) => operation,
        OperationAdmission::Existing { .. }
        | OperationAdmission::MissingProject
        | OperationAdmission::ActiveSession { .. } => {
            panic!("one-shot operation must insert")
        }
    };
    let cross_kind = ConversationStart {
        request_id: one_shot_request_id,
        project_id: project.project_id.clone(),
        intent: OperationIntent::New,
        turn_id: TurnId::new(Uuid::new_v4()),
        prompt: "live prompt".to_owned(),
        review_profile: ReviewProfile::OpusReadOnly,
    };
    match state
        .persist_conversation_start(
            &cross_kind,
            SessionId::new_exact(Uuid::new_v4()),
            "live fingerprint",
        )
        .expect("cross-kind request occupation returns durable fact")
    {
        ConversationStartAdmission::ExistingOperation {
            operation,
            fingerprint,
        } => {
            assert_eq!(operation.operation_id, one_shot_operation.operation_id);
            assert_eq!(fingerprint, "one-shot fingerprint");
        }
        ConversationStartAdmission::Existing { .. }
        | ConversationStartAdmission::MissingProject
        | ConversationStartAdmission::ActiveSession { .. }
        | ConversationStartAdmission::Inserted { .. } => {
            panic!("one-shot request occupation must not create a conversation")
        }
    }
    match state.get_conversation(ConversationId::new(one_shot_operation.operation_id)) {
        Err(OperatorError::UnknownOperation(operation_id))
            if operation_id == one_shot_operation.operation_id.value().to_string() => {}
        other => panic!("cross-kind request reuse must not create a conversation: {other:?}"),
    }
}

#[test]
fn one_live_child_preserves_two_durable_turns_and_emits_text_before_results() {
    let directory = TempDir::new().expect("temporary test directory is available");
    let target = ClaudeTarget::default();
    let operation_id = TargetOperationId(Uuid::new_v4());
    let session_id = TargetSessionId(Uuid::new_v4());
    let first_turn_id = TargetTurnId(Uuid::new_v4());
    let second_turn_id = TargetTurnId(Uuid::new_v4());
    let (observations, receiver) = mpsc::channel();
    let (running_grant, running_permission) = mpsc::channel();
    target
        .start_live(
            TargetLiveStart {
                operation_id,
                working_directory: directory.path().to_path_buf(),
                executable: PathBuf::from(env!("CARGO_BIN_EXE_aiop-fake-claude")),
                expected_model: "opus".to_owned(),
                intent: TargetIntent::New,
                session_id,
                first_turn: TargetLiveTurn {
                    turn_id: first_turn_id,
                    position: 1,
                    prompt: "first live prompt".to_owned(),
                },
                running_permission,
            },
            observations,
        )
        .expect("live target starts one child");
    running_grant
        .send(Ok(()))
        .expect("Control grants durable Running before the first input write");
    target
        .stop_live(
            operation_id,
            TargetLiveStop::Graceful {
                through_position: 2,
            },
        )
        .expect("graceful close waits for its durably admitted position");
    target
        .send_live(
            operation_id,
            TargetLiveTurn {
                turn_id: second_turn_id,
                position: 2,
                prompt: "second live prompt".to_owned(),
            },
        )
        .expect("late target delivery still reaches the graceful durable bound");
    target
        .send_live(
            operation_id,
            TargetLiveTurn {
                turn_id: second_turn_id,
                position: 2,
                prompt: "second live prompt".to_owned(),
            },
        )
        .expect("equivalent duplicate turn must not become a second input write");
    let changed_turn = target.send_live(
        operation_id,
        TargetLiveTurn {
            turn_id: second_turn_id,
            position: 2,
            prompt: "changed second live prompt".to_owned(),
        },
    );
    match changed_turn {
        Err(error) if error.contains("different content") => {}
        other => panic!("changed caller turn UUID must conflict before input write: {other:?}"),
    }
    let mut observed = Vec::new();
    loop {
        let observation = receiver
            .recv_timeout(Duration::from_secs(5))
            .expect("live child produces a terminal observation");
        let ended = observation == TargetLiveObservation::Exited;
        observed.push(observation);
        if ended {
            break;
        }
    }
    assert_eq!(
        observed,
        vec![
            TargetLiveObservation::TurnQueued {
                turn_id: first_turn_id,
            },
            TargetLiveObservation::TurnStarted {
                turn_id: first_turn_id,
            },
            TargetLiveObservation::Initialized {
                session_id,
                model: "opus".to_owned(),
                version: Some("fixture-1".to_owned()),
            },
            TargetLiveObservation::TurnAcknowledged {
                turn_id: first_turn_id,
            },
            TargetLiveObservation::AssistantTextDelta {
                turn_id: first_turn_id,
                text: "live: first live prompt".to_owned(),
            },
            TargetLiveObservation::TurnCompleted {
                turn_id: first_turn_id,
                result: "live result: first live prompt".to_owned(),
            },
            TargetLiveObservation::TurnQueued {
                turn_id: second_turn_id,
            },
            TargetLiveObservation::TurnStarted {
                turn_id: second_turn_id,
            },
            TargetLiveObservation::Initialized {
                session_id,
                model: "opus".to_owned(),
                version: Some("fixture-1".to_owned()),
            },
            TargetLiveObservation::TurnAcknowledged {
                turn_id: second_turn_id,
            },
            TargetLiveObservation::AssistantTextDelta {
                turn_id: second_turn_id,
                text: "live: second live prompt".to_owned(),
            },
            TargetLiveObservation::TurnCompleted {
                turn_id: second_turn_id,
                result: "live result: second live prompt".to_owned(),
            },
            TargetLiveObservation::Exited,
        ]
    );
    let spawns = fs::read_to_string(directory.path().join(".aiop-fake-live-spawns.jsonl"))
        .expect("fake records one process launch");
    assert_eq!(spawns.lines().count(), 1);
    let input = fs::read_to_string(directory.path().join(".aiop-fake-invocations.jsonl"))
        .expect("fake records every delivered input");
    assert_eq!(input.lines().count(), 2);
}

#[test]
fn concurrent_turn_delivery_preserves_durable_position_input_and_completion_order() {
    let directory = TempDir::new().expect("temporary test directory is available");
    let state = Arc::new(
        SqliteState::open(&directory.path().join("operator.sqlite")).expect("SQLite state opens"),
    );
    let project = ProjectRegistration {
        project_id: ProjectId::new("v03-concurrent-order".to_owned()).expect("project id is valid"),
        working_directory: directory.path().to_path_buf(),
        claude_executable: PathBuf::from(env!("CARGO_BIN_EXE_aiop-fake-claude")),
        expected_opus_model: "opus".to_owned(),
    };
    state
        .register_project(project.clone())
        .expect("project registers");
    let first_id = TurnId::new(Uuid::new_v4());
    let state_port: Arc<dyn StatePort> = state.clone();
    let control = OperationControl::new(state_port, Arc::new(ClaudeTarget::default()));
    let started = match control
        .handle(aiop::contract::control::DaemonRequest::ConversationStart(
            start(&project, first_id),
        ))
        .expect("Control durably admits the first live turn")
    {
        aiop::contract::control::DaemonResponse::Conversation(snapshot) => snapshot,
        other => panic!("conversation start returned another payload: {other:?}"),
    };
    let conversation_id = started.conversation.conversation_id;
    let second_id = TurnId::new(Uuid::new_v4());
    let third_id = TurnId::new(Uuid::new_v4());
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let second_control = control.clone();
    let third_control = control.clone();
    let second_barrier = Arc::clone(&barrier);
    let third_barrier = Arc::clone(&barrier);
    let send_second = std::thread::spawn(move || {
        second_barrier.wait();
        second_control.handle(aiop::contract::control::DaemonRequest::ConversationSend(
            ConversationSend {
                conversation_id,
                turn_id: second_id,
                prompt: "second concurrent prompt".to_owned(),
            },
        ))
    });
    let send_third = std::thread::spawn(move || {
        third_barrier.wait();
        third_control.handle(aiop::contract::control::DaemonRequest::ConversationSend(
            ConversationSend {
                conversation_id,
                turn_id: third_id,
                prompt: "third concurrent prompt".to_owned(),
            },
        ))
    });
    barrier.wait();
    send_second
        .join()
        .expect("second sender does not panic")
        .expect("second Control request persists and reaches the live writer");
    send_third
        .join()
        .expect("third sender does not panic")
        .expect("third Control request persists and reaches the live writer");
    let snapshot = state
        .get_conversation_snapshot(conversation_id, 0)
        .expect("durable snapshot reads after concurrent admission");
    assert_eq!(
        snapshot
            .turns
            .iter()
            .map(|turn| turn.position)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    let expected: Vec<(Uuid, String)> = snapshot
        .turns
        .iter()
        .map(|turn| (turn.turn_id.value(), turn.prompt.clone()))
        .collect();
    control
        .handle(aiop::contract::control::DaemonRequest::ConversationStop {
            conversation_id,
            mode: aiop::contract::control::ConversationStopMode::Graceful,
        })
        .expect("graceful close drains every concurrently admitted durable turn");
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(5))
        .expect("terminal observation deadline is representable");
    let terminal = loop {
        let observed = state
            .get_conversation_snapshot(conversation_id, 0)
            .expect("terminal conversation snapshot reads");
        if observed.conversation.state.terminal() {
            break observed;
        }
        assert!(
            Instant::now() < deadline,
            "concurrent live turns did not reach a terminal conversation"
        );
        std::thread::yield_now();
    };
    assert_eq!(terminal.conversation.state, ConversationState::Succeeded);
    let input = fs::read_to_string(directory.path().join(".aiop-fake-invocations.jsonl"))
        .expect("fake records input");
    let input_ids: Vec<Uuid> = input
        .lines()
        .map(|line| {
            serde_json::from_str::<serde_json::Value>(line)
                .expect("record JSON")
                .get("prompt")
                .and_then(serde_json::Value::as_str)
                .expect("prompt in record")
                .to_owned()
        })
        .map(|prompt| {
            match expected
                .iter()
                .find(|(_, expected_prompt)| *expected_prompt == prompt)
            {
                Some((turn_id, _)) => *turn_id,
                None => panic!("fake received a prompt absent from durable order: {prompt}"),
            }
        })
        .collect();
    let completed: Vec<Uuid> = terminal
        .events
        .iter()
        .filter_map(|event| match &event.payload {
            ConversationEventPayload::TurnCompleted { turn_id, .. } => Some(turn_id.value()),
            ConversationEventPayload::Initialized { .. }
            | ConversationEventPayload::TurnQueued { .. }
            | ConversationEventPayload::TurnAcknowledged { .. }
            | ConversationEventPayload::TurnStarted { .. }
            | ConversationEventPayload::AssistantTextDelta { .. }
            | ConversationEventPayload::TurnFailed { .. }
            | ConversationEventPayload::TurnIndeterminate { .. }
            | ConversationEventPayload::TurnCancelled { .. }
            | ConversationEventPayload::TurnDiscarded { .. }
            | ConversationEventPayload::ConversationTerminal { .. } => None,
        })
        .collect();
    let expected_ids: Vec<Uuid> = expected.iter().map(|(turn_id, _)| *turn_id).collect();
    assert_eq!(input_ids, expected_ids);
    assert_eq!(completed, expected_ids);
}

#[test]
fn live_cancel_wins_while_the_first_turn_waits_for_provider_progress() {
    let directory = TempDir::new().expect("temporary test directory is available");
    let target = ClaudeTarget::default();
    let operation_id = TargetOperationId(Uuid::new_v4());
    let session_id = TargetSessionId(Uuid::new_v4());
    let turn_id = TargetTurnId(Uuid::new_v4());
    let (observations, receiver) = mpsc::channel();
    let (running_grant, running_permission) = mpsc::channel();
    target
        .start_live(
            TargetLiveStart {
                operation_id,
                working_directory: directory.path().to_path_buf(),
                executable: PathBuf::from(env!("CARGO_BIN_EXE_aiop-fake-claude")),
                expected_model: "opus".to_owned(),
                intent: TargetIntent::New,
                session_id,
                first_turn: TargetLiveTurn {
                    turn_id,
                    position: 1,
                    prompt: "__fixture_live_hold__".to_owned(),
                },
                running_permission,
            },
            observations,
        )
        .expect("live target starts before durable Running permission");
    let spawn_record = directory.path().join(".aiop-fake-live-spawns.jsonl");
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(5))
        .expect("fixture readiness deadline is representable");
    while !spawn_record.exists() {
        assert!(
            Instant::now() < deadline,
            "fake live child did not reach its stdin loop before the readiness deadline"
        );
        std::thread::yield_now();
    }
    assert!(
        !directory
            .path()
            .join(".aiop-fake-invocations.jsonl")
            .exists()
    );
    assert!(receiver.try_recv().is_err());
    running_grant
        .send(Ok(()))
        .expect("Control grants durable Running once it persisted the transition");
    target
        .stop_live(operation_id, TargetLiveStop::Cancel)
        .expect("cancellation terminates the direct child");
    let terminal = receiver
        .recv_timeout(Duration::from_secs(5))
        .expect("cancelled direct child reports a terminal observation");
    assert_eq!(terminal, TargetLiveObservation::Cancelled);
}

#[test]
fn live_provider_failure_retains_the_causal_turn_identity() {
    let directory = TempDir::new().expect("temporary test directory is available");
    let target = ClaudeTarget::default();
    let operation_id = TargetOperationId(Uuid::new_v4());
    let session_id = TargetSessionId(Uuid::new_v4());
    let turn_id = TargetTurnId(Uuid::new_v4());
    let (observations, receiver) = mpsc::channel();
    let (running_grant, running_permission) = mpsc::channel();
    target
        .start_live(
            TargetLiveStart {
                operation_id,
                working_directory: directory.path().to_path_buf(),
                executable: PathBuf::from(env!("CARGO_BIN_EXE_aiop-fake-claude")),
                expected_model: "opus".to_owned(),
                intent: TargetIntent::New,
                session_id,
                first_turn: TargetLiveTurn {
                    turn_id,
                    position: 1,
                    prompt: "__fixture_live_provider_failure__".to_owned(),
                },
                running_permission,
            },
            observations,
        )
        .expect("live target starts");
    running_grant
        .send(Ok(()))
        .expect("Control grants durable Running");
    let mut observed = Vec::new();
    loop {
        let observation = receiver
            .recv_timeout(Duration::from_secs(5))
            .expect("provider failure is observed");
        let failed = matches!(observation, TargetLiveObservation::Failed(_));
        observed.push(observation);
        if failed {
            break;
        }
    }
    assert!(observed.contains(&TargetLiveObservation::TurnFailed {
        turn_id,
        message: "fixture live provider failure".to_owned(),
    }));
    assert!(observed.contains(&TargetLiveObservation::Failed(
        "fixture live provider failure".to_owned(),
    )));
}

#[test]
fn control_persists_one_live_conversation_across_two_turns_and_graceful_close() {
    let directory = TempDir::new().expect("temporary test directory is available");
    let state = Arc::new(
        SqliteState::open(&directory.path().join("operator.sqlite")).expect("SQLite state opens"),
    );
    let project = ProjectRegistration {
        project_id: ProjectId::new("v03-control-project".to_owned())
            .expect("fixture project id is valid"),
        working_directory: directory.path().to_path_buf(),
        claude_executable: PathBuf::from(env!("CARGO_BIN_EXE_aiop-fake-claude")),
        expected_opus_model: "opus".to_owned(),
    };
    state
        .register_project(project.clone())
        .expect("project registers");
    let control = OperationControl::new(state, Arc::new(ClaudeTarget::default()));
    let first_turn_id = TurnId::new(Uuid::new_v4());
    let started = match control
        .handle(aiop::contract::control::DaemonRequest::ConversationStart(
            ConversationStart {
                request_id: RequestId::new(Uuid::new_v4()),
                project_id: project.project_id.clone(),
                intent: OperationIntent::New,
                turn_id: first_turn_id,
                prompt: "first durable live prompt".to_owned(),
                review_profile: ReviewProfile::OpusReadOnly,
            },
        ))
        .expect("conversation starts through Operation Control")
    {
        aiop::contract::control::DaemonResponse::Conversation(snapshot) => snapshot,
        other => panic!("conversation start returned another payload: {other:?}"),
    };
    let conversation_id = started.conversation.conversation_id;
    let second_turn_id = TurnId::new(Uuid::new_v4());
    match control
        .handle(aiop::contract::control::DaemonRequest::ConversationSend(
            ConversationSend {
                conversation_id,
                turn_id: second_turn_id,
                prompt: "second durable live prompt".to_owned(),
            },
        ))
        .expect("second turn persists before delivery")
    {
        aiop::contract::control::DaemonResponse::Conversation(snapshot) => {
            assert_eq!(snapshot.turns.len(), 2);
            assert_eq!(snapshot.turns[1].position, 2);
        }
        other => panic!("conversation send returned another payload: {other:?}"),
    }
    control
        .handle(aiop::contract::control::DaemonRequest::ConversationStop {
            conversation_id,
            mode: aiop::contract::control::ConversationStopMode::Graceful,
        })
        .expect("immediate graceful close preserves admitted turns");
    let repeated_graceful = match control
        .handle(aiop::contract::control::DaemonRequest::ConversationStop {
            conversation_id,
            mode: aiop::contract::control::ConversationStopMode::Graceful,
        })
        .expect("repeated graceful close is idempotent")
    {
        aiop::contract::control::DaemonResponse::Conversation(snapshot) => snapshot,
        other => panic!("repeated graceful close returned another payload: {other:?}"),
    };
    assert_eq!(
        repeated_graceful.conversation.conversation_id,
        conversation_id
    );
    let mut cursor = 0;
    let mut observed_events = Vec::new();
    let terminal = loop {
        let snapshot = match control
            .handle(aiop::contract::control::DaemonRequest::ConversationWait(
                aiop::contract::control::ConversationWait {
                    conversation_id,
                    after_sequence: cursor,
                    wait_millis: 5_000,
                },
            ))
            .expect("conversation progress is observable")
        {
            aiop::contract::control::DaemonResponse::Conversation(snapshot) => snapshot,
            other => panic!("conversation wait returned another payload: {other:?}"),
        };
        observed_events.extend(snapshot.events.clone());
        if snapshot.conversation.state.terminal() {
            break snapshot;
        }
        cursor = snapshot
            .events
            .last()
            .expect("nonterminal wait returned an event")
            .sequence;
    };
    assert_eq!(
        terminal.conversation.state,
        ConversationState::Succeeded,
        "terminal snapshot: {terminal:?}"
    );
    assert_eq!(terminal.turns.len(), 2);
    assert_eq!(terminal.turns[0].state, TurnState::Completed);
    assert_eq!(terminal.turns[1].state, TurnState::Completed);
    assert!(observed_events.iter().any(|event| matches!(
        event.payload,
        ConversationEventPayload::AssistantTextDelta { turn_id, .. } if turn_id == first_turn_id
    )));
    assert!(observed_events.iter().any(|event| matches!(
        event.payload,
        ConversationEventPayload::TurnCompleted { turn_id, .. } if turn_id == second_turn_id
    )));
}

#[test]
fn control_cancel_records_started_turn_cancellation_before_terminal_conversation() {
    let directory = TempDir::new().expect("temporary test directory is available");
    let state = Arc::new(
        SqliteState::open(&directory.path().join("operator.sqlite")).expect("SQLite state opens"),
    );
    let project = ProjectRegistration {
        project_id: ProjectId::new("v03-cancel-project".to_owned())
            .expect("fixture project id is valid"),
        working_directory: directory.path().to_path_buf(),
        claude_executable: PathBuf::from(env!("CARGO_BIN_EXE_aiop-fake-claude")),
        expected_opus_model: "opus".to_owned(),
    };
    state
        .register_project(project.clone())
        .expect("project registers");
    let control = OperationControl::new(state, Arc::new(ClaudeTarget::default()));
    let turn_id = TurnId::new(Uuid::new_v4());
    let started = match control
        .handle(aiop::contract::control::DaemonRequest::ConversationStart(
            ConversationStart {
                request_id: RequestId::new(Uuid::new_v4()),
                project_id: project.project_id,
                intent: OperationIntent::New,
                turn_id,
                prompt: "__fixture_live_hold_after_start__".to_owned(),
                review_profile: ReviewProfile::OpusReadOnly,
            },
        ))
        .expect("conversation starts")
    {
        aiop::contract::control::DaemonResponse::Conversation(snapshot) => snapshot,
        other => panic!("conversation start returned another payload: {other:?}"),
    };
    let conversation_id = started.conversation.conversation_id;
    let mut cursor = 0;
    let mut observed_events = Vec::new();
    loop {
        let snapshot = match control
            .handle(aiop::contract::control::DaemonRequest::ConversationWait(
                aiop::contract::control::ConversationWait {
                    conversation_id,
                    after_sequence: cursor,
                    wait_millis: 5_000,
                },
            ))
            .expect("started turn becomes observable")
        {
            aiop::contract::control::DaemonResponse::Conversation(snapshot) => snapshot,
            other => panic!("conversation wait returned another payload: {other:?}"),
        };
        observed_events.extend(snapshot.events.clone());
        if observed_events.iter().any(|event| {
            matches!(
                event.payload,
                ConversationEventPayload::TurnStarted { turn_id: observed } if observed == turn_id
            )
        }) {
            break;
        }
        cursor = snapshot
            .events
            .last()
            .expect("nonterminal wait returned an event")
            .sequence;
    }
    control
        .handle(aiop::contract::control::DaemonRequest::ConversationStop {
            conversation_id,
            mode: aiop::contract::control::ConversationStopMode::Graceful,
        })
        .expect("first graceful close preserves the started turn while rejecting new admission");
    control
        .handle(aiop::contract::control::DaemonRequest::ConversationStop {
            conversation_id,
            mode: aiop::contract::control::ConversationStopMode::Cancel,
        })
        .expect("cancel escalates the durable graceful close and terminates the direct child");
    let repeated_cancel = match control
        .handle(aiop::contract::control::DaemonRequest::ConversationStop {
            conversation_id,
            mode: aiop::contract::control::ConversationStopMode::Cancel,
        })
        .expect("repeated cancellation is idempotent")
    {
        aiop::contract::control::DaemonResponse::Conversation(snapshot) => snapshot,
        other => panic!("repeated cancellation returned another payload: {other:?}"),
    };
    assert_eq!(
        repeated_cancel.conversation.conversation_id,
        conversation_id
    );
    let terminal = loop {
        let snapshot = match control
            .handle(aiop::contract::control::DaemonRequest::ConversationWait(
                aiop::contract::control::ConversationWait {
                    conversation_id,
                    after_sequence: cursor,
                    wait_millis: 5_000,
                },
            ))
            .expect("cancel terminalization is observable")
        {
            aiop::contract::control::DaemonResponse::Conversation(snapshot) => snapshot,
            other => panic!("conversation wait returned another payload: {other:?}"),
        };
        observed_events.extend(snapshot.events.clone());
        if snapshot.conversation.state.terminal() {
            break snapshot;
        }
        cursor = snapshot
            .events
            .last()
            .expect("nonterminal wait returned an event")
            .sequence;
    };
    assert_eq!(terminal.conversation.state, ConversationState::Cancelled);
    assert_eq!(terminal.turns[0].state, TurnState::Cancelled);
    let started_sequence = event_sequence(&observed_events, |payload| {
        matches!(
            payload,
            ConversationEventPayload::TurnStarted { turn_id: observed } if *observed == turn_id
        )
    });
    let cancelled_sequence = event_sequence(&observed_events, |payload| {
        matches!(
            payload,
            ConversationEventPayload::TurnCancelled {
                turn_id: observed,
                message: _,
            } if *observed == turn_id
        )
    });
    let terminal_sequence = event_sequence(&observed_events, |payload| {
        matches!(
            payload,
            ConversationEventPayload::ConversationTerminal { .. }
        )
    });
    assert!(started_sequence < cancelled_sequence);
    assert!(cancelled_sequence < terminal_sequence);
}

#[test]
fn generic_operation_cancel_terminates_the_live_conversation_operation() {
    let directory = TempDir::new().expect("temporary test directory is available");
    let state = Arc::new(
        SqliteState::open(&directory.path().join("operator.sqlite")).expect("SQLite state opens"),
    );
    let project = ProjectRegistration {
        project_id: ProjectId::new("v03-generic-cancel-project".to_owned())
            .expect("fixture project id is valid"),
        working_directory: directory.path().to_path_buf(),
        claude_executable: PathBuf::from(env!("CARGO_BIN_EXE_aiop-fake-claude")),
        expected_opus_model: "opus".to_owned(),
    };
    state
        .register_project(project.clone())
        .expect("project registers");
    let state_port: Arc<dyn StatePort> = state.clone();
    let control = OperationControl::new(state_port, Arc::new(ClaudeTarget::default()));
    let started = match control
        .handle(aiop::contract::control::DaemonRequest::ConversationStart(
            ConversationStart {
                request_id: RequestId::new(Uuid::new_v4()),
                project_id: project.project_id,
                intent: OperationIntent::New,
                turn_id: TurnId::new(Uuid::new_v4()),
                prompt: "__fixture_live_hold_after_start__".to_owned(),
                review_profile: ReviewProfile::OpusReadOnly,
            },
        ))
        .expect("live conversation starts through the operation owner")
    {
        aiop::contract::control::DaemonResponse::Conversation(snapshot) => snapshot,
        other => panic!("conversation start returned another payload: {other:?}"),
    };
    let operation_id = started.conversation.conversation_id.operation_id();
    let cancelled = match control
        .handle(aiop::contract::control::DaemonRequest::OperationCancel { operation_id })
        .expect("generic operation cancellation reaches the live lifecycle")
    {
        aiop::contract::control::DaemonResponse::Operation(operation) => operation,
        other => panic!("generic operation cancellation returned another payload: {other:?}"),
    };
    assert_eq!(cancelled.operation_id, operation_id);
    assert_eq!(cancelled.state, OperationState::Cancelled);
    assert!(matches!(
        cancelled.terminal_outcome,
        Some(TerminalOutcome::Cancelled(_))
    ));
    let snapshot = state
        .get_conversation_snapshot(started.conversation.conversation_id, 0)
        .expect("authoritative live snapshot reads after generic cancellation");
    assert_eq!(snapshot.conversation.state, ConversationState::Cancelled);
    assert!(snapshot.conversation.terminal_outcome.is_some());
}

#[test]
fn mcp_reconnects_to_the_same_live_conversation_and_gracefully_finishes() {
    let directory = TempDir::new().expect("temporary test directory is available");
    let database = directory.path().join("operator.sqlite");
    let socket = directory.path().join("operator.sock");
    let mut daemon = start_daemon(&database, &socket);
    let project_id = "v03-mcp-project";
    let executable = env!("CARGO_BIN_EXE_aiop-fake-claude");
    let request_id = Uuid::new_v4();
    let first_turn_id = Uuid::new_v4();
    let conversation_id;
    let first_result;
    let saved_cursor;
    let mut observed_events = Vec::new();
    {
        let mut mcp = start_mcp(&socket);
        let mut input = mcp.take_stdin();
        let mut output = BufReader::new(mcp.take_stdout());
        initialize_mcp(&mut input, &mut output);
        let registration = call_tool(
            &mut input,
            &mut output,
            2,
            "project_register",
            serde_json::json!({
                "project_id": project_id,
                "working_directory": directory.path(),
                "claude_executable": executable,
                "expected_opus_model": "opus"
            }),
        );
        assert_eq!(registration["result"]["isError"], false);
        let started = call_tool(
            &mut input,
            &mut output,
            3,
            "conversation_start",
            serde_json::json!({
                "request_id": request_id,
                "project_id": project_id,
                "intent": {"kind": "new"},
                "turn_id": first_turn_id,
                "prompt": "first reconnectable live prompt",
                "review_profile": "opus_read_only"
            }),
        );
        assert_eq!(started["result"]["isError"], false);
        conversation_id = conversation_from_mcp(started)
            .conversation
            .conversation_id
            .operation_id()
            .value();
        let invalid_cursor = call_tool(
            &mut input,
            &mut output,
            4,
            "conversation_wait",
            serde_json::json!({
                "conversation_id": conversation_id,
                "after_sequence": 9_223_372_036_854_775_808_u64,
                "wait_millis": 0
            }),
        );
        assert_eq!(invalid_cursor["result"]["isError"], true);
        let invalid_text = invalid_cursor["result"]["content"][0]["text"]
            .as_str()
            .expect("MCP invalid cursor error contains JSON");
        let invalid_error: serde_json::Value =
            serde_json::from_str(invalid_text).expect("MCP invalid cursor error is JSON");
        assert_eq!(invalid_error["kind"], "operator");
        assert_eq!(invalid_error["data"]["error"]["kind"], "invalid_request");
        let mut cursor = 0;
        loop {
            let waited = call_tool(
                &mut input,
                &mut output,
                5,
                "conversation_wait",
                serde_json::json!({
                    "conversation_id": conversation_id,
                    "after_sequence": cursor,
                    "wait_millis": 5000
                }),
            );
            assert_eq!(waited["result"]["isError"], false);
            let snapshot = conversation_from_mcp(waited);
            observed_events.extend(snapshot.events.clone());
            let completed = snapshot
                .events
                .iter()
                .find_map(|event| match &event.payload {
                    ConversationEventPayload::TurnCompleted { turn_id, result }
                        if *turn_id == TurnId::new(first_turn_id) =>
                    {
                        Some(result.clone())
                    }
                    ConversationEventPayload::Initialized { .. }
                    | ConversationEventPayload::TurnQueued { .. }
                    | ConversationEventPayload::TurnAcknowledged { .. }
                    | ConversationEventPayload::TurnStarted { .. }
                    | ConversationEventPayload::AssistantTextDelta { .. }
                    | ConversationEventPayload::TurnCompleted { .. }
                    | ConversationEventPayload::TurnFailed { .. }
                    | ConversationEventPayload::TurnIndeterminate { .. }
                    | ConversationEventPayload::TurnCancelled { .. }
                    | ConversationEventPayload::TurnDiscarded { .. }
                    | ConversationEventPayload::ConversationTerminal { .. } => None,
                });
            match (completed, snapshot.events.last()) {
                (Some(result), Some(event)) => {
                    first_result = result;
                    saved_cursor = event.sequence;
                    break;
                }
                (Some(_), None) => panic!("completed first turn must have a durable event"),
                (None, Some(event)) => cursor = event.sequence,
                (None, None) => {}
            }
        }
    }
    let mut mcp = start_mcp(&socket);
    let mut input = mcp.take_stdin();
    let mut output = BufReader::new(mcp.take_stdout());
    initialize_mcp(&mut input, &mut output);
    let second_turn_id = Uuid::new_v4();
    let sent = call_tool(
        &mut input,
        &mut output,
        5,
        "conversation_send",
        serde_json::json!({
            "conversation_id": conversation_id,
            "turn_id": second_turn_id,
            "prompt": format!("second reconnectable live prompt after {first_result}")
        }),
    );
    assert_eq!(sent["result"]["isError"], false);
    let mut cursor = saved_cursor;
    loop {
        let waited = call_tool(
            &mut input,
            &mut output,
            6,
            "conversation_wait",
            serde_json::json!({
                "conversation_id": conversation_id,
                "after_sequence": cursor,
                "wait_millis": 5000
            }),
        );
        assert_eq!(waited["result"]["isError"], false);
        let snapshot = conversation_from_mcp(waited);
        observed_events.extend(snapshot.events.clone());
        if snapshot.events.iter().any(|event| {
            matches!(
                event.payload,
                ConversationEventPayload::TurnCompleted { turn_id, .. }
                    if turn_id == TurnId::new(second_turn_id)
            )
        }) {
            cursor = snapshot
                .events
                .last()
                .expect("completed second turn has a durable event")
                .sequence;
            break;
        }
        cursor = snapshot
            .events
            .last()
            .expect("nonterminal wait returns a durable event")
            .sequence;
    }
    let stopped = call_tool(
        &mut input,
        &mut output,
        7,
        "conversation_stop",
        serde_json::json!({"conversation_id": conversation_id, "mode": "graceful"}),
    );
    assert_eq!(stopped["result"]["isError"], false);
    let terminal = loop {
        let waited = call_tool(
            &mut input,
            &mut output,
            8,
            "conversation_wait",
            serde_json::json!({
                "conversation_id": conversation_id,
                "after_sequence": cursor,
                "wait_millis": 5000
            }),
        );
        assert_eq!(waited["result"]["isError"], false);
        let snapshot = conversation_from_mcp(waited);
        observed_events.extend(snapshot.events.clone());
        if snapshot.conversation.state.terminal() {
            break snapshot;
        }
        cursor = snapshot
            .events
            .last()
            .expect("nonterminal wait returns a durable event")
            .sequence;
    };
    assert_eq!(terminal.conversation.state, ConversationState::Succeeded);
    assert_eq!(terminal.turns.len(), 2);
    assert_eq!(terminal.turns[0].turn_id, TurnId::new(first_turn_id));
    assert_eq!(terminal.turns[1].turn_id, TurnId::new(second_turn_id));
    assert_turn_event_order(&observed_events, TurnId::new(first_turn_id));
    assert_turn_event_order(&observed_events, TurnId::new(second_turn_id));
    let first_completed = event_sequence(&observed_events, |payload| {
        matches!(
            payload,
            ConversationEventPayload::TurnCompleted { turn_id, .. } if *turn_id == TurnId::new(first_turn_id)
        )
    });
    let second_started = event_sequence(&observed_events, |payload| {
        matches!(
            payload,
            ConversationEventPayload::TurnStarted { turn_id } if *turn_id == TurnId::new(second_turn_id)
        )
    });
    assert!(first_completed < second_started);
    let spawns = fs::read_to_string(directory.path().join(".aiop-fake-live-spawns.jsonl"))
        .expect("fake records live child spawn");
    assert_eq!(spawns.lines().count(), 1);
    daemon.terminate();
}

fn assert_turn_event_order(events: &[aiop::contract::control::ConversationEvent], turn_id: TurnId) {
    let delta = event_sequence(events, |payload| {
        matches!(
            payload,
            ConversationEventPayload::AssistantTextDelta { turn_id: observed, .. } if *observed == turn_id
        )
    });
    let completed = event_sequence(events, |payload| {
        matches!(
            payload,
            ConversationEventPayload::TurnCompleted { turn_id: observed, .. } if *observed == turn_id
        )
    });
    assert!(delta < completed);
}

fn event_sequence(
    events: &[aiop::contract::control::ConversationEvent],
    predicate: impl Fn(&ConversationEventPayload) -> bool,
) -> u64 {
    events
        .iter()
        .find(|event| predicate(&event.payload))
        .expect("required durable conversation event exists")
        .sequence
}

fn conversation_from_mcp(
    response: serde_json::Value,
) -> aiop::contract::control::ConversationSnapshot {
    let text = response["result"]["content"][0]["text"]
        .as_str()
        .expect("MCP conversation tool returns daemon JSON");
    match serde_json::from_str::<aiop::contract::control::DaemonResponse>(text)
        .expect("daemon response JSON")
    {
        aiop::contract::control::DaemonResponse::Conversation(snapshot) => snapshot,
        other => panic!("conversation tool returned another payload: {other:?}"),
    }
}
