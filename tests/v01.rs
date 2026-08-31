// Copyright 2026 Yuriy Krasilnikov
// SPDX-License-Identifier: Apache-2.0

mod support;

use std::{
    fs,
    io::{BufRead, BufReader, Write},
    os::unix::net::UnixListener,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
};

use support::{call_tool, initialize_mcp, receive_mcp, send_mcp, start_daemon, start_mcp};

use aiop::{
    contract::control::{
        DaemonRequest, DaemonResponse, Operation, OperationAdmission, OperationIntent,
        OperationStart, OperationState, ProjectId, ProjectRegistration, RequestId, ReviewProfile,
        StatePort, TerminalOutcome,
    },
    contract::target::{TargetCommand, TargetOperationId, TargetOutcome, TargetPort},
    control::OperationControl,
    gateway::client,
    state::SqliteState,
    target::ClaudeTarget,
};
use serde::Deserialize;
use tempfile::TempDir;
use uuid::Uuid;

fn passive_fixture() -> (TempDir, ProjectRegistration) {
    let directory = TempDir::new().expect("temporary test directory is available");
    let project = ProjectRegistration {
        project_id: ProjectId::new("fixture-project".to_owned())
            .expect("fixture project id is valid"),
        working_directory: directory.path().to_path_buf(),
        claude_executable: Path::new(env!("CARGO_BIN_EXE_aiop-fake-claude")).to_path_buf(),
        expected_opus_model: "opus".to_owned(),
    };
    (directory, project)
}

fn fixture() -> (TempDir, OperationControl, ProjectRegistration) {
    let (directory, project) = passive_fixture();
    let database = directory.path().join("operator.sqlite");
    let state = Arc::new(SqliteState::open(&database).expect("SQLite state opens"));
    let target = Arc::new(ClaudeTarget::default());
    let control = OperationControl::new(state, target);
    (directory, control, project)
}

struct TerminalFailingState {
    inner: SqliteState,
    fail_once: AtomicBool,
}

impl StatePort for TerminalFailingState {
    fn register_project(
        &self,
        project: ProjectRegistration,
    ) -> Result<ProjectRegistration, aiop::contract::control::OperatorError> {
        self.inner.register_project(project)
    }
    fn get_project(
        &self,
        project_id: &ProjectId,
    ) -> Result<ProjectRegistration, aiop::contract::control::OperatorError> {
        self.inner.get_project(project_id)
    }
    fn list_projects(
        &self,
    ) -> Result<Vec<ProjectRegistration>, aiop::contract::control::OperatorError> {
        self.inner.list_projects()
    }
    fn persist_operation_admission(
        &self,
        request: &OperationStart,
        session: aiop::contract::control::SessionId,
        fingerprint: &str,
    ) -> Result<OperationAdmission, aiop::contract::control::OperatorError> {
        self.inner
            .persist_operation_admission(request, session, fingerprint)
    }
    fn get_operation(
        &self,
        operation: aiop::contract::control::OperationId,
    ) -> Result<Operation, aiop::contract::control::OperatorError> {
        self.inner.get_operation(operation)
    }
    fn transition(
        &self,
        operation: aiop::contract::control::OperationId,
        next: OperationState,
        terminal: Option<TerminalOutcome>,
        session: Option<aiop::contract::control::SessionId>,
        model: Option<String>,
        version: Option<String>,
    ) -> Result<Operation, aiop::contract::control::OperatorError> {
        if next.terminal() && self.fail_once.swap(false, Ordering::SeqCst) {
            return Err(aiop::contract::control::OperatorError::State(
                "injected terminal transition failure".to_owned(),
            ));
        }
        self.inner
            .transition(operation, next, terminal, session, model, version)
    }
    fn recover_current_daemon_incomplete(
        &self,
    ) -> Result<(), aiop::contract::control::OperatorError> {
        self.inner.recover_current_daemon_incomplete()
    }
    fn persist_conversation_start(
        &self,
        request: &aiop::contract::control::ConversationStart,
        session: aiop::contract::control::SessionId,
        fingerprint: &str,
    ) -> Result<
        aiop::contract::control::ConversationStartAdmission,
        aiop::contract::control::OperatorError,
    > {
        self.inner
            .persist_conversation_start(request, session, fingerprint)
    }
    fn get_conversation(
        &self,
        conversation_id: aiop::contract::control::ConversationId,
    ) -> Result<aiop::contract::control::Conversation, aiop::contract::control::OperatorError> {
        self.inner.get_conversation(conversation_id)
    }
    fn get_conversation_snapshot(
        &self,
        conversation_id: aiop::contract::control::ConversationId,
        after_sequence: u64,
    ) -> Result<aiop::contract::control::ConversationSnapshot, aiop::contract::control::OperatorError>
    {
        self.inner
            .get_conversation_snapshot(conversation_id, after_sequence)
    }
    fn persist_conversation_turn(
        &self,
        request: &aiop::contract::control::ConversationSend,
        fingerprint: &str,
    ) -> Result<
        aiop::contract::control::ConversationTurnAdmission,
        aiop::contract::control::OperatorError,
    > {
        self.inner.persist_conversation_turn(request, fingerprint)
    }
    fn record_conversation_turn_observation(
        &self,
        conversation_id: aiop::contract::control::ConversationId,
        turn_id: aiop::contract::control::TurnId,
        state: Option<aiop::contract::control::TurnState>,
        result: Option<String>,
        payload: aiop::contract::control::ConversationEventPayload,
    ) -> Result<
        aiop::contract::control::ConversationTurnObservation,
        aiop::contract::control::OperatorError,
    > {
        self.inner.record_conversation_turn_observation(
            conversation_id,
            turn_id,
            state,
            result,
            payload,
        )
    }
    fn record_conversation_initialization(
        &self,
        conversation_id: aiop::contract::control::ConversationId,
        session_id: aiop::contract::control::SessionId,
        model: String,
        claude_version: Option<String>,
    ) -> Result<aiop::contract::control::ConversationEvent, aiop::contract::control::OperatorError>
    {
        self.inner.record_conversation_initialization(
            conversation_id,
            session_id,
            model,
            claude_version,
        )
    }
    fn close_conversation(
        &self,
        conversation_id: aiop::contract::control::ConversationId,
        mode: aiop::contract::control::ConversationStopMode,
    ) -> Result<
        aiop::contract::control::ConversationCloseAdmission,
        aiop::contract::control::OperatorError,
    > {
        self.inner.close_conversation(conversation_id, mode)
    }
    fn terminalize_conversation(
        &self,
        conversation_id: aiop::contract::control::ConversationId,
        conversation_state: aiop::contract::control::ConversationState,
        operation_state: OperationState,
        terminal: TerminalOutcome,
        claim_disposition: aiop::contract::control::SessionClaimDisposition,
    ) -> Result<aiop::contract::control::Conversation, aiop::contract::control::OperatorError> {
        self.inner.terminalize_conversation(
            conversation_id,
            conversation_state,
            operation_state,
            terminal,
            claim_disposition,
        )
    }
    fn list_session_evidence(
        &self,
        project_id: &ProjectId,
    ) -> Result<Vec<aiop::contract::control::SessionEvidence>, aiop::contract::control::OperatorError>
    {
        self.inner.list_session_evidence(project_id)
    }
    fn inspect_session_evidence(
        &self,
        project_id: &ProjectId,
        target_session_id: aiop::contract::control::SessionId,
    ) -> Result<Vec<aiop::contract::control::SessionEvidence>, aiop::contract::control::OperatorError>
    {
        self.inner
            .inspect_session_evidence(project_id, target_session_id)
    }
    fn persist_initiator_binding(
        &self,
        binding: &aiop::contract::control::InitiatorBinding,
    ) -> Result<aiop::contract::control::BindingPersistence, aiop::contract::control::OperatorError>
    {
        self.inner.persist_initiator_binding(binding)
    }
    fn get_initiator_binding(
        &self,
        project_id: &ProjectId,
        identity: &aiop::contract::control::InitiatorIdentity,
    ) -> Result<
        Option<aiop::contract::control::InitiatorBinding>,
        aiop::contract::control::OperatorError,
    > {
        self.inner.get_initiator_binding(project_id, identity)
    }
    fn list_initiator_bindings_for_initiator(
        &self,
        project_id: &ProjectId,
        initiator_session_id: &aiop::contract::control::InitiatorSessionIdentity,
        initiator_agent_id: &aiop::contract::control::InitiatorAgentIdentity,
    ) -> Result<
        Vec<aiop::contract::control::InitiatorBinding>,
        aiop::contract::control::OperatorError,
    > {
        self.inner.list_initiator_bindings_for_initiator(
            project_id,
            initiator_session_id,
            initiator_agent_id,
        )
    }
}

#[test]
fn terminal_state_failure_refuses_the_current_daemon() {
    let directory = TempDir::new().expect("temporary test directory is available");
    let state = Arc::new(TerminalFailingState {
        inner: SqliteState::open(&directory.path().join("operator.sqlite"))
            .expect("SQLite state opens"),
        fail_once: AtomicBool::new(true),
    });
    let control = OperationControl::new(state, Arc::new(ClaudeTarget::default()));
    let project = ProjectRegistration {
        project_id: ProjectId::new("state-failure-project".to_owned())
            .expect("project id is valid"),
        working_directory: directory.path().to_path_buf(),
        claude_executable: Path::new(env!("CARGO_BIN_EXE_aiop-fake-claude")).to_path_buf(),
        expected_opus_model: "opus".to_owned(),
    };
    control
        .handle(DaemonRequest::ProjectRegister(project.clone()))
        .expect("project registers");
    let accepted = operation(
        control
            .handle(DaemonRequest::OperationStart(start(
                &project,
                "state failure",
                OperationIntent::New,
            )))
            .expect("operation accepts"),
    );
    let refusal = control.handle(DaemonRequest::OperationWait {
        operation_id: accepted.operation_id,
        wait_millis: 5_000,
    });
    match refusal {
        Err(aiop::contract::control::OperatorError::State(message))
            if message == "injected terminal transition failure" => {}
        other => panic!("daemon must preserve injected terminal transition error: {other:?}"),
    }
}

fn start(project: &ProjectRegistration, prompt: &str, intent: OperationIntent) -> OperationStart {
    OperationStart {
        request_id: RequestId::new(Uuid::new_v4()),
        project_id: project.project_id.clone(),
        intent,
        prompt: prompt.to_owned(),
        review_profile: ReviewProfile::OpusReadOnly,
    }
}

fn operation(response: DaemonResponse) -> Operation {
    match response {
        DaemonResponse::Operation(operation) => operation,
        DaemonResponse::Project(_) | DaemonResponse::Projects(_) => {
            panic!("operation request must return an operation")
        }
        DaemonResponse::SessionInventory(_)
        | DaemonResponse::SessionEvidence(_)
        | DaemonResponse::BindingRegistration(_)
        | DaemonResponse::SessionDecision(_)
        | DaemonResponse::Conversation(_) => {
            panic!("operation request must not return a V0.2 session response")
        }
    }
}

fn terminal(
    control: &OperationControl,
    operation_id: aiop::contract::control::OperationId,
) -> Operation {
    operation(
        control
            .handle(DaemonRequest::OperationWait {
                operation_id,
                wait_millis: 5_000,
            })
            .expect("operation wait succeeds"),
    )
}

struct CancellationBeforeLaunchTarget {
    observed_operation: mpsc::Sender<TargetOperationId>,
    invocations: AtomicUsize,
}

impl TargetPort for CancellationBeforeLaunchTarget {
    fn execute(&self, command: TargetCommand) -> TargetOutcome {
        if self.invocations.fetch_add(1, Ordering::SeqCst) > 0 {
            let failure = "test provider launch failure after cancellation".to_owned();
            return match command.launch_report.send(
                aiop::contract::target::TargetLaunch::SpawnFailed(failure.clone()),
            ) {
                Ok(()) => TargetOutcome::SpawnFailed(failure),
                Err(_) => TargetOutcome::Indeterminate(
                    "test coordinator stopped before launch failure acknowledgement".to_owned(),
                ),
            };
        }
        match self.observed_operation.send(command.operation_id) {
            Ok(()) => {}
            Err(_) => {
                return TargetOutcome::Indeterminate(
                    "test coordinator stopped before observing target admission".to_owned(),
                );
            }
        }
        while !command.cancel_requested.load(Ordering::SeqCst) {
            thread::yield_now();
        }
        match command
            .launch_report
            .send(aiop::contract::target::TargetLaunch::CancelledBeforeLaunch)
        {
            Ok(()) => TargetOutcome::CancelledBeforeLaunch(
                "test cancellation reached target before provider launch".to_owned(),
            ),
            Err(_) => TargetOutcome::Indeterminate(
                "test coordinator stopped before cancellation acknowledgement".to_owned(),
            ),
        }
    }

    fn cancel(&self, _: TargetOperationId) -> Result<(), String> {
        Ok(())
    }

    fn start_live(
        &self,
        _: aiop::contract::target::TargetLiveStart,
        _: std::sync::mpsc::Sender<aiop::contract::target::TargetLiveObservation>,
    ) -> Result<(), aiop::contract::target::TargetLiveStartError> {
        Err(aiop::contract::target::TargetLiveStartError::NoWriter(
            "pre-launch target fixture does not implement live conversations".to_owned(),
        ))
    }

    fn send_live(
        &self,
        _: TargetOperationId,
        _: aiop::contract::target::TargetLiveTurn,
    ) -> Result<(), String> {
        Err("pre-launch target fixture does not implement live conversations".to_owned())
    }

    fn stop_live(
        &self,
        _: TargetOperationId,
        _: aiop::contract::target::TargetLiveStop,
    ) -> Result<(), String> {
        Err("pre-launch target fixture does not implement live conversations".to_owned())
    }
}

#[test]
fn cancellation_before_provider_launch_returns_the_durable_cancelled_operation() {
    let (directory, project) = passive_fixture();
    let (target_sender, target_receiver) = mpsc::channel();
    let control = OperationControl::new(
        Arc::new(
            SqliteState::open(&directory.path().join("operator.sqlite"))
                .expect("SQLite state opens"),
        ),
        Arc::new(CancellationBeforeLaunchTarget {
            observed_operation: target_sender,
            invocations: AtomicUsize::new(0),
        }),
    );
    control
        .handle(DaemonRequest::ProjectRegister(project.clone()))
        .expect("project registers");
    let starter = control.clone();
    let start_request = start(&project, "cancel before provider", OperationIntent::New);
    let started =
        thread::spawn(move || starter.handle(DaemonRequest::OperationStart(start_request)));
    let operation_id = target_receiver
        .recv()
        .expect("target observes one admitted operation");
    let canceller = control.clone();
    let cancelled = thread::spawn(move || {
        canceller.handle(DaemonRequest::OperationCancel {
            operation_id: aiop::contract::control::OperationId::new_exact(operation_id.0),
        })
    });
    let started = operation(
        started
            .join()
            .expect("operation starter joins")
            .expect("operation start returns durable cancellation"),
    );
    let cancelled = operation(
        cancelled
            .join()
            .expect("operation canceller joins")
            .expect("operation cancellation returns durable cancellation"),
    );
    assert_eq!(started.state, OperationState::Cancelled);
    assert_eq!(cancelled.state, OperationState::Cancelled);
    assert_eq!(started.operation_id, cancelled.operation_id);
    let reused = operation(
        control
            .handle(DaemonRequest::OperationStart(start(
                &project,
                "same session after pre-launch cancellation",
                OperationIntent::ResumeExact {
                    session_id: started.session_id,
                },
            )))
            .expect("cancelled session is admitted for an exact resume"),
    );
    assert_eq!(reused.state, OperationState::Failed);
}

#[test]
fn cancellation_after_runtime_gate_release_returns_the_durable_terminal_operation() {
    let (_directory, control, project) = fixture();
    control
        .handle(DaemonRequest::ProjectRegister(project.clone()))
        .expect("project registers");
    let started = operation(
        control
            .handle(DaemonRequest::OperationStart(start(
                &project,
                "terminal before cancellation race",
                OperationIntent::New,
            )))
            .expect("operation starts"),
    );
    let completed = terminal(&control, started.operation_id);
    let cancellation = operation(
        control
            .handle(DaemonRequest::OperationCancel {
                operation_id: started.operation_id,
            })
            .expect("terminal operation remains observable after gate release"),
    );
    assert_eq!(cancellation, completed);
}

#[test]
fn new_and_exact_resume_are_durable_and_idempotent() {
    let (directory, control, project) = fixture();
    control
        .handle(DaemonRequest::ProjectRegister(project.clone()))
        .expect("project registers");
    let request = start(&project, "first review", OperationIntent::New);
    let accepted = operation(
        control
            .handle(DaemonRequest::OperationStart(request.clone()))
            .expect("operation accepts"),
    );
    let duplicate = operation(
        control
            .handle(DaemonRequest::OperationStart(request))
            .expect("same complete request is idempotent"),
    );
    assert_eq!(accepted.operation_id, duplicate.operation_id);
    let completed = terminal(&control, accepted.operation_id);
    assert_eq!(completed.state, OperationState::Succeeded);
    assert_eq!(completed.observed_session_id, Some(completed.session_id));
    assert_eq!(completed.observed_model.as_deref(), Some("opus"));
    assert_eq!(
        completed.observed_claude_version.as_deref(),
        Some("fixture-1")
    );
    let resume = start(
        &project,
        "follow-up depends on first review",
        OperationIntent::ResumeExact {
            session_id: completed.session_id,
        },
    );
    let resumed = operation(
        control
            .handle(DaemonRequest::OperationStart(resume))
            .expect("exact resume accepts"),
    );
    let resumed = terminal(&control, resumed.operation_id);
    assert_eq!(resumed.state, OperationState::Succeeded);
    assert_eq!(resumed.session_id, completed.session_id);
    match resumed.terminal_outcome {
        Some(TerminalOutcome::Succeeded(result)) => assert!(result.contains("first review")),
        Some(TerminalOutcome::Failed(_))
        | Some(TerminalOutcome::Cancelled(_))
        | Some(TerminalOutcome::Indeterminate(_))
        | None => panic!("exact resume must use prior same-session review content"),
    }
    let invocations = invocations(&directory);
    assert_eq!(invocations.len(), 2);
    assert!(
        invocations[0]
            .argv
            .iter()
            .any(|argument| argument == "--session-id")
    );
    assert!(
        invocations[1]
            .argv
            .iter()
            .any(|argument| argument == "--resume")
    );
}

#[test]
fn invalid_request_precedes_project_lookup_and_changed_fingerprint_conflicts() {
    let (_directory, control, project) = fixture();
    let invalid = OperationStart {
        prompt: String::new(),
        ..start(&project, "unused", OperationIntent::New)
    };
    let invalid_result = control.handle(DaemonRequest::OperationStart(invalid));
    assert!(matches!(
        invalid_result,
        Err(aiop::contract::control::OperatorError::InvalidRequest(_))
    ));
    let response = control.handle(DaemonRequest::OperationStart(start(
        &project,
        "unknown project",
        OperationIntent::New,
    )));
    assert!(matches!(
        response,
        Err(aiop::contract::control::OperatorError::UnknownProject(_))
    ));
    control
        .handle(DaemonRequest::ProjectRegister(project.clone()))
        .expect("project registers");
    let initial = start(&project, "same request", OperationIntent::New);
    let conflict = OperationStart {
        prompt: "changed prompt".to_owned(),
        ..initial.clone()
    };
    control
        .handle(DaemonRequest::OperationStart(initial))
        .expect("initial accepts");
    let conflict_result = control.handle(DaemonRequest::OperationStart(conflict));
    assert!(matches!(
        conflict_result,
        Err(aiop::contract::control::OperatorError::Conflict(_))
    ));
}

#[test]
fn concurrent_session_writer_is_refused_and_cancellation_observes_terminal_result() {
    let (_directory, control, project) = fixture();
    control
        .handle(DaemonRequest::ProjectRegister(project.clone()))
        .expect("project registers");
    let first = operation(
        control
            .handle(DaemonRequest::OperationStart(start(
                &project,
                "__fixture_hold_for_cancel__",
                OperationIntent::New,
            )))
            .expect("operation accepts"),
    );
    thread::sleep(Duration::from_millis(30));
    let writer = control.handle(DaemonRequest::OperationStart(start(
        &project,
        "conflicting writer",
        OperationIntent::ResumeExact {
            session_id: first.session_id,
        },
    )));
    assert!(matches!(
        writer,
        Err(aiop::contract::control::OperatorError::Conflict(_))
    ));
    let cancelled = operation(
        control
            .handle(DaemonRequest::OperationCancel {
                operation_id: first.operation_id,
            })
            .expect("cancellation observes child exit"),
    );
    assert_eq!(cancelled.state, OperationState::Cancelled);
    assert!(matches!(
        cancelled.terminal_outcome,
        Some(TerminalOutcome::Cancelled(_))
    ));
}

#[test]
fn target_handles_unknown_events_and_large_results() {
    let (_directory, control, project) = fixture();
    control
        .handle(DaemonRequest::ProjectRegister(project.clone()))
        .expect("project registers");
    for prompt in ["__fixture_unknown_event__", "__fixture_large_result__"] {
        let accepted = operation(
            control
                .handle(DaemonRequest::OperationStart(start(
                    &project,
                    prompt,
                    OperationIntent::New,
                )))
                .expect("operation accepts"),
        );
        let completed = terminal(&control, accepted.operation_id);
        assert_eq!(completed.state, OperationState::Succeeded);
        if prompt == "__fixture_large_result__" {
            match completed.terminal_outcome {
                Some(TerminalOutcome::Succeeded(result)) => {
                    assert_eq!(result, "r".repeat(32 * 1024));
                }
                Some(TerminalOutcome::Failed(_))
                | Some(TerminalOutcome::Cancelled(_))
                | Some(TerminalOutcome::Indeterminate(_))
                | None => panic!("large terminal result must be byte-exact and complete"),
            }
        }
    }
}

#[test]
fn target_decodes_complete_byte_frames_without_consuming_partial_stdout_suffixes() {
    let (_directory, control, project) = fixture();
    control
        .handle(DaemonRequest::ProjectRegister(project.clone()))
        .expect("project registers");
    for (prompt, expected_result) in [
        (
            "__fixture_split_utf8_result__",
            "split UTF-8 review: Привет 😀",
        ),
        (
            "__fixture_result_then_fragment__",
            "terminal result before fragment",
        ),
    ] {
        let started = operation(
            control
                .handle(DaemonRequest::OperationStart(start(
                    &project,
                    prompt,
                    OperationIntent::New,
                )))
                .expect("operation starts"),
        );
        let completed = terminal(&control, started.operation_id);
        assert_eq!(completed.state, OperationState::Succeeded);
        match completed.terminal_outcome {
            Some(TerminalOutcome::Succeeded(result)) => assert_eq!(result, expected_result),
            Some(TerminalOutcome::Failed(_))
            | Some(TerminalOutcome::Cancelled(_))
            | Some(TerminalOutcome::Indeterminate(_))
            | None => panic!("complete terminal result must survive partial stdout bytes"),
        }
    }
}

#[test]
fn direct_terminal_classification_outranks_pending_large_prompt_writer() {
    let (_directory, control, project) = fixture();
    control
        .handle(DaemonRequest::ProjectRegister(project.clone()))
        .expect("project registers");
    let pending_prompt = format!(
        "__fixture_pending_input_cancel__{}",
        "x".repeat(2 * 1024 * 1024)
    );
    let cancelled = operation(
        control
            .handle(DaemonRequest::OperationStart(start(
                &project,
                &pending_prompt,
                OperationIntent::New,
            )))
            .expect("pending-input operation starts"),
    );
    let cancelled = operation(
        control
            .handle(DaemonRequest::OperationCancel {
                operation_id: cancelled.operation_id,
            })
            .expect("cancellation observes direct child exit"),
    );
    assert_eq!(cancelled.state, OperationState::Cancelled);
    let nonzero_prompt = format!(
        "__fixture_nonzero_without_input__{}",
        "x".repeat(2 * 1024 * 1024)
    );
    let nonzero = operation(
        control
            .handle(DaemonRequest::OperationStart(start(
                &project,
                &nonzero_prompt,
                OperationIntent::New,
            )))
            .expect("nonzero pending-input operation starts"),
    );
    match terminal(&control, nonzero.operation_id).terminal_outcome {
        Some(TerminalOutcome::Failed(message)) => {
            assert!(message.contains("direct Claude child exited unsuccessfully"));
        }
        Some(TerminalOutcome::Succeeded(_))
        | Some(TerminalOutcome::Cancelled(_))
        | Some(TerminalOutcome::Indeterminate(_))
        | None => panic!("nonzero direct child exit must outrank prompt writer failure"),
    }
}

#[test]
fn provider_failure_and_missing_result_are_causally_classified() {
    let (_directory, control, project) = fixture();
    control
        .handle(DaemonRequest::ProjectRegister(project.clone()))
        .expect("project registers");
    let failed = operation(
        control
            .handle(DaemonRequest::OperationStart(start(
                &project,
                "__fixture_terminal_failure__",
                OperationIntent::New,
            )))
            .expect("provider failure operation accepts"),
    );
    let failed = terminal(&control, failed.operation_id);
    assert_eq!(failed.state, OperationState::Failed);
    match failed.terminal_outcome {
        Some(TerminalOutcome::Failed(message)) => {
            assert!(message.contains("fixture provider rejected review"));
            assert!(message.contains("fixture provider diagnostic"));
        }
        Some(TerminalOutcome::Succeeded(_))
        | Some(TerminalOutcome::Cancelled(_))
        | Some(TerminalOutcome::Indeterminate(_))
        | None => panic!("provider terminal failure must retain its causal evidence"),
    }
    let missing_failure_text = operation(
        control
            .handle(DaemonRequest::OperationStart(start(
                &project,
                "__fixture_terminal_failure_without_text__",
                OperationIntent::New,
            )))
            .expect("failure without text operation accepts"),
    );
    match terminal(&control, missing_failure_text.operation_id).terminal_outcome {
        Some(TerminalOutcome::Failed(message)) => {
            assert!(message.contains("omitted result text"));
        }
        Some(TerminalOutcome::Succeeded(_))
        | Some(TerminalOutcome::Cancelled(_))
        | Some(TerminalOutcome::Indeterminate(_))
        | None => panic!("missing provider failure text must remain explicit"),
    }
    let missing = operation(
        control
            .handle(DaemonRequest::OperationStart(start(
                &project,
                "__fixture_exit_without_result__",
                OperationIntent::New,
            )))
            .expect("missing result operation accepts"),
    );
    assert_eq!(
        terminal(&control, missing.operation_id).state,
        OperationState::Indeterminate
    );
}

#[test]
fn launch_failure_persists_failed_operation_and_releases_new_session() {
    let (_directory, control, mut project) = fixture();
    project.claude_executable = Path::new("/not-a-real-aiop-claude").to_path_buf();
    control
        .handle(DaemonRequest::ProjectRegister(project.clone()))
        .expect("project registers");
    let accepted = operation(
        control
            .handle(DaemonRequest::OperationStart(start(
                &project,
                "launch must fail",
                OperationIntent::New,
            )))
            .expect("durable operation is returned despite launch failure"),
    );
    assert_eq!(
        terminal(&control, accepted.operation_id).state,
        OperationState::Failed
    );
    let reused = operation(
        control
            .handle(DaemonRequest::OperationStart(start(
                &project,
                "same session after launch failure",
                OperationIntent::ResumeExact {
                    session_id: accepted.session_id,
                },
            )))
            .expect("failed launch releases the session writer claim"),
    );
    assert_eq!(reused.state, OperationState::Failed);
}

#[test]
fn daemon_eof_before_response_is_transport_unavailable() {
    let directory = TempDir::new().expect("temporary test directory is available");
    let socket = directory.path().join("eof.sock");
    let listener = UnixListener::bind(&socket).expect("test listener binds");
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().expect("test listener accepts");
        let mut request = String::new();
        BufReader::new(stream)
            .read_line(&mut request)
            .expect("test listener reads request");
    });
    let result = client::call(&socket, DaemonRequest::ProjectList);
    server.join().expect("test listener joins");
    assert!(matches!(
        result,
        Err(aiop::contract::control::OperatorError::TransportUnavailable(message))
            if message == "daemon closed before sending a response"
    ));
}

#[test]
fn mismatch_is_indeterminate_and_large_prompt_with_early_output_completes() {
    let (_directory, control, project) = fixture();
    control
        .handle(DaemonRequest::ProjectRegister(project.clone()))
        .expect("project registers");
    let mismatch = operation(
        control
            .handle(DaemonRequest::OperationStart(start(
                &project,
                "__fixture_session_mismatch__",
                OperationIntent::New,
            )))
            .expect("mismatch operation launches"),
    );
    match terminal(&control, mismatch.operation_id).terminal_outcome {
        Some(TerminalOutcome::Indeterminate(message)) => {
            assert!(message.contains("init session mismatch"));
            assert!(message.contains(&mismatch.session_id.value().to_string()));
            assert!(message.contains("00000000-0000-4000-8000-000000000000"));
        }
        Some(TerminalOutcome::Succeeded(_))
        | Some(TerminalOutcome::Failed(_))
        | Some(TerminalOutcome::Cancelled(_))
        | None => panic!("session mismatch must be an explicit indeterminate outcome"),
    }
    let model_mismatch = operation(
        control
            .handle(DaemonRequest::OperationStart(start(
                &project,
                "__fixture_model_mismatch__",
                OperationIntent::New,
            )))
            .expect("model mismatch operation launches"),
    );
    match terminal(&control, model_mismatch.operation_id).terminal_outcome {
        Some(TerminalOutcome::Indeterminate(message)) => {
            assert!(message.contains("init model mismatch"));
            assert!(message.contains("intended opus"));
            assert!(message.contains("observed other-model"));
        }
        Some(TerminalOutcome::Succeeded(_))
        | Some(TerminalOutcome::Failed(_))
        | Some(TerminalOutcome::Cancelled(_))
        | None => panic!("model mismatch must be an explicit indeterminate outcome"),
    }
    let prompt = format!("__fixture_early_large_output__{}", "p".repeat(128 * 1024));
    let large = operation(
        control
            .handle(DaemonRequest::OperationStart(start(
                &project,
                &prompt,
                OperationIntent::New,
            )))
            .expect("large prompt operation launches"),
    );
    assert_eq!(
        terminal(&control, large.operation_id).state,
        OperationState::Succeeded
    );
}

#[test]
fn second_state_owner_refusal_preserves_live_operation() {
    let (directory, control, project) = fixture();
    let database = directory.path().join("operator.sqlite");
    control
        .handle(DaemonRequest::ProjectRegister(project.clone()))
        .expect("project registers");
    let started = operation(
        control
            .handle(DaemonRequest::OperationStart(start(
                &project,
                "__fixture_hold_for_cancel__",
                OperationIntent::New,
            )))
            .expect("live operation starts"),
    );
    assert_eq!(started.state, OperationState::Running);
    match SqliteState::open(&database) {
        Ok(_) => panic!("a second state owner must be refused while the first is live"),
        Err(aiop::contract::control::OperatorError::State(message)) => {
            assert!(message.contains("already owned"));
        }
        Err(error) => panic!("state owner refusal must preserve its causal type: {error}"),
    }
    let unchanged = operation(
        control
            .handle(DaemonRequest::OperationGet {
                operation_id: started.operation_id,
            })
            .expect("live operation remains queryable"),
    );
    assert_eq!(unchanged, started);
    let completed = terminal(&control, started.operation_id);
    assert_eq!(completed.state, OperationState::Succeeded);
    match completed.terminal_outcome {
        Some(TerminalOutcome::Succeeded(result)) => {
            assert_eq!(result, "complete review: __fixture_hold_for_cancel__");
        }
        Some(TerminalOutcome::Failed(message)) => {
            panic!("live operation failed after second-owner refusal: {message}")
        }
        Some(TerminalOutcome::Cancelled(message)) => {
            panic!("live operation was cancelled after second-owner refusal: {message}")
        }
        Some(TerminalOutcome::Indeterminate(message)) => {
            panic!("live operation became indeterminate after second-owner refusal: {message}")
        }
        None => panic!("live operation omitted its terminal result after second-owner refusal"),
    }
}

#[test]
fn daemon_restart_marks_interrupted_work_indeterminate_and_refuses_exact_resume() {
    let (directory, project) = passive_fixture();
    let database = directory.path().join("operator.sqlite");
    let socket_a = directory.path().join("daemon-a.sock");
    let socket_b = directory.path().join("daemon-b.sock");
    let mut daemon_a = start_daemon(&database, &socket_a);
    let mut mcp_a = start_mcp(&socket_a);
    let mut input_a = mcp_a.take_stdin();
    let mut output_a = BufReader::new(mcp_a.take_stdout());
    initialize_mcp(&mut input_a, &mut output_a);
    register_project_mcp(&mut input_a, &mut output_a, 2, &project, &directory);
    send_mcp(
        &mut input_a,
        serde_json::json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"operation_start","arguments":{"request_id":Uuid::new_v4().to_string(),"project_id":project.project_id.as_str(),"intent":{"kind":"new"},"prompt":"__fixture_hold_for_cancel__","review_profile":"opus_read_only"}}}),
    );
    let interrupted = operation_from_mcp(receive_mcp(&mut output_a));
    assert_eq!(interrupted.state, OperationState::Running);
    daemon_a.terminate();
    let mut daemon_b = start_daemon(&database, &socket_b);
    let mut mcp_b = start_mcp(&socket_b);
    let mut input_b = mcp_b.take_stdin();
    let mut output_b = BufReader::new(mcp_b.take_stdout());
    initialize_mcp(&mut input_b, &mut output_b);
    send_mcp(
        &mut input_b,
        serde_json::json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"operation_get","arguments":{"operation_id":interrupted.operation_id.value().to_string()}}}),
    );
    let recovered = operation_from_mcp(receive_mcp(&mut output_b));
    assert_eq!(recovered.operation_id, interrupted.operation_id);
    assert_eq!(recovered.state, OperationState::Indeterminate);
    match recovered.terminal_outcome {
        Some(TerminalOutcome::Indeterminate(message)) => {
            assert_eq!(
                message,
                "daemon restarted before direct child was classified"
            );
        }
        Some(TerminalOutcome::Succeeded(result)) => {
            panic!("interrupted operation unexpectedly succeeded: {result}")
        }
        Some(TerminalOutcome::Failed(message)) => {
            panic!("interrupted operation unexpectedly failed: {message}")
        }
        Some(TerminalOutcome::Cancelled(message)) => {
            panic!("interrupted operation unexpectedly cancelled: {message}")
        }
        None => panic!("interrupted operation omitted its durable terminal outcome"),
    }
    send_mcp(
        &mut input_b,
        serde_json::json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"operation_start","arguments":{"request_id":Uuid::new_v4().to_string(),"project_id":project.project_id.as_str(),"intent":{"kind":"resume_exact","session_id":interrupted.session_id.value().to_string()},"prompt":"must be refused after daemon restart","review_profile":"opus_read_only"}}}),
    );
    let refusal = receive_mcp(&mut output_b);
    assert_eq!(refusal["id"], 3);
    assert_eq!(refusal["result"]["isError"], true);
    let refusal_text = refusal["result"]["content"][0]["text"]
        .as_str()
        .expect("unclassified-session refusal contains text");
    assert!(refusal_text.contains("session cannot be classified after daemon restart"));
    send_mcp(
        &mut input_b,
        serde_json::json!({"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"operation_start","arguments":{"request_id":Uuid::new_v4().to_string(),"project_id":project.project_id.as_str(),"intent":{"kind":"new"},"prompt":"independent session after restart","review_profile":"opus_read_only"}}}),
    );
    let independent = operation_from_mcp(receive_mcp(&mut output_b));
    let independent = wait_mcp(
        &mut input_b,
        &mut output_b,
        5,
        independent.operation_id.value().to_string(),
    );
    match independent.terminal_outcome {
        Some(TerminalOutcome::Succeeded(result)) => {
            assert_eq!(result, "complete review: independent session after restart");
        }
        Some(TerminalOutcome::Failed(message)) => {
            panic!("independent operation failed after restart: {message}")
        }
        Some(TerminalOutcome::Cancelled(message)) => {
            panic!("independent operation was cancelled after restart: {message}")
        }
        Some(TerminalOutcome::Indeterminate(message)) => {
            panic!("independent operation was indeterminate after restart: {message}")
        }
        None => panic!("independent operation omitted its terminal outcome"),
    }
    mcp_b.terminate();
    daemon_b.terminate();
    mcp_a.terminate();
}

#[test]
fn mcp_stdio_projects_through_the_real_daemon() {
    let (directory, project) = passive_fixture();
    let socket = directory.path().join("operator.sock");
    let mut daemon = start_daemon(&directory.path().join("operator.sqlite"), &socket);
    let mut mcp = start_mcp(&socket);
    let mut input = mcp.take_stdin();
    let mut output = BufReader::new(mcp.take_stdout());
    initialize_mcp(&mut input, &mut output);
    send_mcp(
        &mut input,
        serde_json::json!({"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}),
    );
    let listed = receive_mcp(&mut output);
    assert_eq!(listed["id"], 2);
    let tools = listed["result"]["tools"]
        .as_array()
        .expect("tools/list returns tool schemas");
    assert!(tools.iter().any(|tool| tool["name"] == "project_register"));
    let operation_start = tools
        .iter()
        .find(|tool| tool["name"] == "operation_start")
        .expect("operation_start is advertised");
    assert!(
        operation_start["inputSchema"]["required"]
            .as_array()
            .is_some_and(|required| required.iter().any(|field| field == "review_profile"))
    );
    register_project_mcp(&mut input, &mut output, 3, &project, &directory);
    let invalid_request_id = Uuid::new_v4().to_string();
    send_mcp(
        &mut input,
        serde_json::json!({"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"operation_start","arguments":{"request_id":invalid_request_id,"project_id":project.project_id.as_str(),"intent":{"kind":"new"},"prompt":"must not reach daemon","review_profile":"unsupported"}}}),
    );
    let unsupported_profile = receive_mcp(&mut output);
    assert_eq!(unsupported_profile["id"], 4);
    assert_eq!(unsupported_profile["result"]["isError"], true);
    let missing_request_id = Uuid::new_v4().to_string();
    send_mcp(
        &mut input,
        serde_json::json!({"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"operation_start","arguments":{"request_id":missing_request_id,"project_id":project.project_id.as_str(),"intent":{"kind":"new"},"prompt":"must not reach daemon"}}}),
    );
    let missing_profile = receive_mcp(&mut output);
    assert_eq!(missing_profile["id"], 5);
    assert_eq!(missing_profile["result"]["isError"], true);
    let request_id = Uuid::new_v4().to_string();
    send_mcp(
        &mut input,
        serde_json::json!({"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"operation_start","arguments":{"request_id":request_id,"project_id":project.project_id.as_str(),"intent":{"kind":"new"},"prompt":"first review content","review_profile":"opus_read_only"}}}),
    );
    let started = operation_from_mcp(receive_mcp(&mut output));
    assert_eq!(started.state, OperationState::Running);
    let first = wait_mcp(
        &mut input,
        &mut output,
        7,
        started.operation_id.value().to_string(),
    );
    assert_eq!(first.state, OperationState::Succeeded);
    let resume_request = Uuid::new_v4().to_string();
    send_mcp(
        &mut input,
        serde_json::json!({"jsonrpc":"2.0","id":8,"method":"tools/call","params":{"name":"operation_start","arguments":{"request_id":resume_request,"project_id":project.project_id.as_str(),"intent":{"kind":"resume_exact","session_id":first.session_id.value().to_string()},"prompt":"follow-up","review_profile":"opus_read_only"}}}),
    );
    let resumed = operation_from_mcp(receive_mcp(&mut output));
    assert_eq!(resumed.session_id, first.session_id);
    let second = wait_mcp(
        &mut input,
        &mut output,
        9,
        resumed.operation_id.value().to_string(),
    );
    match second.terminal_outcome {
        Some(TerminalOutcome::Succeeded(result)) => {
            assert!(result.contains("first review content"))
        }
        Some(TerminalOutcome::Failed(message)) => {
            panic!("resume failed instead of preserving prior review content: {message}")
        }
        Some(TerminalOutcome::Cancelled(message)) => {
            panic!("resume was cancelled instead of preserving prior review content: {message}")
        }
        Some(TerminalOutcome::Indeterminate(message)) => {
            panic!("resume was indeterminate instead of preserving prior review content: {message}")
        }
        None => panic!("resume omitted a terminal outcome"),
    }
    assert_eq!(invocations(&directory).len(), 2);
    mcp.terminate();
    daemon.terminate();
}

fn register_project_mcp(
    input: &mut impl Write,
    output: &mut impl BufRead,
    id: u64,
    project: &ProjectRegistration,
    directory: &TempDir,
) {
    let working_directory = directory.path().display().to_string();
    let executable = project.claude_executable.display().to_string();
    let registered = call_tool(
        input,
        output,
        id,
        "project_register",
        serde_json::json!({"project_id":project.project_id.as_str(),"working_directory":working_directory,"claude_executable":executable,"expected_opus_model":"opus"}),
    );
    assert_eq!(registered["result"]["isError"], false);
}

fn operation_from_mcp(response: serde_json::Value) -> Operation {
    let text = response["result"]["content"][0]["text"]
        .as_str()
        .expect("MCP tool response contains operation JSON");
    match serde_json::from_str::<DaemonResponse>(text).expect("daemon response JSON") {
        DaemonResponse::Operation(operation) => operation,
        DaemonResponse::Project(_) | DaemonResponse::Projects(_) => {
            panic!("MCP response must be an operation")
        }
        DaemonResponse::SessionInventory(_)
        | DaemonResponse::SessionEvidence(_)
        | DaemonResponse::BindingRegistration(_)
        | DaemonResponse::SessionDecision(_)
        | DaemonResponse::Conversation(_) => {
            panic!("MCP response must not be a V0.2 session response")
        }
    }
}
fn wait_mcp(
    input: &mut impl Write,
    output: &mut impl BufRead,
    id: u64,
    operation_id: String,
) -> Operation {
    send_mcp(
        input,
        serde_json::json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":{"name":"operation_wait","arguments":{"operation_id":operation_id,"wait_millis":5000}}}),
    );
    operation_from_mcp(receive_mcp(output))
}
#[derive(Deserialize)]
struct Invocation {
    argv: Vec<String>,
}

fn invocations(directory: &TempDir) -> Vec<Invocation> {
    let path = directory.path().join(".aiop-fake-invocations.jsonl");
    let contents = fs::read_to_string(path).expect("fake invocation trace is readable");
    contents
        .lines()
        .map(|line| serde_json::from_str(line).expect("fake invocation trace is JSON"))
        .collect()
}
