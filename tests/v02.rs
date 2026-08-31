// Copyright 2026 Yuriy Krasilnikov
// SPDX-License-Identifier: Apache-2.0

mod support;

use std::{
    fs,
    io::{BufRead, BufReader, Write},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use support::{call_tool, initialize_mcp, receive_mcp, send_mcp, start_daemon, start_mcp};

use aiop::{
    contract::{
        control::{
            BindingPersistence, DaemonRequest, DaemonResponse, InitiatorAgentIdentity,
            InitiatorBinding, InitiatorBindingRequest, InitiatorIdentity, InitiatorSessionIdentity,
            Operation, OperationAdmission, OperationIntent, OperationStart, OperationState,
            OperatorError, ProjectId, ProjectRegistration, RequestId, ReviewProfile, RoleIdentity,
            SessionContinuity, SessionDecision, SessionDecisionEvidence, SessionDecisionRequest,
            SessionEvidence, SessionId, SessionInspectRequest, SessionInventoryRequest,
            SessionRefusalReason, StatePort, SubjectIdentity, TaskIdentity, TerminalOutcome,
        },
        target::{
            TargetCommand, TargetLaunch, TargetOperationId, TargetOutcome, TargetPort,
            TargetSuccess,
        },
    },
    control::OperationControl,
    state::SqliteState,
};
use tempfile::TempDir;
use uuid::Uuid;

struct SucceedingTarget {
    invocations: AtomicUsize,
    version: Option<String>,
}

impl TargetPort for SucceedingTarget {
    fn execute(&self, command: TargetCommand) -> TargetOutcome {
        self.invocations.fetch_add(1, Ordering::SeqCst);
        match command.launch_report.send(TargetLaunch::Launched) {
            Ok(()) => {}
            Err(_) => {
                return TargetOutcome::Indeterminate(
                    "test operation starter stopped before launch acknowledgement".to_owned(),
                );
            }
        }
        match command.running_permission.recv() {
            Ok(Ok(())) => TargetOutcome::Success(TargetSuccess {
                result: format!("review result for {}", command.prompt),
                observed_session_id: command.session_id,
                observed_model: command.expected_model,
                observed_version: self.version.clone(),
            }),
            Ok(Err(error)) => TargetOutcome::Failed(error),
            Err(_) => TargetOutcome::Indeterminate(
                "test operation starter stopped before durable running acknowledgement".to_owned(),
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
            "V0.2 target fixture does not implement live conversations".to_owned(),
        ))
    }

    fn send_live(
        &self,
        _: TargetOperationId,
        _: aiop::contract::target::TargetLiveTurn,
    ) -> Result<(), String> {
        Err("V0.2 target fixture does not implement live conversations".to_owned())
    }

    fn stop_live(
        &self,
        _: TargetOperationId,
        _: aiop::contract::target::TargetLiveStop,
    ) -> Result<(), String> {
        Err("V0.2 target fixture does not implement live conversations".to_owned())
    }
}

struct Fixture {
    _directory: TempDir,
    database: PathBuf,
    state: Arc<SqliteState>,
    control: OperationControl,
    target: Arc<SucceedingTarget>,
    project: ProjectRegistration,
}

fn fixture(version: Option<String>) -> Fixture {
    let directory = TempDir::new().expect("temporary test directory is available");
    let database = directory.path().join("operator.sqlite");
    let state = Arc::new(SqliteState::open(&database).expect("SQLite state opens"));
    let target = Arc::new(SucceedingTarget {
        invocations: AtomicUsize::new(0),
        version,
    });
    let control = OperationControl::new(state.clone(), target.clone());
    let project = ProjectRegistration {
        project_id: ProjectId::new("v02-project".to_owned()).expect("fixture project id is valid"),
        working_directory: directory.path().to_path_buf(),
        claude_executable: PathBuf::from("test-target"),
        expected_opus_model: "opus".to_owned(),
    };
    control
        .handle(DaemonRequest::ProjectRegister(project.clone()))
        .expect("project registers");
    Fixture {
        _directory: directory,
        database,
        state,
        control,
        target,
        project,
    }
}

fn identity(role: &str, task: &str, subject: &str) -> InitiatorIdentity {
    InitiatorIdentity {
        initiator_session_id: InitiatorSessionIdentity::new("initiator-session".to_owned())
            .expect("fixture initiator session is valid"),
        initiator_agent_id: InitiatorAgentIdentity::new("main-agent".to_owned())
            .expect("fixture initiator agent is valid"),
        role_id: RoleIdentity::new(role.to_owned()).expect("fixture role is valid"),
        task_id: TaskIdentity::new(task.to_owned()).expect("fixture task is valid"),
        subject_id: SubjectIdentity::new(subject.to_owned()).expect("fixture subject is valid"),
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
        DaemonResponse::Project(_)
        | DaemonResponse::Projects(_)
        | DaemonResponse::SessionInventory(_)
        | DaemonResponse::SessionEvidence(_)
        | DaemonResponse::BindingRegistration(_)
        | DaemonResponse::SessionDecision(_)
        | DaemonResponse::Conversation(_)
        | DaemonResponse::OperationDiagnostics(_) => {
            panic!("operation request returned a non-operation payload")
        }
    }
}

fn decision(response: DaemonResponse) -> SessionDecision {
    match response {
        DaemonResponse::SessionDecision(decision) => decision,
        DaemonResponse::Project(_)
        | DaemonResponse::Projects(_)
        | DaemonResponse::Operation(_)
        | DaemonResponse::SessionInventory(_)
        | DaemonResponse::SessionEvidence(_)
        | DaemonResponse::BindingRegistration(_)
        | DaemonResponse::Conversation(_)
        | DaemonResponse::OperationDiagnostics(_) => {
            panic!("session decision request returned another payload")
        }
    }
}

fn evidence(response: DaemonResponse) -> Vec<SessionEvidence> {
    match response {
        DaemonResponse::SessionInventory(evidence) | DaemonResponse::SessionEvidence(evidence) => {
            evidence
        }
        DaemonResponse::Project(_)
        | DaemonResponse::Projects(_)
        | DaemonResponse::Operation(_)
        | DaemonResponse::BindingRegistration(_)
        | DaemonResponse::SessionDecision(_)
        | DaemonResponse::Conversation(_)
        | DaemonResponse::OperationDiagnostics(_) => {
            panic!("session evidence request returned another payload")
        }
    }
}

fn successful_operation(fixture: &Fixture, prompt: &str) -> Operation {
    let started = operation(
        fixture
            .control
            .handle(DaemonRequest::OperationStart(start(
                &fixture.project,
                prompt,
                OperationIntent::New,
            )))
            .expect("operation starts"),
    );
    operation(
        fixture
            .control
            .handle(DaemonRequest::OperationWait {
                operation_id: started.operation_id,
                wait_millis: 5_000,
            })
            .expect("operation reaches a terminal result"),
    )
}

#[test]
fn session_tools_use_registered_projects_after_the_execution_directory_disappears() {
    let directory = TempDir::new().expect("temporary test directory is available");
    let working_directory = directory.path().join("working-directory");
    fs::create_dir(&working_directory).expect("fixture working directory creates");
    let state = Arc::new(
        SqliteState::open(&directory.path().join("operator.sqlite")).expect("SQLite state opens"),
    );
    let target = Arc::new(SucceedingTarget {
        invocations: AtomicUsize::new(0),
        version: None,
    });
    let control = OperationControl::new(state, target);
    let project = ProjectRegistration {
        project_id: ProjectId::new("removed-directory-project".to_owned())
            .expect("fixture project id is valid"),
        working_directory: working_directory.clone(),
        claude_executable: PathBuf::from("test-target"),
        expected_opus_model: "opus".to_owned(),
    };
    control
        .handle(DaemonRequest::ProjectRegister(project.clone()))
        .expect("project registers before its directory disappears");
    let started = operation(
        control
            .handle(DaemonRequest::OperationStart(start(
                &project,
                "durable evidence before directory removal",
                OperationIntent::New,
            )))
            .expect("operation starts while its directory exists"),
    );
    let completed = operation(
        control
            .handle(DaemonRequest::OperationWait {
                operation_id: started.operation_id,
                wait_millis: 5_000,
            })
            .expect("operation completes"),
    );
    fs::remove_dir(&working_directory).expect("fixture working directory removes");

    let inventory = evidence(
        control
            .handle(DaemonRequest::SessionInventory(SessionInventoryRequest {
                project_id: project.project_id.clone(),
            }))
            .expect("inventory remains available for a registered project"),
    );
    assert_eq!(inventory.len(), 1);
    let inspected = evidence(
        control
            .handle(DaemonRequest::SessionInspect(SessionInspectRequest {
                project_id: project.project_id.clone(),
                target_session_id: completed.session_id,
            }))
            .expect("inspection remains available for a registered project"),
    );
    assert_eq!(inspected, inventory);
    let current_identity = identity("reviewer", "gone-directory", "subject");
    control
        .handle(DaemonRequest::InitiatorBindingRegister(
            InitiatorBindingRequest {
                project_id: project.project_id.clone(),
                identity: current_identity.clone(),
                target_session_id: completed.session_id,
            },
        ))
        .expect("binding remains available for a registered project");
    match decision(
        control
            .handle(DaemonRequest::SessionDecide(SessionDecisionRequest {
                project_id: project.project_id.clone(),
                identity: current_identity,
                continuity: SessionContinuity::ContinueBound,
            }))
            .expect("bound decision remains available for a registered project"),
    ) {
        SessionDecision::ResumeExact {
            target_session_id,
            evidence_operation_ids,
        } => {
            assert_eq!(target_session_id, completed.session_id);
            assert_eq!(evidence_operation_ids, vec![completed.operation_id]);
        }
        SessionDecision::New { .. } | SessionDecision::Refuse { .. } => {
            panic!("registered durable evidence must still allow exact resume")
        }
    }
    match control.handle(DaemonRequest::OperationStart(start(
        &project,
        "execution after directory removal",
        OperationIntent::New,
    ))) {
        Err(OperatorError::InvalidRequest(message)) => {
            assert_eq!(message, "registered working_directory is not a directory")
        }
        other => panic!("operation start must retain execution-time cwd validation: {other:?}"),
    }
}

#[test]
fn exact_session_decisions_use_only_durable_operator_evidence_and_bindings() {
    let fixture = fixture(None);
    let first = successful_operation(&fixture, "first evidence");
    match first.terminal_outcome {
        Some(TerminalOutcome::Succeeded(_)) => {}
        Some(TerminalOutcome::Failed(message)) => panic!("evidence operation failed: {message}"),
        Some(TerminalOutcome::Cancelled(message)) => {
            panic!("evidence operation cancelled: {message}")
        }
        Some(TerminalOutcome::Indeterminate(message)) => {
            panic!("evidence operation was indeterminate: {message}")
        }
        None => panic!("evidence operation omitted its result"),
    }
    let inventory = evidence(
        fixture
            .control
            .handle(DaemonRequest::SessionInventory(SessionInventoryRequest {
                project_id: fixture.project.project_id.clone(),
            }))
            .expect("inventory succeeds"),
    );
    assert_eq!(inventory.len(), 1);
    assert_eq!(inventory[0].operation_id, first.operation_id);
    assert_eq!(inventory[0].target_session_id, first.session_id);
    assert_eq!(inventory[0].observed_model, "opus");
    assert_eq!(inventory[0].observed_claude_version, None);

    let inspected = evidence(
        fixture
            .control
            .handle(DaemonRequest::SessionInspect(SessionInspectRequest {
                project_id: fixture.project.project_id.clone(),
                target_session_id: first.session_id,
            }))
            .expect("exact evidence inspection succeeds"),
    );
    assert_eq!(inspected, inventory);
    let unknown = fixture
        .control
        .handle(DaemonRequest::SessionInspect(SessionInspectRequest {
            project_id: fixture.project.project_id.clone(),
            target_session_id: aiop::contract::control::SessionId::new(),
        }));
    match unknown {
        Err(OperatorError::UnknownSession { project_id, .. }) => {
            assert_eq!(project_id, fixture.project.project_id)
        }
        other => panic!("inspection must return typed unknown_session: {other:?}"),
    }

    let current_identity = identity("reviewer", "task-a", "subject-a");
    let registration = fixture
        .control
        .handle(DaemonRequest::InitiatorBindingRegister(
            InitiatorBindingRequest {
                project_id: fixture.project.project_id.clone(),
                identity: current_identity.clone(),
                target_session_id: first.session_id,
            },
        ))
        .expect("evidenced binding registers");
    match registration {
        DaemonResponse::BindingRegistration(registration) => {
            assert_eq!(
                registration.status,
                aiop::contract::control::BindingRegistrationStatus::Inserted
            );
            assert_eq!(registration.binding.target_session_id, first.session_id);
        }
        DaemonResponse::Project(_)
        | DaemonResponse::Projects(_)
        | DaemonResponse::Operation(_)
        | DaemonResponse::SessionInventory(_)
        | DaemonResponse::SessionEvidence(_)
        | DaemonResponse::SessionDecision(_)
        | DaemonResponse::Conversation(_)
        | DaemonResponse::OperationDiagnostics(_) => {
            panic!("binding registration returned another payload")
        }
    }
    let existing = fixture
        .control
        .handle(DaemonRequest::InitiatorBindingRegister(
            InitiatorBindingRequest {
                project_id: fixture.project.project_id.clone(),
                identity: current_identity.clone(),
                target_session_id: first.session_id,
            },
        ))
        .expect("identical binding is idempotent");
    match existing {
        DaemonResponse::BindingRegistration(registration) => assert_eq!(
            registration.status,
            aiop::contract::control::BindingRegistrationStatus::Existing
        ),
        DaemonResponse::Project(_)
        | DaemonResponse::Projects(_)
        | DaemonResponse::Operation(_)
        | DaemonResponse::SessionInventory(_)
        | DaemonResponse::SessionEvidence(_)
        | DaemonResponse::SessionDecision(_)
        | DaemonResponse::Conversation(_)
        | DaemonResponse::OperationDiagnostics(_) => {
            panic!("idempotent binding registration returned another payload")
        }
    }
    let different_session = successful_operation(&fixture, "second evidence");
    let conflict = fixture
        .control
        .handle(DaemonRequest::InitiatorBindingRegister(
            InitiatorBindingRequest {
                project_id: fixture.project.project_id.clone(),
                identity: current_identity.clone(),
                target_session_id: different_session.session_id,
            },
        ));
    match conflict {
        Err(OperatorError::BindingConflict {
            existing_target_session_id,
            requested_target_session_id,
        }) => {
            assert_eq!(existing_target_session_id, first.session_id);
            assert_eq!(requested_target_session_id, different_session.session_id);
        }
        other => panic!("different target under one identity must be typed conflict: {other:?}"),
    }
    let exact = decision(
        fixture
            .control
            .handle(DaemonRequest::SessionDecide(SessionDecisionRequest {
                project_id: fixture.project.project_id.clone(),
                identity: current_identity.clone(),
                continuity: SessionContinuity::ContinueBound,
            }))
            .expect("bound continuity decides"),
    );
    match exact {
        SessionDecision::ResumeExact {
            target_session_id,
            evidence_operation_ids,
        } => {
            assert_eq!(target_session_id, first.session_id);
            assert_eq!(evidence_operation_ids, vec![first.operation_id]);
        }
        SessionDecision::New { .. } | SessionDecision::Refuse { .. } => {
            panic!("exact binding must resume its evidenced target session")
        }
    }
    let invocations_before_decisions = fixture.target.invocations.load(Ordering::SeqCst);
    let independent = decision(
        fixture
            .control
            .handle(DaemonRequest::SessionDecide(SessionDecisionRequest {
                project_id: fixture.project.project_id.clone(),
                identity: current_identity.clone(),
                continuity: SessionContinuity::Independent,
            }))
            .expect("independent decision succeeds"),
    );
    assert_eq!(
        independent,
        SessionDecision::New {
            evidence: SessionDecisionEvidence::Independent
        }
    );
    assert_eq!(
        fixture.target.invocations.load(Ordering::SeqCst),
        invocations_before_decisions
    );

    for changed_identity in [
        identity("other-role", "task-a", "subject-a"),
        identity("reviewer", "other-task", "subject-a"),
        identity("reviewer", "task-a", "other-subject"),
    ] {
        let changed = decision(
            fixture
                .control
                .handle(DaemonRequest::SessionDecide(SessionDecisionRequest {
                    project_id: fixture.project.project_id.clone(),
                    identity: changed_identity,
                    continuity: SessionContinuity::ContinueBound,
                }))
                .expect("changed identity decides"),
        );
        match changed {
            SessionDecision::Refuse {
                reason: SessionRefusalReason::IdentityMismatch,
                evidence: SessionDecisionEvidence::IdentityBindings { bindings },
            } => assert_eq!(bindings.len(), 1),
            SessionDecision::New { .. }
            | SessionDecision::ResumeExact { .. }
            | SessionDecision::Refuse { .. } => {
                panic!("changed identity must refuse before candidate selection")
            }
        }
    }
}

#[test]
fn unbound_and_unproved_bindings_preserve_their_closed_outcomes() {
    let context = fixture(Some("fixture-version".to_owned()));
    let first = successful_operation(&context, "first session");
    let second = successful_operation(&context, "second session");
    let unbound = decision(
        read_only_decision(&context, || {
            context
                .control
                .handle(DaemonRequest::SessionDecide(SessionDecisionRequest {
                    project_id: context.project.project_id.clone(),
                    identity: identity("reviewer", "task-b", "subject-b"),
                    continuity: SessionContinuity::ContinueBound,
                }))
        })
        .expect("unbound decision succeeds"),
    );
    match unbound {
        SessionDecision::Refuse {
            reason: SessionRefusalReason::AmbiguousSessions,
            evidence: SessionDecisionEvidence::CandidateSessions { target_session_ids },
        } => {
            let mut expected = vec![first.session_id, second.session_id];
            expected.sort_by_key(|session_id| session_id.value());
            assert_eq!(target_session_ids, expected);
        }
        SessionDecision::New { .. }
        | SessionDecision::ResumeExact { .. }
        | SessionDecision::Refuse { .. } => {
            panic!("two unbound evidence sessions must refuse as ambiguous")
        }
    }
    let unknown_binding = context
        .control
        .handle(DaemonRequest::InitiatorBindingRegister(
            InitiatorBindingRequest {
                project_id: context.project.project_id.clone(),
                identity: identity("reviewer", "task-c", "subject-c"),
                target_session_id: aiop::contract::control::SessionId::new(),
            },
        ));
    match unknown_binding {
        Err(OperatorError::UnknownSession { .. }) => {}
        other => panic!("unproved binding must not persist: {other:?}"),
    }
    assert_eq!(
        context
            .state
            .get_initiator_binding(
                &context.project.project_id,
                &identity("reviewer", "task-c", "subject-c"),
            )
            .expect("binding lookup succeeds after unknown-session refusal"),
        None
    );
    let other_project = ProjectRegistration {
        project_id: ProjectId::new("other-v02-project".to_owned())
            .expect("fixture project id is valid"),
        working_directory: context._directory.path().to_path_buf(),
        claude_executable: PathBuf::from("test-target"),
        expected_opus_model: "opus".to_owned(),
    };
    context
        .control
        .handle(DaemonRequest::ProjectRegister(other_project.clone()))
        .expect("other project registers");
    let cross_project = context
        .control
        .handle(DaemonRequest::InitiatorBindingRegister(
            InitiatorBindingRequest {
                project_id: other_project.project_id.clone(),
                identity: identity("reviewer", "task-cross", "subject-cross"),
                target_session_id: first.session_id,
            },
        ));
    match cross_project {
        Err(OperatorError::UnknownSession { project_id, .. }) => {
            assert_eq!(project_id, other_project.project_id)
        }
        other => panic!("cross-project session must not be binding evidence: {other:?}"),
    }
    assert_eq!(
        context
            .state
            .get_initiator_binding(
                &other_project.project_id,
                &identity("reviewer", "task-cross", "subject-cross"),
            )
            .expect("cross-project binding lookup succeeds"),
        None
    );
    let stale_binding = InitiatorBinding {
        project_id: context.project.project_id.clone(),
        identity: identity("reviewer", "task-d", "subject-d"),
        target_session_id: aiop::contract::control::SessionId::new(),
    };
    match context
        .state
        .persist_initiator_binding(&stale_binding)
        .expect("state fixture persists exact stale binding")
    {
        BindingPersistence::Inserted => {}
        BindingPersistence::Existing { target_session_id } => {
            panic!("stale fixture binding unexpectedly existed for {target_session_id}")
        }
    }
    let stale = read_only_decision(&context, || {
        context
            .control
            .handle(DaemonRequest::SessionDecide(SessionDecisionRequest {
                project_id: context.project.project_id.clone(),
                identity: stale_binding.identity.clone(),
                continuity: SessionContinuity::ContinueBound,
            }))
    });
    match stale {
        Err(OperatorError::BoundSessionEvidenceMissing {
            binding,
            target_session_id,
        }) => {
            assert_eq!(*binding, stale_binding);
            assert_eq!(target_session_id, stale_binding.target_session_id);
        }
        other => panic!("stale exact binding must remain a typed state error: {other:?}"),
    }
    let no_candidate_fixture = fixture(None);
    let no_candidate = decision(
        read_only_decision(&no_candidate_fixture, || {
            no_candidate_fixture
                .control
                .handle(DaemonRequest::SessionDecide(SessionDecisionRequest {
                    project_id: no_candidate_fixture.project.project_id.clone(),
                    identity: identity("reviewer", "task-e", "subject-e"),
                    continuity: SessionContinuity::ContinueBound,
                }))
        })
        .expect("empty evidence decision succeeds"),
    );
    assert_eq!(
        no_candidate,
        SessionDecision::Refuse {
            reason: SessionRefusalReason::BindingRequired,
            evidence: SessionDecisionEvidence::CandidateSessions {
                target_session_ids: Vec::new(),
            },
        }
    );
    let one_candidate_fixture = fixture(None);
    let only = successful_operation(&one_candidate_fixture, "only candidate");
    let one_candidate = decision(
        read_only_decision(&one_candidate_fixture, || {
            one_candidate_fixture
                .control
                .handle(DaemonRequest::SessionDecide(SessionDecisionRequest {
                    project_id: one_candidate_fixture.project.project_id.clone(),
                    identity: identity("reviewer", "task-f", "subject-f"),
                    continuity: SessionContinuity::ContinueBound,
                }))
        })
        .expect("one-candidate decision succeeds"),
    );
    assert_eq!(
        one_candidate,
        SessionDecision::Refuse {
            reason: SessionRefusalReason::BindingRequired,
            evidence: SessionDecisionEvidence::CandidateSessions {
                target_session_ids: vec![only.session_id],
            },
        }
    );
}

#[test]
fn decisions_exclude_nonqualifying_evidence_and_never_mutate_durable_state() {
    let fixture = fixture(None);
    let qualifying = successful_operation(&fixture, "qualifying evidence");
    let failed = durable_operation(
        &fixture,
        OperationState::Failed,
        Some(qualifying.session_id),
        TerminalOutcome::Failed("fixture failure".to_owned()),
    );
    let cancelled = durable_operation(
        &fixture,
        OperationState::Cancelled,
        Some(qualifying.session_id),
        TerminalOutcome::Cancelled("fixture cancellation".to_owned()),
    );
    let indeterminate = durable_operation(
        &fixture,
        OperationState::Indeterminate,
        Some(qualifying.session_id),
        TerminalOutcome::Indeterminate("fixture ambiguity".to_owned()),
    );
    let accepted = durable_operation(
        &fixture,
        OperationState::Accepted,
        None,
        TerminalOutcome::Succeeded("unused".to_owned()),
    );
    let running = durable_operation(
        &fixture,
        OperationState::Running,
        None,
        TerminalOutcome::Succeeded("unused".to_owned()),
    );
    let mismatched = durable_operation(
        &fixture,
        OperationState::Succeeded,
        Some(SessionId::new()),
        TerminalOutcome::Succeeded("mismatched identity".to_owned()),
    );
    let other_project = ProjectRegistration {
        project_id: ProjectId::new("cross-project-evidence".to_owned())
            .expect("fixture project id is valid"),
        working_directory: fixture._directory.path().to_path_buf(),
        claude_executable: PathBuf::from("test-target"),
        expected_opus_model: "opus".to_owned(),
    };
    fixture
        .control
        .handle(DaemonRequest::ProjectRegister(other_project.clone()))
        .expect("cross-project fixture registers");
    let cross_project = durable_operation_for_project(
        &fixture.state,
        &other_project,
        OperationState::Succeeded,
        None,
        TerminalOutcome::Succeeded("cross project".to_owned()),
    );
    let inventory = evidence(
        fixture
            .control
            .handle(DaemonRequest::SessionInventory(SessionInventoryRequest {
                project_id: fixture.project.project_id.clone(),
            }))
            .expect("inventory reads durable evidence"),
    );
    assert_eq!(inventory.len(), 1);
    assert_eq!(inventory[0].operation_id, qualifying.operation_id);
    assert_ne!(inventory[0].operation_id, failed.operation_id);
    assert_ne!(inventory[0].operation_id, cancelled.operation_id);
    assert_ne!(inventory[0].operation_id, indeterminate.operation_id);
    assert_ne!(inventory[0].operation_id, accepted.operation_id);
    assert_ne!(inventory[0].operation_id, running.operation_id);
    assert_ne!(inventory[0].operation_id, mismatched.operation_id);
    assert_ne!(inventory[0].operation_id, cross_project.operation_id);

    let bound_identity = identity("reviewer", "bound", "subject");
    fixture
        .control
        .handle(DaemonRequest::InitiatorBindingRegister(
            InitiatorBindingRequest {
                project_id: fixture.project.project_id.clone(),
                identity: bound_identity.clone(),
                target_session_id: qualifying.session_id,
            },
        ))
        .expect("qualifying binding registers");
    let exact = read_only_decision(&fixture, || {
        fixture
            .control
            .handle(DaemonRequest::SessionDecide(SessionDecisionRequest {
                project_id: fixture.project.project_id.clone(),
                identity: bound_identity.clone(),
                continuity: SessionContinuity::ContinueBound,
            }))
    });
    match exact.expect("bound decision succeeds") {
        DaemonResponse::SessionDecision(SessionDecision::ResumeExact {
            target_session_id,
            evidence_operation_ids,
        }) => {
            assert_eq!(target_session_id, qualifying.session_id);
            assert_eq!(evidence_operation_ids, vec![qualifying.operation_id]);
        }
        other => panic!("bound evidence must exact-resume: {other:?}"),
    }
    let independent = read_only_decision(&fixture, || {
        fixture
            .control
            .handle(DaemonRequest::SessionDecide(SessionDecisionRequest {
                project_id: fixture.project.project_id.clone(),
                identity: bound_identity.clone(),
                continuity: SessionContinuity::Independent,
            }))
    });
    assert_eq!(
        decision(independent.expect("independent decision succeeds")),
        SessionDecision::New {
            evidence: SessionDecisionEvidence::Independent
        }
    );
    let mismatch = read_only_decision(&fixture, || {
        fixture
            .control
            .handle(DaemonRequest::SessionDecide(SessionDecisionRequest {
                project_id: fixture.project.project_id.clone(),
                identity: identity("other-role", "bound", "subject"),
                continuity: SessionContinuity::ContinueBound,
            }))
    });
    match decision(mismatch.expect("identity mismatch is a refusal")) {
        SessionDecision::Refuse {
            reason: SessionRefusalReason::IdentityMismatch,
            evidence: SessionDecisionEvidence::IdentityBindings { bindings },
        } => assert_eq!(bindings.len(), 1),
        other => panic!("identity mismatch must retain its selecting evidence: {other:?}"),
    }
    let stale_binding = InitiatorBinding {
        project_id: fixture.project.project_id.clone(),
        identity: identity("reviewer", "stale", "subject"),
        target_session_id: failed.session_id,
    };
    match fixture
        .state
        .persist_initiator_binding(&stale_binding)
        .expect("stale binding fixture persists")
    {
        BindingPersistence::Inserted => {}
        BindingPersistence::Existing { target_session_id } => {
            panic!("stale binding unexpectedly existed for {target_session_id}")
        }
    }
    let stale = read_only_decision(&fixture, || {
        fixture
            .control
            .handle(DaemonRequest::SessionDecide(SessionDecisionRequest {
                project_id: fixture.project.project_id.clone(),
                identity: stale_binding.identity.clone(),
                continuity: SessionContinuity::ContinueBound,
            }))
    });
    match stale {
        Err(OperatorError::BoundSessionEvidenceMissing {
            binding,
            target_session_id,
        }) => {
            assert_eq!(*binding, stale_binding);
            assert_eq!(target_session_id, failed.session_id);
        }
        other => panic!("stale binding must remain a no-effect state error: {other:?}"),
    }

    let unbound_identity = InitiatorIdentity {
        initiator_session_id: InitiatorSessionIdentity::new("other-session".to_owned())
            .expect("fixture identity is valid"),
        initiator_agent_id: InitiatorAgentIdentity::new("other-agent".to_owned())
            .expect("fixture identity is valid"),
        role_id: RoleIdentity::new("reviewer".to_owned()).expect("fixture identity is valid"),
        task_id: TaskIdentity::new("unbound".to_owned()).expect("fixture identity is valid"),
        subject_id: SubjectIdentity::new("subject".to_owned()).expect("fixture identity is valid"),
    };
    let ambiguous = read_only_decision(&fixture, || {
        fixture
            .control
            .handle(DaemonRequest::SessionDecide(SessionDecisionRequest {
                project_id: fixture.project.project_id.clone(),
                identity: unbound_identity,
                continuity: SessionContinuity::ContinueBound,
            }))
    });
    match decision(ambiguous.expect("unbound decision succeeds")) {
        SessionDecision::Refuse {
            reason: SessionRefusalReason::BindingRequired,
            evidence: SessionDecisionEvidence::CandidateSessions { target_session_ids },
        } => assert_eq!(target_session_ids, vec![qualifying.session_id]),
        other => panic!("excluded rows must not make one qualifying session ambiguous: {other:?}"),
    }
}

fn durable_operation(
    fixture: &Fixture,
    state: OperationState,
    observed_session_id: Option<SessionId>,
    terminal: TerminalOutcome,
) -> Operation {
    durable_operation_for_project(
        &fixture.state,
        &fixture.project,
        state,
        observed_session_id,
        terminal,
    )
}

fn durable_operation_for_project(
    state_port: &Arc<SqliteState>,
    project: &ProjectRegistration,
    state: OperationState,
    observed_session_id: Option<SessionId>,
    terminal: TerminalOutcome,
) -> Operation {
    let request = start(project, &Uuid::new_v4().to_string(), OperationIntent::New);
    let session_id = SessionId::new();
    let operation = match state_port
        .persist_operation_admission(&request, session_id, "durable fixture")
        .expect("fixture operation persists")
    {
        OperationAdmission::Inserted(operation) => operation,
        OperationAdmission::Existing { operation, .. } => {
            panic!(
                "fixture operation unexpectedly existed: {:?}",
                operation.operation_id
            )
        }
        OperationAdmission::MissingProject => panic!("fixture project disappeared"),
        OperationAdmission::ActiveSession { operation } => {
            panic!(
                "fixture session was already claimed by {:?}",
                operation.operation_id
            )
        }
    };
    match state {
        OperationState::Accepted => operation,
        OperationState::Running => state_port
            .transition(
                operation.operation_id,
                OperationState::Running,
                None,
                None,
                None,
                None,
            )
            .expect("fixture operation reaches running"),
        OperationState::Succeeded
        | OperationState::Failed
        | OperationState::Cancelled
        | OperationState::Indeterminate => {
            state_port
                .transition(
                    operation.operation_id,
                    OperationState::Running,
                    None,
                    None,
                    None,
                    None,
                )
                .expect("fixture operation reaches running");
            state_port
                .transition(
                    operation.operation_id,
                    state,
                    Some(terminal),
                    observed_session_id,
                    Some("opus".to_owned()),
                    None,
                )
                .expect("fixture operation reaches its terminal state")
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
struct DurableSnapshot {
    operations: i64,
    active_sessions: i64,
    bindings: i64,
}

fn read_only_decision<T>(fixture: &Fixture, action: impl FnOnce() -> T) -> T {
    let before = durable_snapshot(&fixture.database);
    let invocations = fixture.target.invocations.load(Ordering::SeqCst);
    let result = action();
    assert_eq!(durable_snapshot(&fixture.database), before);
    assert_eq!(
        fixture.target.invocations.load(Ordering::SeqCst),
        invocations
    );
    result
}

fn durable_snapshot(database: &std::path::Path) -> DurableSnapshot {
    let connection = ::sqlite::open(database).expect("durable state is observable");
    DurableSnapshot {
        operations: row_count(&connection, "operations"),
        active_sessions: row_count(&connection, "active_sessions"),
        bindings: row_count(&connection, "initiator_bindings"),
    }
}

fn row_count(connection: &::sqlite::Connection, table: &str) -> i64 {
    let statement = format!("SELECT COUNT(*) FROM {table}");
    let mut query = connection
        .prepare(statement)
        .expect("durable state count query prepares");
    match query.next().expect("durable state count query executes") {
        ::sqlite::State::Row => query
            .read::<i64, _>(0)
            .expect("durable state count row reads"),
        ::sqlite::State::Done => panic!("durable state count query omitted its row"),
    }
}

#[test]
fn additive_binding_schema_opens_a_v01_database_without_a_second_state_representation() {
    let directory = TempDir::new().expect("temporary test directory is available");
    let database = directory.path().join("operator.sqlite");
    let project = ProjectRegistration {
        project_id: ProjectId::new("v01-schema-project".to_owned())
            .expect("fixture project id is valid"),
        working_directory: directory.path().to_path_buf(),
        claude_executable: PathBuf::from("test-target"),
        expected_opus_model: "opus".to_owned(),
    };
    let session_id = aiop::contract::control::SessionId::new();
    let existing = Operation {
        operation_id: aiop::contract::control::OperationId::new(),
        request_id: RequestId::new(Uuid::new_v4()),
        project_id: project.project_id.clone(),
        intent: OperationIntent::New,
        session_id,
        state: OperationState::Succeeded,
        observed_session_id: Some(session_id),
        observed_model: Some("opus".to_owned()),
        observed_claude_version: Some("preexisting-v01-version".to_owned()),
        terminal_outcome: Some(TerminalOutcome::Succeeded(
            "preexisting V0.1 result".to_owned(),
        )),
    };
    let v01 = ::sqlite::open(&database).expect("V0.1 database opens");
    for schema in [
        "CREATE TABLE projects (project_id TEXT PRIMARY KEY NOT NULL, record_json TEXT NOT NULL)",
        "CREATE TABLE operations (request_id TEXT PRIMARY KEY NOT NULL, operation_id TEXT UNIQUE NOT NULL, session_id TEXT NOT NULL, fingerprint TEXT NOT NULL, record_json TEXT NOT NULL)",
        "CREATE TABLE active_sessions (session_id TEXT PRIMARY KEY NOT NULL, operation_id TEXT UNIQUE NOT NULL)",
    ] {
        v01.execute(schema).expect("V0.1 schema fixture creates");
    }
    insert_v01_project(&v01, &project);
    insert_v01_operation(&v01, &existing);
    drop(v01);
    let state = Arc::new(SqliteState::open(&database).expect("V0.2 opens the V0.1 database"));
    let target = Arc::new(SucceedingTarget {
        invocations: AtomicUsize::new(0),
        version: None,
    });
    let control = OperationControl::new(state, target.clone());
    control
        .handle(DaemonRequest::ProjectRegister(project.clone()))
        .expect("V0.1 project remains registerable idempotently");
    control
        .handle(DaemonRequest::InitiatorBindingRegister(
            InitiatorBindingRequest {
                project_id: project.project_id.clone(),
                identity: identity("reviewer", "v01-task", "v01-subject"),
                target_session_id: existing.session_id,
            },
        ))
        .expect("V0.2 binding persists alongside V0.1 records");
    match decision(
        control
            .handle(DaemonRequest::SessionDecide(SessionDecisionRequest {
                project_id: project.project_id,
                identity: identity("reviewer", "v01-task", "v01-subject"),
                continuity: SessionContinuity::ContinueBound,
            }))
            .expect("exact decision succeeds on the opened V0.1 database"),
    ) {
        SessionDecision::ResumeExact {
            target_session_id,
            evidence_operation_ids,
        } => {
            assert_eq!(target_session_id, existing.session_id);
            assert_eq!(evidence_operation_ids, vec![existing.operation_id]);
        }
        SessionDecision::New { .. } | SessionDecision::Refuse { .. } => {
            panic!("V0.1 evidence plus V0.2 binding must exact-resume")
        }
    }
    assert_eq!(target.invocations.load(Ordering::SeqCst), 0);
}

fn insert_v01_project(connection: &::sqlite::Connection, project: &ProjectRegistration) {
    let record = serde_json::to_string(project).expect("V0.1 project record encodes");
    let mut statement = connection
        .prepare("INSERT INTO projects (project_id, record_json) VALUES (?, ?)")
        .expect("V0.1 project insert prepares");
    statement
        .bind(&[(1, project.project_id.as_str()), (2, record.as_str())][..])
        .expect("V0.1 project insert binds");
    match statement.next().expect("V0.1 project insert executes") {
        ::sqlite::State::Done => {}
        ::sqlite::State::Row => panic!("V0.1 project insert unexpectedly returns a row"),
    }
}

fn insert_v01_operation(connection: &::sqlite::Connection, operation: &Operation) {
    let record = serde_json::to_string(operation).expect("V0.1 operation record encodes");
    let request_id = operation.request_id.value().to_string();
    let operation_id = operation.operation_id.value().to_string();
    let session_id = operation.session_id.value().to_string();
    let mut statement = connection
        .prepare(
            "INSERT INTO operations (request_id, operation_id, session_id, fingerprint, record_json) VALUES (?, ?, ?, ?, ?)",
        )
        .expect("V0.1 operation insert prepares");
    statement
        .bind(
            &[
                (1, request_id.as_str()),
                (2, operation_id.as_str()),
                (3, session_id.as_str()),
                (4, "preexisting-fingerprint"),
                (5, record.as_str()),
            ][..],
        )
        .expect("V0.1 operation insert binds");
    match statement.next().expect("V0.1 operation insert executes") {
        ::sqlite::State::Done => {}
        ::sqlite::State::Row => panic!("V0.1 operation insert unexpectedly returns a row"),
    }
}

#[test]
fn mcp_projects_exact_session_tools_as_closed_success_refusal_and_error_channels() {
    let directory = TempDir::new().expect("temporary test directory is available");
    let project = ProjectRegistration {
        project_id: ProjectId::new("v02-mcp-project".to_owned())
            .expect("fixture project id is valid"),
        working_directory: directory.path().to_path_buf(),
        claude_executable: PathBuf::from(env!("CARGO_BIN_EXE_aiop-fake-claude")),
        expected_opus_model: "opus".to_owned(),
    };
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
    assert_session_tool_schema(tools, "session_inventory", &["project_id"]);
    assert_session_tool_schema(
        tools,
        "session_inspect",
        &["project_id", "target_session_id"],
    );
    assert_session_tool_schema(
        tools,
        "initiator_binding_register",
        &[
            "project_id",
            "initiator_session_id",
            "initiator_agent_id",
            "role_id",
            "task_id",
            "subject_id",
            "target_session_id",
        ],
    );
    assert_session_tool_schema(
        tools,
        "session_decide",
        &[
            "project_id",
            "initiator_session_id",
            "initiator_agent_id",
            "role_id",
            "task_id",
            "subject_id",
            "continuity",
        ],
    );

    register_project_mcp(&mut input, &mut output, 3, &project, directory.path());
    let started = operation_from_mcp(call_tool(
        &mut input,
        &mut output,
        4,
        "operation_start",
        serde_json::json!({
            "request_id":Uuid::new_v4().to_string(),
            "project_id":project.project_id.as_str(),
            "intent":{"kind":"new"},
            "prompt":"MCP evidence",
            "review_profile":"opus_read_only"
        }),
    ));
    let completed = wait_mcp(
        &mut input,
        &mut output,
        5,
        started.operation_id.value().to_string(),
    );
    assert_eq!(completed.state, OperationState::Succeeded);
    let identities = session_arguments(&project, "reviewer", "task", "subject");

    let inventory = daemon_response(call_tool(
        &mut input,
        &mut output,
        6,
        "session_inventory",
        serde_json::json!({"project_id":project.project_id.as_str()}),
    ));
    match inventory {
        DaemonResponse::SessionInventory(items) => assert_eq!(items.len(), 1),
        other => panic!("session inventory returned another payload: {other:?}"),
    }
    let inspected = daemon_response(call_tool(
        &mut input,
        &mut output,
        7,
        "session_inspect",
        serde_json::json!({
            "project_id":project.project_id.as_str(),
            "target_session_id":completed.session_id.value().to_string()
        }),
    ));
    match inspected {
        DaemonResponse::SessionEvidence(items) => assert_eq!(items.len(), 1),
        other => panic!("session inspection returned another payload: {other:?}"),
    }
    let unbound = daemon_response(call_tool(
        &mut input,
        &mut output,
        8,
        "session_decide",
        serde_json::json!(
            identities
                .clone()
                .into_iter()
                .chain(std::iter::once((
                    "continuity".to_owned(),
                    serde_json::json!("continue_bound"),
                )))
                .collect::<serde_json::Map<String, serde_json::Value>>()
        ),
    ));
    match unbound {
        DaemonResponse::SessionDecision(SessionDecision::Refuse {
            reason: SessionRefusalReason::BindingRequired,
            evidence: SessionDecisionEvidence::CandidateSessions { target_session_ids },
        }) => assert_eq!(target_session_ids, vec![completed.session_id]),
        other => panic!("unbound continuation returned the wrong public decision: {other:?}"),
    }
    let mut binding_arguments = identities.clone();
    binding_arguments.insert(
        "target_session_id".to_owned(),
        serde_json::json!(completed.session_id.value().to_string()),
    );
    let binding = daemon_response(call_tool(
        &mut input,
        &mut output,
        9,
        "initiator_binding_register",
        serde_json::Value::Object(binding_arguments),
    ));
    match binding {
        DaemonResponse::BindingRegistration(registration) => assert_eq!(
            registration.status,
            aiop::contract::control::BindingRegistrationStatus::Inserted
        ),
        other => panic!("binding registration returned another payload: {other:?}"),
    }
    let mut existing_binding_arguments = identities.clone();
    existing_binding_arguments.insert(
        "target_session_id".to_owned(),
        serde_json::json!(completed.session_id.value().to_string()),
    );
    match daemon_response(call_tool(
        &mut input,
        &mut output,
        10,
        "initiator_binding_register",
        serde_json::Value::Object(existing_binding_arguments),
    )) {
        DaemonResponse::BindingRegistration(registration) => assert_eq!(
            registration.status,
            aiop::contract::control::BindingRegistrationStatus::Existing
        ),
        other => panic!("existing binding returned another payload: {other:?}"),
    }
    let exact = daemon_response(call_tool(
        &mut input,
        &mut output,
        11,
        "session_decide",
        serde_json::json!(
            identities
                .clone()
                .into_iter()
                .chain(std::iter::once((
                    "continuity".to_owned(),
                    serde_json::json!("continue_bound"),
                )))
                .collect::<serde_json::Map<String, serde_json::Value>>()
        ),
    ));
    match exact {
        DaemonResponse::SessionDecision(SessionDecision::ResumeExact {
            target_session_id,
            evidence_operation_ids,
        }) => {
            assert_eq!(target_session_id, completed.session_id);
            assert_eq!(evidence_operation_ids, vec![completed.operation_id]);
        }
        other => panic!("bound continuation returned another public decision: {other:?}"),
    }
    assert_eq!(
        decision(daemon_response(call_tool(
            &mut input,
            &mut output,
            12,
            "session_decide",
            serde_json::json!(
                identities
                    .clone()
                    .into_iter()
                    .chain(std::iter::once((
                        "continuity".to_owned(),
                        serde_json::json!("independent"),
                    )))
                    .collect::<serde_json::Map<String, serde_json::Value>>()
            ),
        ))),
        SessionDecision::New {
            evidence: SessionDecisionEvidence::Independent
        }
    );
    let started_second = operation_from_mcp(call_tool(
        &mut input,
        &mut output,
        13,
        "operation_start",
        serde_json::json!({
            "request_id":Uuid::new_v4().to_string(),
            "project_id":project.project_id.as_str(),
            "intent":{"kind":"new"},
            "prompt":"second MCP evidence",
            "review_profile":"opus_read_only"
        }),
    ));
    let second = wait_mcp(
        &mut input,
        &mut output,
        14,
        started_second.operation_id.value().to_string(),
    );
    let mut conflict_arguments = identities.clone();
    conflict_arguments.insert(
        "target_session_id".to_owned(),
        serde_json::json!(second.session_id.value().to_string()),
    );
    assert_binding_conflict(
        &call_tool(
            &mut input,
            &mut output,
            15,
            "initiator_binding_register",
            serde_json::Value::Object(conflict_arguments),
        ),
        completed.session_id,
        second.session_id,
    );
    let mismatched_identity = session_arguments(&project, "other-role", "task", "subject");
    match decision(daemon_response(call_tool(
        &mut input,
        &mut output,
        16,
        "session_decide",
        with_continuity(mismatched_identity, "continue_bound"),
    ))) {
        SessionDecision::Refuse {
            reason: SessionRefusalReason::IdentityMismatch,
            evidence: SessionDecisionEvidence::IdentityBindings { bindings },
        } => assert_eq!(bindings.len(), 1),
        other => panic!("identity mismatch must be a public refusal: {other:?}"),
    }
    let ambiguous_identity = serde_json::json!({
        "project_id":project.project_id.as_str(),
        "initiator_session_id":"ambiguous-session",
        "initiator_agent_id":"ambiguous-agent",
        "role_id":"reviewer",
        "task_id":"task",
        "subject_id":"subject"
    })
    .as_object()
    .expect("JSON object literal is an object")
    .clone();
    match decision(daemon_response(call_tool(
        &mut input,
        &mut output,
        17,
        "session_decide",
        with_continuity(ambiguous_identity, "continue_bound"),
    ))) {
        SessionDecision::Refuse {
            reason: SessionRefusalReason::AmbiguousSessions,
            evidence: SessionDecisionEvidence::CandidateSessions { target_session_ids },
        } => {
            let mut expected = vec![completed.session_id, second.session_id];
            expected.sort_by_key(|session_id| session_id.value());
            assert_eq!(target_session_ids, expected);
        }
        other => panic!("two evidenced sessions must be an ambiguity refusal: {other:?}"),
    }

    let unknown_session = SessionId::new();
    let domain = call_tool(
        &mut input,
        &mut output,
        18,
        "session_inspect",
        serde_json::json!({
            "project_id":project.project_id.as_str(),
            "target_session_id":unknown_session.value().to_string()
        }),
    );
    assert_unknown_session(&domain, &project.project_id, unknown_session);
    let unknown_project = ProjectId::new("unknown-mcp-project".to_owned())
        .expect("unknown fixture project id is valid");
    assert_unknown_project(
        &call_tool(
            &mut input,
            &mut output,
            19,
            "session_inventory",
            serde_json::json!({"project_id":unknown_project.as_str()}),
        ),
        &unknown_project,
    );
    assert_unknown_project(
        &call_tool(
            &mut input,
            &mut output,
            20,
            "session_inspect",
            serde_json::json!({"project_id":unknown_project.as_str(),"target_session_id":completed.session_id.value().to_string()}),
        ),
        &unknown_project,
    );
    let mut unknown_project_binding = identities.clone();
    unknown_project_binding.insert(
        "project_id".to_owned(),
        serde_json::json!(unknown_project.as_str()),
    );
    unknown_project_binding.insert(
        "target_session_id".to_owned(),
        serde_json::json!(completed.session_id.value().to_string()),
    );
    assert_unknown_project(
        &call_tool(
            &mut input,
            &mut output,
            21,
            "initiator_binding_register",
            serde_json::Value::Object(unknown_project_binding),
        ),
        &unknown_project,
    );
    let mut unknown_project_decision = identities.clone();
    unknown_project_decision.insert(
        "project_id".to_owned(),
        serde_json::json!(unknown_project.as_str()),
    );
    assert_unknown_project(
        &call_tool(
            &mut input,
            &mut output,
            22,
            "session_decide",
            with_continuity(unknown_project_decision, "independent"),
        ),
        &unknown_project,
    );
    let unknown_binding_session = SessionId::new();
    let mut unknown_binding_arguments = session_arguments(&project, "unknown", "task", "subject");
    unknown_binding_arguments.insert(
        "target_session_id".to_owned(),
        serde_json::json!(unknown_binding_session.value().to_string()),
    );
    assert_unknown_session(
        &call_tool(
            &mut input,
            &mut output,
            23,
            "initiator_binding_register",
            serde_json::Value::Object(unknown_binding_arguments),
        ),
        &project.project_id,
        unknown_binding_session,
    );
    let stale_session = SessionId::new();
    let stale_identity = identity("stale", "task", "subject");
    insert_stale_binding(
        &directory.path().join("operator.sqlite"),
        &project,
        &stale_identity,
        stale_session,
    );
    assert_bound_session_evidence_missing(
        &call_tool(
            &mut input,
            &mut output,
            24,
            "session_decide",
            with_continuity(
                session_arguments(&project, "stale", "task", "subject"),
                "continue_bound",
            ),
        ),
        &project,
        &stale_identity,
        stale_session,
    );

    assert_gateway_invalid(&call_tool(
        &mut input,
        &mut output,
        25,
        "session_inventory",
        serde_json::json!({}),
    ));
    assert_gateway_invalid(&call_tool(
        &mut input,
        &mut output,
        26,
        "session_inspect",
        serde_json::json!({"project_id":project.project_id.as_str(),"target_session_id":"not-a-uuid"}),
    ));
    assert_gateway_invalid(&call_tool(
        &mut input,
        &mut output,
        27,
        "initiator_binding_register",
        serde_json::json!({
            "project_id":project.project_id.as_str(),
            "initiator_session_id":"initiator-session",
            "initiator_agent_id":"main-agent",
            "role_id":"reviewer",
            "task_id":"task",
            "subject_id":"subject",
            "target_session_id":completed.session_id.value().to_string(),
            "extra":true
        }),
    ));
    assert_gateway_invalid(&call_tool(
        &mut input,
        &mut output,
        28,
        "session_decide",
        serde_json::json!({
            "project_id":project.project_id.as_str(),
            "initiator_session_id":"initiator-session",
            "initiator_agent_id":"main-agent",
            "role_id":"reviewer",
            "task_id":"task",
            "subject_id":"subject",
            "continuity":"unknown"
        }),
    ));
    assert_eq!(fake_invocations(directory.path()).len(), 2);
    mcp.terminate();
    daemon.terminate();
}

fn assert_session_tool_schema(tools: &[serde_json::Value], name: &str, required: &[&str]) {
    let tool = match tools.iter().find(|tool| tool["name"] == name) {
        Some(tool) => tool,
        None => panic!("{name} is advertised"),
    };
    assert_eq!(tool["inputSchema"]["additionalProperties"], false);
    let advertised = tool["inputSchema"]["required"]
        .as_array()
        .expect("tool schema lists required fields");
    for field in required {
        assert!(advertised.iter().any(|item| item == field));
    }
}

fn session_arguments(
    project: &ProjectRegistration,
    role: &str,
    task: &str,
    subject: &str,
) -> serde_json::Map<String, serde_json::Value> {
    serde_json::json!({
        "project_id":project.project_id.as_str(),
        "initiator_session_id":"initiator-session",
        "initiator_agent_id":"main-agent",
        "role_id":role,
        "task_id":task,
        "subject_id":subject
    })
    .as_object()
    .expect("JSON object literal is an object")
    .clone()
}

fn with_continuity(
    mut arguments: serde_json::Map<String, serde_json::Value>,
    continuity: &str,
) -> serde_json::Value {
    arguments.insert("continuity".to_owned(), serde_json::json!(continuity));
    serde_json::Value::Object(arguments)
}

fn daemon_response(response: serde_json::Value) -> DaemonResponse {
    assert_eq!(response["result"]["isError"], false);
    let text = response["result"]["content"][0]["text"]
        .as_str()
        .expect("successful MCP response contains daemon JSON");
    serde_json::from_str(text).expect("successful MCP response is daemon JSON")
}

fn gateway_operator_error(response: &serde_json::Value, error_kind: &str) -> serde_json::Value {
    assert_eq!(response["result"]["isError"], true);
    let text = response["result"]["content"][0]["text"]
        .as_str()
        .expect("MCP error contains JSON");
    let error: serde_json::Value = serde_json::from_str(text).expect("MCP error is JSON");
    assert_eq!(error["kind"], "operator");
    assert_eq!(error["data"]["error"]["kind"], error_kind);
    error
}

fn assert_unknown_project(response: &serde_json::Value, project_id: &ProjectId) {
    let error = gateway_operator_error(response, "unknown_project");
    assert_eq!(error["data"]["error"]["message"], project_id.as_str());
}

fn assert_unknown_session(
    response: &serde_json::Value,
    project_id: &ProjectId,
    target_session_id: SessionId,
) {
    let error = gateway_operator_error(response, "unknown_session");
    assert_eq!(
        error["data"]["error"]["message"]["project_id"],
        project_id.as_str()
    );
    assert_eq!(
        error["data"]["error"]["message"]["target_session_id"],
        target_session_id.value().to_string()
    );
}

fn assert_binding_conflict(
    response: &serde_json::Value,
    existing_target_session_id: SessionId,
    requested_target_session_id: SessionId,
) {
    let error = gateway_operator_error(response, "binding_conflict");
    assert_eq!(
        error["data"]["error"]["message"]["existing_target_session_id"],
        existing_target_session_id.value().to_string()
    );
    assert_eq!(
        error["data"]["error"]["message"]["requested_target_session_id"],
        requested_target_session_id.value().to_string()
    );
}

fn assert_bound_session_evidence_missing(
    response: &serde_json::Value,
    project: &ProjectRegistration,
    identity: &InitiatorIdentity,
    target_session_id: SessionId,
) {
    let error = gateway_operator_error(response, "bound_session_evidence_missing");
    let binding = &error["data"]["error"]["message"]["binding"];
    assert_eq!(binding["project_id"], project.project_id.as_str());
    assert_eq!(
        binding["identity"]["initiator_session_id"],
        identity.initiator_session_id.as_str()
    );
    assert_eq!(
        binding["identity"]["initiator_agent_id"],
        identity.initiator_agent_id.as_str()
    );
    assert_eq!(binding["identity"]["role_id"], identity.role_id.as_str());
    assert_eq!(binding["identity"]["task_id"], identity.task_id.as_str());
    assert_eq!(
        binding["identity"]["subject_id"],
        identity.subject_id.as_str()
    );
    assert_eq!(
        error["data"]["error"]["message"]["target_session_id"],
        target_session_id.value().to_string()
    );
}

fn insert_stale_binding(
    database: &std::path::Path,
    project: &ProjectRegistration,
    identity: &InitiatorIdentity,
    target_session_id: SessionId,
) {
    let binding = InitiatorBinding {
        project_id: project.project_id.clone(),
        identity: identity.clone(),
        target_session_id,
    };
    let record = serde_json::to_string(&binding).expect("stale binding record encodes");
    let target = target_session_id.value().to_string();
    let connection = ::sqlite::open(database).expect("durable state opens for stale fixture");
    let mut statement = connection
        .prepare(
            "INSERT INTO initiator_bindings (project_id, initiator_session_id, initiator_agent_id, role_id, task_id, subject_id, target_session_id, record_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .expect("stale binding insert prepares");
    statement
        .bind(
            &[
                (1, project.project_id.as_str()),
                (2, identity.initiator_session_id.as_str()),
                (3, identity.initiator_agent_id.as_str()),
                (4, identity.role_id.as_str()),
                (5, identity.task_id.as_str()),
                (6, identity.subject_id.as_str()),
                (7, target.as_str()),
                (8, record.as_str()),
            ][..],
        )
        .expect("stale binding insert binds");
    match statement.next().expect("stale binding insert executes") {
        ::sqlite::State::Done => {}
        ::sqlite::State::Row => panic!("stale binding insert unexpectedly returns a row"),
    }
}

fn assert_gateway_invalid(response: &serde_json::Value) {
    assert_eq!(response["result"]["isError"], true);
    let text = response["result"]["content"][0]["text"]
        .as_str()
        .expect("MCP invalid arguments error contains JSON");
    let error: serde_json::Value =
        serde_json::from_str(text).expect("MCP invalid arguments error is JSON");
    assert_eq!(error["kind"], "invalid_arguments");
}

fn register_project_mcp(
    input: &mut impl Write,
    output: &mut impl BufRead,
    id: u64,
    project: &ProjectRegistration,
    working_directory: &std::path::Path,
) {
    let response = call_tool(
        input,
        output,
        id,
        "project_register",
        serde_json::json!({
            "project_id":project.project_id.as_str(),
            "working_directory":working_directory.display().to_string(),
            "claude_executable":project.claude_executable.display().to_string(),
            "expected_opus_model":project.expected_opus_model
        }),
    );
    assert_eq!(response["result"]["isError"], false);
}

fn operation_from_mcp(response: serde_json::Value) -> Operation {
    match daemon_response(response) {
        DaemonResponse::Operation(operation) => operation,
        other => panic!("MCP operation call returned another payload: {other:?}"),
    }
}

fn wait_mcp(
    input: &mut impl Write,
    output: &mut impl BufRead,
    id: u64,
    operation_id: String,
) -> Operation {
    operation_from_mcp(call_tool(
        input,
        output,
        id,
        "operation_wait",
        serde_json::json!({"operation_id":operation_id,"wait_millis":5000}),
    ))
}

fn fake_invocations(directory: &std::path::Path) -> Vec<serde_json::Value> {
    let contents = fs::read_to_string(directory.join(".aiop-fake-invocations.jsonl"))
        .expect("fake invocation trace is readable");
    contents
        .lines()
        .map(|line| serde_json::from_str(line).expect("fake invocation trace is JSON"))
        .collect()
}
