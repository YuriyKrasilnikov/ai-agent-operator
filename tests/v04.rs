// Copyright 2026 Yuriy Krasilnikov
// SPDX-License-Identifier: Apache-2.0

mod support;

use std::{fs, io::BufReader, path::Path, sync::Arc};

use aiop::{
    contract::control::{
        ConversationStart, DaemonRequest, DaemonResponse, OperationDiagnosticPayload,
        OperationDiagnosticsRequest, OperationId, OperationIntent, OperationStart, OperationState,
        OperatorError, ProjectId, ProjectRegistration, RequestId, ReviewProfile, SessionId,
        StatePort, TerminalOutcome, TurnId,
    },
    control::OperationControl,
    state::SqliteState,
    target::ClaudeTarget,
};
use tempfile::TempDir;
use uuid::Uuid;

use support::{call_tool, initialize_mcp, start_daemon, start_mcp};

fn fixture() -> (
    TempDir,
    Arc<SqliteState>,
    OperationControl,
    ProjectRegistration,
) {
    let directory = TempDir::new().expect("temporary test directory is available");
    let state = Arc::new(
        SqliteState::open(&directory.path().join("operator.sqlite")).expect("SQLite state opens"),
    );
    let control = OperationControl::new(state.clone(), Arc::new(ClaudeTarget::default()));
    let project = ProjectRegistration {
        project_id: ProjectId::new("v04-project".to_owned()).expect("fixture project id is valid"),
        working_directory: directory.path().to_path_buf(),
        claude_executable: Path::new(env!("CARGO_BIN_EXE_aiop-fake-claude")).to_path_buf(),
        expected_opus_model: "opus".to_owned(),
    };
    control
        .handle(DaemonRequest::ProjectRegister(project.clone()))
        .expect("project registers");
    (directory, state, control, project)
}

fn start(project: &ProjectRegistration, prompt: &str) -> OperationStart {
    OperationStart {
        request_id: RequestId::new(Uuid::new_v4()),
        project_id: project.project_id.clone(),
        intent: OperationIntent::New,
        prompt: prompt.to_owned(),
        review_profile: ReviewProfile::OpusReadOnly,
    }
}

#[test]
fn one_shot_diagnostics_are_normalized_durable_and_visible_while_running() {
    let (_directory, state, control, project) = fixture();
    let operation = match control
        .handle(DaemonRequest::OperationStart(start(
            &project,
            "__fixture_diagnostics__",
        )))
        .expect("operation starts")
    {
        DaemonResponse::Operation(operation) => operation,
        other => panic!("operation start returned another response: {other:?}"),
    };
    let first = control
        .handle(DaemonRequest::OperationDiagnostics(
            OperationDiagnosticsRequest {
                operation_id: operation.operation_id,
                after_diagnostic_sequence: 0,
                wait_millis: 5_000,
            },
        ))
        .expect("diagnostics read while child remains active");
    let snapshot = match first {
        DaemonResponse::OperationDiagnostics(snapshot) => snapshot,
        other => panic!("diagnostic query returned another response: {other:?}"),
    };
    assert_eq!(
        snapshot.operation.state,
        aiop::contract::control::OperationState::Running
    );
    assert!(!snapshot.diagnostics.is_empty());
    assert!(snapshot.diagnostics.len() <= 3);
    assert_eq!(snapshot.diagnostics[0].diagnostic_sequence, 1);
    let terminal = control
        .handle(DaemonRequest::OperationWait {
            operation_id: operation.operation_id,
            wait_millis: 5_000,
        })
        .expect("operation terminalizes after recorder joins");
    match terminal {
        DaemonResponse::Operation(operation) => {
            assert!(operation.state.terminal());
        }
        other => panic!("operation wait returned another response: {other:?}"),
    }
    let closed = state
        .get_operation_diagnostics(operation.operation_id, 0)
        .expect("terminal barrier closes the complete diagnostic timeline");
    assert_eq!(closed.diagnostics.len(), 3);
    assert_eq!(
        serde_json::to_value(&closed.diagnostics[0].payload).expect("payload serializes"),
        serde_json::json!({"kind":"provider_retrying","data":{"attempt":1,"max_retries":2,"retry_delay_ms":0}})
    );
    assert_eq!(
        closed.diagnostics[1].payload,
        OperationDiagnosticPayload::AuthenticationFailed
    );
    assert_eq!(
        closed.diagnostics[2].payload,
        OperationDiagnosticPayload::DiagnosticUnclassified
    );
    assert_eq!(
        closed
            .diagnostics
            .iter()
            .map(|diagnostic| diagnostic.diagnostic_sequence)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    let durable = state
        .get_operation_diagnostics(operation.operation_id, 1)
        .expect("reconnected cursor reads the closed durable suffix");
    assert_eq!(durable.diagnostics, closed.diagnostics[1..]);
    match state.record_operation_diagnostic(
        operation.operation_id,
        OperationDiagnosticPayload::AuthenticationFailed,
    ) {
        Err(OperatorError::Conflict(message))
            if message
                == "operation diagnostics can be appended only while the operation is running" => {}
        other => panic!("terminal operation must refuse late diagnostics: {other:?}"),
    }
}

#[test]
fn live_conversations_are_refused_by_one_shot_diagnostics() {
    let (_directory, state, control, project) = fixture();
    let admission = state
        .persist_conversation_start(
            &ConversationStart {
                request_id: RequestId::new(Uuid::new_v4()),
                project_id: project.project_id,
                intent: OperationIntent::New,
                turn_id: TurnId::new(Uuid::new_v4()),
                prompt: "live prompt".to_owned(),
                review_profile: ReviewProfile::OpusReadOnly,
            },
            aiop::contract::control::SessionId::new(),
            "live-diagnostics-conflict",
        )
        .expect("live operation persists");
    let operation = match admission {
        aiop::contract::control::ConversationStartAdmission::Inserted { operation, .. } => {
            operation
        }
        other => panic!("first live operation must insert: {other:?}"),
    };
    match control.handle(DaemonRequest::OperationDiagnostics(
        OperationDiagnosticsRequest {
            operation_id: operation.operation_id,
            after_diagnostic_sequence: 0,
            wait_millis: 0,
        },
    )) {
        Err(OperatorError::Conflict(message))
            if message == "operation diagnostics are unavailable for live conversations" => {}
        other => panic!("live operation must be a typed diagnostic conflict: {other:?}"),
    }
}

#[test]
fn malformed_provider_retry_and_invalid_public_retry_payloads_are_rejected() {
    let invalid_wire = serde_json::json!({
        "kind":"provider_retrying",
        "data":{"attempt":3,"max_retries":2,"retry_delay_ms":0}
    });
    assert!(serde_json::from_value::<OperationDiagnosticPayload>(invalid_wire).is_err());
}

#[test]
fn pre_output_pump_return_has_covered_but_empty_diagnostics() {
    let (directory, _state, control, mut project) = fixture();
    project.claude_executable = directory.path().join("missing-claude");
    project.project_id = ProjectId::new("v04-pre-pump".to_owned()).expect("project id is valid");
    control
        .handle(DaemonRequest::ProjectRegister(project.clone()))
        .expect("pre-pump project registers");
    let operation = match control
        .handle(DaemonRequest::OperationStart(start(
            &project,
            "never pumped",
        )))
        .expect("spawn failure remains a durable operation")
    {
        DaemonResponse::Operation(operation) => operation,
        other => panic!("operation start returned another response: {other:?}"),
    };
    assert!(operation.state.terminal());
    let snapshot = match control
        .handle(DaemonRequest::OperationDiagnostics(
            OperationDiagnosticsRequest {
                operation_id: operation.operation_id,
                after_diagnostic_sequence: 0,
                wait_millis: 0,
            },
        ))
        .expect("covered pre-pump operation has a diagnostic snapshot")
    {
        DaemonResponse::OperationDiagnostics(snapshot) => snapshot,
        other => panic!("diagnostic query returned another response: {other:?}"),
    };
    assert!(snapshot.diagnostics.is_empty());
}

#[test]
fn old_one_shot_without_coverage_returns_typed_unavailable() {
    let directory = TempDir::new().expect("temporary test directory is available");
    let database = directory.path().join("legacy.sqlite");
    let request_id = RequestId::new(Uuid::new_v4());
    let operation_id = OperationId::new();
    let operation = aiop::contract::control::Operation {
        operation_id,
        request_id,
        project_id: ProjectId::new("legacy-project".to_owned()).expect("project id is valid"),
        intent: OperationIntent::New,
        session_id: SessionId::new(),
        state: OperationState::Succeeded,
        observed_session_id: None,
        observed_model: None,
        observed_claude_version: None,
        terminal_outcome: Some(TerminalOutcome::Succeeded("legacy result".to_owned())),
    };
    let connection = sqlite::open(&database).expect("legacy database opens");
    connection
        .execute(
            "CREATE TABLE operations (request_id TEXT PRIMARY KEY NOT NULL, operation_id TEXT UNIQUE NOT NULL, session_id TEXT NOT NULL, fingerprint TEXT NOT NULL, record_json TEXT NOT NULL)",
        )
        .expect("legacy operation table creates");
    let request_key = request_id.value().to_string();
    let operation_key = operation_id.value().to_string();
    let session_key = operation.session_id.value().to_string();
    let record = serde_json::to_string(&operation).expect("legacy operation serializes");
    let mut statement = connection
        .prepare(
            "INSERT INTO operations (request_id, operation_id, session_id, fingerprint, record_json) VALUES (?, ?, ?, ?, ?)",
        )
        .expect("legacy insertion prepares");
    statement
        .bind(
            &[
                (1, request_key.as_str()),
                (2, operation_key.as_str()),
                (3, session_key.as_str()),
                (4, "legacy-fingerprint"),
                (5, record.as_str()),
            ][..],
        )
        .expect("legacy insertion binds");
    statement.next().expect("legacy operation inserts");
    drop(statement);
    drop(connection);
    let state = Arc::new(SqliteState::open(&database).expect("current state opens legacy record"));
    let control = OperationControl::new(state, Arc::new(ClaudeTarget::default()));
    match control.handle(DaemonRequest::OperationDiagnostics(
        OperationDiagnosticsRequest {
            operation_id,
            after_diagnostic_sequence: 0,
            wait_millis: 0,
        },
    )) {
        Err(OperatorError::DiagnosticsUnavailable(message)) if message == operation_key => {}
        other => panic!("old one-shot must report typed unavailable diagnostics: {other:?}"),
    }
}

#[test]
fn separate_mcp_clients_observe_one_child_diagnostic_timeline_and_cursor_reconnect() {
    let directory = TempDir::new().expect("temporary test directory is available");
    let socket = directory.path().join("operator.sock");
    let mut daemon = start_daemon(&directory.path().join("operator.sqlite"), &socket);
    let mut client_a = start_mcp(&socket);
    let mut input_a = client_a.take_stdin();
    let mut output_a = BufReader::new(client_a.take_stdout());
    initialize_mcp(&mut input_a, &mut output_a);
    let project_id = "v04-mcp-project";
    let executable = Path::new(env!("CARGO_BIN_EXE_aiop-fake-claude"))
        .display()
        .to_string();
    let registered = call_tool(
        &mut input_a,
        &mut output_a,
        2,
        "project_register",
        serde_json::json!({
            "project_id": project_id,
            "working_directory": directory.path().display().to_string(),
            "claude_executable": executable,
            "expected_opus_model": "opus"
        }),
    );
    assert_eq!(registered["result"]["isError"], false);
    let started = call_tool(
        &mut input_a,
        &mut output_a,
        3,
        "operation_start",
        serde_json::json!({
            "request_id": Uuid::new_v4().to_string(),
            "project_id": project_id,
            "intent": {"kind":"new"},
            "prompt": "__fixture_diagnostics__",
            "review_profile": "opus_read_only"
        }),
    );
    let operation = daemon_operation(&started);
    let operation_id = operation.operation_id.value().to_string();
    {
        let mut client_b = start_mcp(&socket);
        let mut input_b = client_b.take_stdin();
        let mut output_b = BufReader::new(client_b.take_stdout());
        initialize_mcp(&mut input_b, &mut output_b);
        let rejected = call_tool(
            &mut input_b,
            &mut output_b,
            2,
            "operation_diagnostics",
            serde_json::json!({
                "operation_id": operation_id,
                "after_diagnostic_sequence": 0,
                "wait_millis": 1,
                "unexpected": true
            }),
        );
        assert_eq!(rejected["result"]["isError"], true);
        let rejected_text = rejected["result"]["content"][0]["text"]
            .as_str()
            .expect("strict diagnostic rejection contains JSON");
        let rejected_error: serde_json::Value =
            serde_json::from_str(rejected_text).expect("strict diagnostic rejection is JSON");
        assert_eq!(rejected_error["kind"], "invalid_arguments");
        let observed = call_tool(
            &mut input_b,
            &mut output_b,
            3,
            "operation_diagnostics",
            serde_json::json!({
                "operation_id": operation_id,
                "after_diagnostic_sequence": 0,
                "wait_millis": 5000
            }),
        );
        let snapshot = daemon_diagnostics(&observed);
        assert_eq!(snapshot.operation.state, OperationState::Running);
        assert!(!snapshot.diagnostics.is_empty());
        assert!(snapshot.diagnostics.len() <= 3);
        assert_eq!(snapshot.diagnostics[0].diagnostic_sequence, 1);
    }
    let terminal = call_tool(
        &mut input_a,
        &mut output_a,
        4,
        "operation_wait",
        serde_json::json!({"operation_id":operation_id,"wait_millis":5000}),
    );
    assert!(daemon_operation(&terminal).state.terminal());
    {
        let mut reconnected_client = start_mcp(&socket);
        let mut input = reconnected_client.take_stdin();
        let mut output = BufReader::new(reconnected_client.take_stdout());
        initialize_mcp(&mut input, &mut output);
        let resumed = call_tool(
            &mut input,
            &mut output,
            2,
            "operation_diagnostics",
            serde_json::json!({
                "operation_id": operation_id,
                "after_diagnostic_sequence": 1,
                "wait_millis": 5000
            }),
        );
        let snapshot = daemon_diagnostics(&resumed);
        assert_eq!(
            snapshot
                .diagnostics
                .iter()
                .map(|diagnostic| diagnostic.diagnostic_sequence)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
    }
    let invocations = fs::read_to_string(directory.path().join(".aiop-fake-invocations.jsonl"))
        .expect("fake child invocation evidence is readable");
    assert_eq!(invocations.lines().count(), 1);
    drop(input_a);
    drop(output_a);
    client_a.terminate();
    daemon.terminate();
}

fn daemon_operation(response: &serde_json::Value) -> aiop::contract::control::Operation {
    let text = response["result"]["content"][0]["text"]
        .as_str()
        .expect("MCP operation response contains daemon JSON");
    match serde_json::from_str::<DaemonResponse>(text).expect("daemon operation response parses") {
        DaemonResponse::Operation(operation) => operation,
        other => panic!("MCP operation request returned another response: {other:?}"),
    }
}

fn daemon_diagnostics(
    response: &serde_json::Value,
) -> aiop::contract::control::OperationDiagnostics {
    let text = response["result"]["content"][0]["text"]
        .as_str()
        .expect("MCP diagnostic response contains daemon JSON");
    match serde_json::from_str::<DaemonResponse>(text).expect("daemon diagnostic response parses") {
        DaemonResponse::OperationDiagnostics(snapshot) => snapshot,
        other => panic!("MCP diagnostic request returned another response: {other:?}"),
    }
}
