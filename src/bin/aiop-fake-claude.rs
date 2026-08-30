// Copyright 2026 Yuriy Krasilnikov
// SPDX-License-Identifier: Apache-2.0

//! Deterministic Claude stream fixture used only by acceptance tests.

use std::{
    env,
    io::{Read, Write},
    path::Path,
    process::ExitCode,
    thread,
    time::Duration,
};

use serde_json::json;

const FIXTURE_LARGE_RESULT_BYTES: usize = 32 * 1024;
const PENDING_INPUT_CANCELLATION_MARKER: &str = "__fixture_pending_input_cancel__";
const NONZERO_WITHOUT_INPUT_MARKER: &str = "__fixture_nonzero_without_input__";

enum PromptInput {
    Complete(String),
    PendingInputCancellation,
    NonzeroWithoutInput,
}

impl PromptInput {
    fn matches_marker(&self, value: &str) -> bool {
        match self {
            Self::Complete(prompt) => prompt.starts_with(value),
            Self::PendingInputCancellation => value == PENDING_INPUT_CANCELLATION_MARKER,
            Self::NonzeroWithoutInput => value == NONZERO_WITHOUT_INPUT_MARKER,
        }
    }

    fn recorded(&self) -> &str {
        match self {
            Self::Complete(prompt) => prompt,
            Self::PendingInputCancellation => PENDING_INPUT_CANCELLATION_MARKER,
            Self::NonzeroWithoutInput => NONZERO_WITHOUT_INPUT_MARKER,
        }
    }
}

fn main() -> ExitCode {
    let arguments: Vec<String> = env::args().skip(1).collect();
    if arguments.as_slice() == ["--fixture_hold_stdout"] {
        thread::sleep(Duration::from_millis(200));
        return ExitCode::SUCCESS;
    }
    let invocation = match Invocation::parse(&arguments) {
        Ok(invocation) => invocation,
        Err(error) => {
            eprintln!("fake Claude: {error}");
            return ExitCode::FAILURE;
        }
    };
    let prompt = match read_prompt() {
        Ok(prompt) => prompt,
        Err(error) => {
            eprintln!("fake Claude: {error}");
            return ExitCode::FAILURE;
        }
    };
    if let Err(error) = record_invocation(&arguments, prompt.recorded()) {
        eprintln!("fake Claude: {error}");
        return ExitCode::FAILURE;
    }
    if prompt.matches_marker(PENDING_INPUT_CANCELLATION_MARKER) {
        if write_event(json!({"type":"system","subtype":"init","session_id":invocation.session,"model":invocation.model,"claude_code_version":"fixture-1"})).is_err() {
            return ExitCode::FAILURE;
        }
        thread::sleep(Duration::from_secs(2));
        return ExitCode::SUCCESS;
    }
    if prompt.matches_marker(NONZERO_WITHOUT_INPUT_MARKER) {
        if write_event(json!({"type":"system","subtype":"init","session_id":invocation.session,"model":invocation.model,"claude_code_version":"fixture-1"})).is_err() {
            return ExitCode::FAILURE;
        }
        return ExitCode::FAILURE;
    }
    let prompt = match prompt {
        PromptInput::Complete(prompt) => prompt,
        PromptInput::PendingInputCancellation | PromptInput::NonzeroWithoutInput => {
            return ExitCode::FAILURE;
        }
    };
    if prompt == "__fixture_long_runtime__" {
        thread::sleep(Duration::from_millis(50));
    }
    if prompt.contains("__fixture_early_large_output__") {
        let output = "x".repeat(128 * 1024);
        if write_event(json!({"type":"assistant","text":output})).is_err() {
            return ExitCode::FAILURE;
        }
    }
    let observed_session = if prompt == "__fixture_session_mismatch__" {
        "00000000-0000-4000-8000-000000000000".to_owned()
    } else {
        invocation.session.clone()
    };
    let observed_model = if prompt == "__fixture_model_mismatch__" {
        "other-model".to_owned()
    } else {
        invocation.model.clone()
    };
    if write_event(json!({"type":"system","subtype":"init","session_id":observed_session,"model":observed_model,"claude_code_version":"fixture-1"})).is_err() { return ExitCode::FAILURE; }
    if prompt == "__fixture_unknown_event__"
        && write_event(json!({"type":"telemetry","unknown_field":"retained as nonterminal"}))
            .is_err()
    {
        return ExitCode::FAILURE;
    }
    if prompt == "__fixture_terminal_failure__" {
        eprintln!("fixture provider diagnostic");
        if write_event(json!({"type":"result","is_error":true,"session_id":invocation.session,"result":"fixture provider rejected review"})).is_err() { return ExitCode::FAILURE; }
        thread::sleep(Duration::from_secs(2));
        return ExitCode::SUCCESS;
    }
    if prompt == "__fixture_terminal_failure_without_text__" {
        if write_event(json!({"type":"result","is_error":true,"session_id":invocation.session}))
            .is_err()
        {
            return ExitCode::FAILURE;
        }
        return ExitCode::SUCCESS;
    }
    if prompt == "__fixture_exit_without_result__" {
        return ExitCode::SUCCESS;
    }
    if prompt == "__fixture_hold_for_cancel__" {
        thread::sleep(Duration::from_secs(2));
    }
    if prompt == "__fixture_split_utf8_result__" {
        return match write_split_event(json!({
            "type":"result",
            "is_error":false,
            "session_id":invocation.session,
            "result":"split UTF-8 review: Привет 😀"
        })) {
            Ok(()) => ExitCode::SUCCESS,
            Err(_) => ExitCode::FAILURE,
        };
    }
    if prompt == "__fixture_result_then_fragment__" {
        if write_event(json!({
            "type":"result",
            "is_error":false,
            "session_id":invocation.session,
            "result":"terminal result before fragment"
        }))
        .is_err()
        {
            return ExitCode::FAILURE;
        }
        return match write_stdout_bytes(b"{\"unterminated\":") {
            Ok(()) => ExitCode::SUCCESS,
            Err(_) => ExitCode::FAILURE,
        };
    }
    let result = if prompt == "__fixture_large_result__" {
        "r".repeat(FIXTURE_LARGE_RESULT_BYTES)
    } else if invocation.resume {
        match resumed_result(&invocation.session, &prompt) {
            Ok(result) => result,
            Err(error) => {
                eprintln!("fake Claude: {error}");
                return ExitCode::FAILURE;
            }
        }
    } else {
        format!("complete review: {prompt}")
    };
    if write_event(
        json!({"type":"result","is_error":false,"session_id":invocation.session,"result":result}),
    )
    .is_err()
    {
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

struct Invocation {
    session: String,
    model: String,
    resume: bool,
}

impl Invocation {
    fn parse(arguments: &[String]) -> Result<Self, String> {
        let prefix = [
            "--print",
            "--verbose",
            "--output-format",
            "stream-json",
            "--model",
            "opus",
            "--effort",
            "max",
            "--permission-mode",
            "dontAsk",
            "--restricted",
            "--safe-mode",
            "--tools",
            "Read,Glob,Grep",
            "--allowedTools",
            "Read,Glob,Grep",
        ];
        if arguments.len() != prefix.len() + 2 {
            return Err("Claude argv did not have the exact supported length".to_owned());
        }
        if arguments[..prefix.len()]
            .iter()
            .map(String::as_str)
            .ne(prefix)
        {
            return Err("Claude argv did not match the exact Opus read-only profile".to_owned());
        }
        let session_flag = &arguments[prefix.len()];
        let resume = match session_flag.as_str() {
            "--session-id" => false,
            "--resume" => true,
            _ => return Err("Claude argv did not select new or exact resume".to_owned()),
        };
        let session = arguments[prefix.len() + 1].clone();
        Ok(Self {
            session,
            model: "opus".to_owned(),
            resume,
        })
    }
}

fn read_prompt() -> Result<PromptInput, String> {
    let mut stdin = std::io::stdin().lock();
    let required_prefix = PENDING_INPUT_CANCELLATION_MARKER
        .len()
        .max(NONZERO_WITHOUT_INPUT_MARKER.len());
    let mut prefix = Vec::new();
    let mut buffer = [0_u8; 256];
    while prefix.len() < required_prefix {
        let read = stdin.read(&mut buffer).map_err(|error| error.to_string())?;
        if read == 0 {
            break;
        }
        prefix.extend_from_slice(&buffer[..read]);
    }
    if prefix.starts_with(PENDING_INPUT_CANCELLATION_MARKER.as_bytes()) {
        return Ok(PromptInput::PendingInputCancellation);
    }
    if prefix.starts_with(NONZERO_WITHOUT_INPUT_MARKER.as_bytes()) {
        return Ok(PromptInput::NonzeroWithoutInput);
    }
    let mut prompt = String::from_utf8(prefix).map_err(|error| error.to_string())?;
    stdin
        .read_to_string(&mut prompt)
        .map_err(|error| error.to_string())?;
    if prompt.is_empty() {
        return Err("prompt stdin was empty".to_owned());
    }
    Ok(PromptInput::Complete(prompt))
}
fn write_event(event: serde_json::Value) -> Result<(), String> {
    let line = serde_json::to_string(&event).map_err(|error| error.to_string())?;
    let mut stdout = std::io::stdout().lock();
    stdout
        .write_all(line.as_bytes())
        .map_err(|error| error.to_string())?;
    stdout.write_all(b"\n").map_err(|error| error.to_string())?;
    stdout.flush().map_err(|error| error.to_string())
}
fn write_split_event(event: serde_json::Value) -> Result<(), String> {
    let line = serde_json::to_string(&event).map_err(|error| error.to_string())?;
    let bytes = line.as_bytes();
    let split = bytes
        .windows("😀".len())
        .position(|window| window == "😀".as_bytes())
        .ok_or_else(|| "fixture result did not contain the intended UTF-8 character".to_owned())?
        + 1;
    let mut stdout = std::io::stdout().lock();
    stdout
        .write_all(&bytes[..split])
        .map_err(|error| error.to_string())?;
    stdout.flush().map_err(|error| error.to_string())?;
    thread::sleep(Duration::from_millis(30));
    stdout
        .write_all(&bytes[split..])
        .map_err(|error| error.to_string())?;
    stdout.write_all(b"\n").map_err(|error| error.to_string())?;
    stdout.flush().map_err(|error| error.to_string())
}
fn write_stdout_bytes(bytes: &[u8]) -> Result<(), String> {
    let mut stdout = std::io::stdout().lock();
    stdout.write_all(bytes).map_err(|error| error.to_string())?;
    stdout.flush().map_err(|error| error.to_string())
}
fn record_invocation(arguments: &[String], prompt: &str) -> Result<(), String> {
    let path = Path::new(".aiop-fake-invocations.jsonl");
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|error| error.to_string())?;
    let record = json!({"argv":arguments,"prompt":prompt});
    writeln!(file, "{record}").map_err(|error| error.to_string())
}
fn resumed_result(session: &str, prompt: &str) -> Result<String, String> {
    let records = std::fs::read_to_string(".aiop-fake-invocations.jsonl")
        .map_err(|error| format!("invocation evidence could not be read: {error}"))?;
    for line in records.lines() {
        let record: serde_json::Value = serde_json::from_str(line)
            .map_err(|error| format!("invocation evidence was invalid JSON: {error}"))?;
        let same_session = record
            .get("argv")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|argv| {
                argv.iter()
                    .any(|argument| argument.as_str() == Some(session))
            });
        let prior_prompt = record
            .get("prompt")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| "invocation evidence omitted prompt".to_owned())?;
        if same_session && prior_prompt != prompt {
            return Ok(format!(
                "resume follows prior content: {prior_prompt}; {prompt}"
            ));
        }
    }
    Ok(format!("resume has no prior content for session: {prompt}"))
}
