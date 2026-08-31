// Copyright 2026 Yuriy Krasilnikov
// SPDX-License-Identifier: Apache-2.0

//! Interprets only the provider evidence that establishes a terminal outcome.

use serde_json::Value;

use crate::contract::target::{TargetCommand, TargetDiagnostic, TargetSessionId, TargetSuccess};

#[derive(Debug)]
pub enum InterpretationError {
    ProviderFailure(String),
    Ambiguous(String),
}

#[derive(Default)]
pub struct Evidence {
    init_session: Option<TargetSessionId>,
    init_model: Option<String>,
    version: Option<String>,
    result: Option<Result<String, String>>,
}

impl Evidence {
    pub fn observe(
        &mut self,
        line: &str,
        command: &TargetCommand,
    ) -> Result<(), InterpretationError> {
        let event: Value = serde_json::from_str(line).map_err(|error| {
            InterpretationError::Ambiguous(format!("Claude emitted invalid stream JSON: {error}"))
        })?;
        if let Some(diagnostic) = diagnostic(&event)
            && command.diagnostics.send(diagnostic).is_err()
        {
            // Control has already retained the state failure and signalled the
            // exact runtime gate. Terminal classification remains its concern.
        }
        match (
            event.get("type").and_then(Value::as_str),
            event.get("subtype").and_then(Value::as_str),
        ) {
            (Some("system"), Some("init")) => self.init(event, command),
            (Some("result"), _) => self.result(event, command),
            _ => Ok(()),
        }
    }

    pub fn success(self, command: &TargetCommand) -> Result<TargetSuccess, InterpretationError> {
        let result = self.result.ok_or_else(|| {
            InterpretationError::Ambiguous(
                "direct Claude child exited without a result event".to_owned(),
            )
        })?;
        let result = result.map_err(InterpretationError::ProviderFailure)?;
        ensure_matching_session(self.init_session, command)?;
        ensure_matching_model(self.init_model.as_deref(), command)?;
        Ok(TargetSuccess {
            result,
            observed_session_id: command.session_id,
            observed_model: command.expected_model.clone(),
            observed_version: self.version,
        })
    }

    fn init(&mut self, event: Value, command: &TargetCommand) -> Result<(), InterpretationError> {
        let session = read_session(&event, "init")?;
        let model = read_model(&event, "init")?;
        if session != command.session_id {
            return Err(InterpretationError::Ambiguous(format!(
                "Claude init session mismatch: intended {}; observed {}",
                command.session_id.0, session.0
            )));
        }
        if model != command.expected_model {
            return Err(InterpretationError::Ambiguous(format!(
                "Claude init model mismatch: intended {}; observed {model}",
                command.expected_model
            )));
        }
        self.init_session = Some(session);
        self.init_model = Some(model);
        self.version = event
            .get("claude_code_version")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        Ok(())
    }

    fn result(&mut self, event: Value, command: &TargetCommand) -> Result<(), InterpretationError> {
        let session = read_session(&event, "result")?;
        if session != command.session_id {
            return Err(InterpretationError::Ambiguous(
                "Claude result session did not match the intended session".to_owned(),
            ));
        }
        match event.get("is_error").and_then(Value::as_bool) {
            Some(true) => {
                let message = match event.get("result").and_then(Value::as_str) {
                    Some(message) if !message.is_empty() => message.to_owned(),
                    Some(_) => "Claude terminal failure result was empty".to_owned(),
                    None => "Claude terminal failure omitted result text".to_owned(),
                };
                return Err(InterpretationError::ProviderFailure(message));
            }
            Some(false) => {
                let value = event.get("result").and_then(Value::as_str).ok_or_else(|| {
                    InterpretationError::Ambiguous("Claude result omitted review text".to_owned())
                })?;
                self.result = Some(Ok(value.to_owned()));
            }
            None => {
                return Err(InterpretationError::Ambiguous(
                    "Claude result omitted is_error".to_owned(),
                ));
            }
        }
        Ok(())
    }
}

fn diagnostic(event: &Value) -> Option<TargetDiagnostic> {
    match (
        event.get("type").and_then(Value::as_str),
        event.get("subtype").and_then(Value::as_str),
    ) {
        (Some("system"), Some("api_retry")) => Some(retry_diagnostic(event)),
        (Some("assistant"), _)
            if event.get("is_api_error_message").and_then(Value::as_bool) == Some(true) =>
        {
            match event.get("error") {
                Some(Value::String(error)) if error == "authentication_failed" => {
                    Some(TargetDiagnostic::AuthenticationFailed)
                }
                _ => Some(TargetDiagnostic::DiagnosticUnclassified),
            }
        }
        _ => None,
    }
}

fn retry_diagnostic(event: &Value) -> TargetDiagnostic {
    let attempt = event.get("attempt").and_then(Value::as_u64);
    let max_retries = event.get("max_retries").and_then(Value::as_u64);
    let retry_delay_ms = event.get("retry_delay_ms").and_then(Value::as_u64);
    match (attempt, max_retries, retry_delay_ms) {
        (Some(attempt), Some(max_retries), Some(retry_delay_ms))
            if attempt > 0 && max_retries > 0 && attempt <= max_retries =>
        {
            TargetDiagnostic::ProviderRetrying {
                attempt,
                max_retries,
                retry_delay_ms,
            }
        }
        _ => TargetDiagnostic::DiagnosticUnclassified,
    }
}

fn ensure_matching_session(
    observed: Option<TargetSessionId>,
    command: &TargetCommand,
) -> Result<(), InterpretationError> {
    match observed {
        Some(session) if session == command.session_id => Ok(()),
        Some(session) => Err(InterpretationError::Ambiguous(format!(
            "Claude init session mismatch: intended {}; observed {}",
            command.session_id.0, session.0
        ))),
        None => Err(InterpretationError::Ambiguous(format!(
            "Claude init session evidence is missing: intended {}",
            command.session_id.0
        ))),
    }
}

fn ensure_matching_model(
    observed: Option<&str>,
    command: &TargetCommand,
) -> Result<(), InterpretationError> {
    match observed {
        Some(model) if model == command.expected_model => Ok(()),
        Some(model) => Err(InterpretationError::Ambiguous(format!(
            "Claude init model mismatch: intended {}; observed {model}",
            command.expected_model
        ))),
        None => Err(InterpretationError::Ambiguous(format!(
            "Claude init model evidence is missing: intended {}",
            command.expected_model
        ))),
    }
}

fn read_session(event: &Value, event_name: &str) -> Result<TargetSessionId, InterpretationError> {
    let value = event
        .get("session_id")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            InterpretationError::Ambiguous(format!("Claude {event_name} omitted session_id"))
        })?;
    value.parse().map(TargetSessionId).map_err(|error| {
        InterpretationError::Ambiguous(format!(
            "Claude {event_name} session_id was not a UUID: {error}"
        ))
    })
}

fn read_model(event: &Value, event_name: &str) -> Result<String, InterpretationError> {
    event
        .get("model")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| InterpretationError::Ambiguous(format!("Claude {event_name} omitted model")))
}
