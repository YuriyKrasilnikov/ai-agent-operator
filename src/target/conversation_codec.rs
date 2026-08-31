// Copyright 2026 Yuriy Krasilnikov
// SPDX-License-Identifier: Apache-2.0

//! Validates UUID-correlated structured provider events into neutral observations.

use std::collections::HashMap;

use serde_json::Value;

use crate::contract::target::{
    TargetLiveObservation, TargetLiveTurn, TargetSessionId, TargetTurnId,
};

/// A provider terminal failure is distinct from ambiguous protocol evidence.
#[derive(Debug)]
pub(crate) enum CodecError {
    Failed {
        turn_id: TargetTurnId,
        message: String,
    },
    Indeterminate(String),
}

impl CodecError {
    fn indeterminate(message: impl Into<String>) -> Self {
        Self::Indeterminate(message.into())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TurnPhase {
    AwaitingQueue,
    Queued,
    Started,
    ResultObserved,
    Completed,
}

#[derive(Default)]
pub(crate) struct Codec {
    initialized: Option<(TargetSessionId, String, Option<String>)>,
    turns: HashMap<TargetTurnId, (TargetLiveTurn, TurnPhase, Option<String>)>,
}

impl Codec {
    pub(crate) fn register_turn(&mut self, turn: TargetLiveTurn) -> Result<(), CodecError> {
        if self
            .turns
            .insert(turn.turn_id, (turn, TurnPhase::AwaitingQueue, None))
            .is_some()
        {
            return Err(CodecError::indeterminate(
                "provider input reported a duplicate turn UUID",
            ));
        }
        Ok(())
    }

    pub(crate) fn observe(
        &mut self,
        line: &str,
        expected_model: &str,
        intended_session: TargetSessionId,
    ) -> Result<Vec<TargetLiveObservation>, CodecError> {
        let event: Value = serde_json::from_str(line).map_err(|error| {
            CodecError::indeterminate(format!("Claude emitted invalid structured JSON: {error}"))
        })?;
        match event.get("type").and_then(Value::as_str) {
            Some("system") if event.get("subtype").and_then(Value::as_str) == Some("init") => {
                self.init(&event, expected_model, intended_session)
            }
            Some("command_lifecycle") => self.lifecycle(&event, intended_session),
            Some("user") => self.user(&event, intended_session),
            Some("assistant") => self.assistant(&event, intended_session),
            Some("stream_event") => self.stream_event(&event, intended_session),
            Some("result") => self.result(&event, intended_session),
            Some(_) | None => Ok(Vec::new()),
        }
    }

    pub(crate) fn all_completed(&self) -> bool {
        self.turns
            .values()
            .all(|(_, phase, _)| *phase == TurnPhase::Completed)
    }

    fn init(
        &mut self,
        event: &Value,
        expected_model: &str,
        intended_session: TargetSessionId,
    ) -> Result<Vec<TargetLiveObservation>, CodecError> {
        let session_id = read_session(event, "init", "session_id")?;
        let model = read_string(event, "init", "model")?;
        let version = event
            .get("claude_code_version")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        if session_id != intended_session {
            return Err(CodecError::indeterminate(format!(
                "Claude init session mismatch: intended {}; observed {}",
                intended_session.0, session_id.0
            )));
        }
        if model != expected_model {
            return Err(CodecError::indeterminate(format!(
                "Claude init model mismatch: intended {}; observed {model}",
                expected_model
            )));
        }
        match &self.initialized {
            Some((prior_session, prior_model, prior_version))
                if *prior_session == session_id
                    && prior_model == &model
                    && prior_version == &version => {}
            Some(_) => {
                return Err(CodecError::indeterminate(
                    "Claude init contradicted prior live conversation identity",
                ));
            }
            None => self.initialized = Some((session_id, model.clone(), version.clone())),
        }
        Ok(vec![TargetLiveObservation::Initialized {
            session_id,
            model,
            version,
        }])
    }

    fn lifecycle(
        &mut self,
        event: &Value,
        intended_session: TargetSessionId,
    ) -> Result<Vec<TargetLiveObservation>, CodecError> {
        validate_session(event, "command_lifecycle", intended_session)?;
        let turn_id = read_turn(event, "command_lifecycle", "command_uuid")?;
        let state = read_string(event, "command_lifecycle", "state")?;
        if state == "started" && self.has_active_turn() {
            return Err(CodecError::indeterminate(
                "Claude started a live turn while another turn was active",
            ));
        }
        let (_, phase, result) = self.turn(turn_id)?;
        match (*phase, state.as_str()) {
            (TurnPhase::AwaitingQueue, "queued") => {
                *phase = TurnPhase::Queued;
                Ok(vec![TargetLiveObservation::TurnQueued { turn_id }])
            }
            (TurnPhase::Queued, "started") => {
                *phase = TurnPhase::Started;
                Ok(vec![TargetLiveObservation::TurnStarted { turn_id }])
            }
            (TurnPhase::ResultObserved, "completed") => {
                let result = result.clone().ok_or_else(|| {
                    CodecError::indeterminate(
                        "Claude completed a live turn without a matching result",
                    )
                })?;
                *phase = TurnPhase::Completed;
                Ok(vec![TargetLiveObservation::TurnCompleted {
                    turn_id,
                    result,
                }])
            }
            (TurnPhase::AwaitingQueue, "started" | "completed")
            | (TurnPhase::Queued, "queued" | "completed")
            | (TurnPhase::Started, "queued" | "started" | "completed")
            | (TurnPhase::ResultObserved, "queued" | "started")
            | (TurnPhase::Completed, "queued" | "started" | "completed") => {
                Err(CodecError::indeterminate(format!(
                    "Claude lifecycle {state} contradicted turn {turn_id:?} phase"
                )))
            }
            (_, _) => Err(CodecError::indeterminate(format!(
                "Claude lifecycle state is unsupported: {state}"
            ))),
        }
    }

    fn user(
        &mut self,
        event: &Value,
        intended_session: TargetSessionId,
    ) -> Result<Vec<TargetLiveObservation>, CodecError> {
        self.require_initialized()?;
        validate_session(event, "replayed user", intended_session)?;
        match event.get("isReplay") {
            Some(Value::Bool(true)) => self.replayed_user(event),
            None => self.tool_result(event),
            Some(Value::Null)
            | Some(Value::Bool(false))
            | Some(Value::Number(_))
            | Some(Value::String(_))
            | Some(Value::Array(_))
            | Some(Value::Object(_)) => Err(CodecError::indeterminate(
                "Claude user event is neither an exact caller replay nor a provider tool result",
            )),
        }
    }

    fn replayed_user(&mut self, event: &Value) -> Result<Vec<TargetLiveObservation>, CodecError> {
        let turn_id = read_turn(event, "replayed user", "uuid")?;
        let prompt = read_user_prompt(event)?;
        let (turn, phase, _) = self.turn(turn_id)?;
        if prompt != turn.prompt {
            return Err(CodecError::indeterminate(
                "Claude replayed user prompt differed from the admitted turn",
            ));
        }
        match phase {
            TurnPhase::Queued | TurnPhase::Started => {
                Ok(vec![TargetLiveObservation::TurnAcknowledged { turn_id }])
            }
            TurnPhase::AwaitingQueue | TurnPhase::ResultObserved | TurnPhase::Completed => Err(
                CodecError::indeterminate("Claude replayed user prompt outside the turn lifecycle"),
            ),
        }
    }

    fn tool_result(&mut self, event: &Value) -> Result<Vec<TargetLiveObservation>, CodecError> {
        let provider_uuid = read_turn(event, "tool result", "uuid")?;
        if self.turns.contains_key(&provider_uuid) {
            return Err(CodecError::indeterminate(
                "Claude tool result reused an admitted caller turn UUID",
            ));
        }
        self.unique_started_turn()?;
        validate_tool_result(event)?;
        Ok(Vec::new())
    }

    fn stream_event(
        &mut self,
        event: &Value,
        intended_session: TargetSessionId,
    ) -> Result<Vec<TargetLiveObservation>, CodecError> {
        if !is_text_delta(event) {
            return Ok(Vec::new());
        }
        self.require_initialized()?;
        validate_session(event, "partial assistant", intended_session)?;
        let turn_id = self.unique_started_turn()?;
        let (_, phase, _) = self.turn(turn_id)?;
        if *phase != TurnPhase::Started {
            return Err(CodecError::indeterminate(
                "Claude assistant text was not correlated to a started turn",
            ));
        }
        let text = read_assistant_text(event)?;
        if text.is_empty() {
            return Ok(Vec::new());
        }
        Ok(vec![TargetLiveObservation::AssistantTextDelta {
            turn_id,
            text,
        }])
    }

    fn assistant(
        &mut self,
        event: &Value,
        intended_session: TargetSessionId,
    ) -> Result<Vec<TargetLiveObservation>, CodecError> {
        self.require_initialized()?;
        validate_session(event, "assistant", intended_session)?;
        self.unique_started_turn()?;
        Ok(Vec::new())
    }

    fn result(
        &mut self,
        event: &Value,
        intended_session: TargetSessionId,
    ) -> Result<Vec<TargetLiveObservation>, CodecError> {
        self.require_initialized()?;
        let session_id = read_session(event, "result", "session_id")?;
        if session_id != intended_session {
            return Err(CodecError::indeterminate(format!(
                "Claude result session mismatch: intended {}; observed {}",
                intended_session.0, session_id.0
            )));
        }
        let turn_id = self.unique_started_turn()?;
        let (_, phase, result) = self.turn(turn_id)?;
        if *phase != TurnPhase::Started {
            return Err(CodecError::indeterminate(
                "Claude result was not correlated to a started turn",
            ));
        }
        match event.get("is_error").and_then(Value::as_bool) {
            Some(false) => {
                let text = read_string(event, "result", "result")?;
                *result = Some(text);
                *phase = TurnPhase::ResultObserved;
                Ok(Vec::new())
            }
            Some(true) => Err(CodecError::Failed {
                turn_id,
                message: read_string(event, "failed result", "result")?,
            }),
            None => Err(CodecError::indeterminate("Claude result omitted is_error")),
        }
    }

    fn turn(
        &mut self,
        turn_id: TargetTurnId,
    ) -> Result<&mut (TargetLiveTurn, TurnPhase, Option<String>), CodecError> {
        self.turns.get_mut(&turn_id).ok_or_else(|| {
            CodecError::indeterminate("Claude event referenced an unknown live turn UUID")
        })
    }

    fn unique_started_turn(&self) -> Result<TargetTurnId, CodecError> {
        let mut active = None;
        for (turn_id, (_, phase, _)) in &self.turns {
            if *phase == TurnPhase::Started {
                match active {
                    Some(_) => {
                        return Err(CodecError::indeterminate(
                            "Claude provider event had more than one started turn",
                        ));
                    }
                    None => active = Some(*turn_id),
                }
            }
        }
        active.ok_or_else(|| {
            CodecError::indeterminate("Claude provider event had no uniquely started turn")
        })
    }

    fn has_active_turn(&self) -> bool {
        self.turns
            .values()
            .any(|(_, phase, _)| matches!(phase, TurnPhase::Started | TurnPhase::ResultObserved))
    }

    fn require_initialized(&self) -> Result<(), CodecError> {
        if self.initialized.is_some() {
            Ok(())
        } else {
            Err(CodecError::indeterminate(
                "Claude emitted a live turn event before exact initialization",
            ))
        }
    }
}

fn read_session(
    event: &Value,
    event_name: &str,
    field: &str,
) -> Result<TargetSessionId, CodecError> {
    read_string(event, event_name, field)?
        .parse()
        .map(TargetSessionId)
        .map_err(|error| {
            CodecError::indeterminate(format!(
                "Claude {event_name} {field} was not a UUID: {error}"
            ))
        })
}

fn validate_session(
    event: &Value,
    event_name: &str,
    intended_session: TargetSessionId,
) -> Result<(), CodecError> {
    let observed = read_session(event, event_name, "session_id")?;
    if observed == intended_session {
        Ok(())
    } else {
        Err(CodecError::indeterminate(format!(
            "Claude {event_name} session mismatch: intended {}; observed {}",
            intended_session.0, observed.0
        )))
    }
}

fn read_turn(event: &Value, event_name: &str, field: &str) -> Result<TargetTurnId, CodecError> {
    read_string(event, event_name, field)?
        .parse()
        .map(TargetTurnId)
        .map_err(|error| {
            CodecError::indeterminate(format!(
                "Claude {event_name} {field} was not a UUID: {error}"
            ))
        })
}

fn read_string(event: &Value, event_name: &str, field: &str) -> Result<String, CodecError> {
    event
        .get(field)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| CodecError::indeterminate(format!("Claude {event_name} omitted {field}")))
}

fn read_user_prompt(event: &Value) -> Result<String, CodecError> {
    let content = user_content(event, "Claude replayed user message was malformed")?;
    let [block] = content.as_slice() else {
        return Err(CodecError::indeterminate(
            "Claude replayed user message must contain exactly one text block",
        ));
    };
    let block = block.as_object().ok_or_else(|| {
        CodecError::indeterminate("Claude replayed user text block was malformed")
    })?;
    if block.get("type").and_then(Value::as_str) != Some("text") {
        return Err(CodecError::indeterminate(
            "Claude replayed user content block was not text",
        ));
    }
    block
        .get("text")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| CodecError::indeterminate("Claude replayed user text was absent"))
}

fn validate_tool_result(event: &Value) -> Result<(), CodecError> {
    let content = user_content(event, "Claude tool result message was malformed")?;
    let [block] = content.as_slice() else {
        return Err(CodecError::indeterminate(
            "Claude tool result message must contain exactly one tool_result block",
        ));
    };
    let block = block
        .as_object()
        .ok_or_else(|| CodecError::indeterminate("Claude tool result block was malformed"))?;
    if block.get("type").and_then(Value::as_str) != Some("tool_result") {
        return Err(CodecError::indeterminate(
            "Claude provider user event was not a tool_result",
        ));
    }
    block
        .get("content")
        .and_then(Value::as_str)
        .ok_or_else(|| CodecError::indeterminate("Claude tool result content was not a string"))?;
    match block.get("is_error") {
        None | Some(Value::Bool(true)) => {}
        Some(Value::Null)
        | Some(Value::Bool(false))
        | Some(Value::Number(_))
        | Some(Value::String(_))
        | Some(Value::Array(_))
        | Some(Value::Object(_)) => {
            return Err(CodecError::indeterminate(
                "Claude tool result is_error must be absent or true",
            ));
        }
    }
    block
        .get("tool_use_id")
        .and_then(Value::as_str)
        .ok_or_else(|| CodecError::indeterminate("Claude tool result omitted tool_use_id"))?;
    Ok(())
}

fn user_content<'a>(
    event: &'a Value,
    malformed_message: &str,
) -> Result<&'a Vec<Value>, CodecError> {
    event
        .get("message")
        .and_then(Value::as_object)
        .and_then(|message| message.get("content"))
        .and_then(Value::as_array)
        .ok_or_else(|| CodecError::indeterminate(malformed_message))
}

fn read_assistant_text(event: &Value) -> Result<String, CodecError> {
    event
        .get("event")
        .and_then(Value::as_object)
        .and_then(|stream_event| stream_event.get("delta"))
        .and_then(Value::as_object)
        .and_then(|delta| delta.get("text"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| {
            CodecError::indeterminate("Claude partial message omitted a text content-block delta")
        })
}

fn is_text_delta(event: &Value) -> bool {
    event
        .get("event")
        .and_then(Value::as_object)
        .is_some_and(|stream_event| {
            stream_event.get("type").and_then(Value::as_str) == Some("content_block_delta")
                && stream_event
                    .get("delta")
                    .and_then(Value::as_object)
                    .and_then(|delta| delta.get("type"))
                    .and_then(Value::as_str)
                    == Some("text_delta")
        })
}
