// Copyright 2026 Yuriy Krasilnikov
// SPDX-License-Identifier: Apache-2.0

//! Provider-neutral execution contract owned by Target Execution.

use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::AtomicBool,
        mpsc::{Receiver, Sender},
    },
};

use uuid::Uuid;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TargetOperationId(pub Uuid);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TargetSessionId(pub Uuid);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TargetTurnId(pub Uuid);

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TargetIntent {
    New,
    ResumeExact { session_id: TargetSessionId },
}

#[derive(Debug)]
pub struct TargetCommand {
    pub operation_id: TargetOperationId,
    pub working_directory: PathBuf,
    pub executable: PathBuf,
    pub expected_model: String,
    pub intent: TargetIntent,
    pub session_id: TargetSessionId,
    pub prompt: String,
    pub cancel_requested: Arc<AtomicBool>,
    pub launch_report: Sender<TargetLaunch>,
    pub running_permission: Receiver<Result<(), String>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TargetLaunch {
    Launched,
    SpawnFailed(String),
    CancelledBeforeLaunch,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TargetSuccess {
    pub result: String,
    pub observed_session_id: TargetSessionId,
    pub observed_model: String,
    pub observed_version: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TargetOutcome {
    SpawnFailed(String),
    Success(TargetSuccess),
    Failed(String),
    Cancelled(String),
    CancelledBeforeLaunch(String),
    Indeterminate(String),
}

pub trait TargetPort: Send + Sync {
    fn execute(&self, command: TargetCommand) -> TargetOutcome;
    fn cancel(&self, operation_id: TargetOperationId) -> Result<(), String>;
    fn start_live(
        &self,
        start: TargetLiveStart,
        observations: Sender<TargetLiveObservation>,
    ) -> Result<(), TargetLiveStartError>;
    fn send_live(
        &self,
        operation_id: TargetOperationId,
        turn: TargetLiveTurn,
    ) -> Result<(), String>;
    fn stop_live(
        &self,
        operation_id: TargetOperationId,
        stop: TargetLiveStop,
    ) -> Result<(), String>;
}

/// One Control-admitted turn, identified by the UUID sent to the provider.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TargetLiveTurn {
    pub turn_id: TargetTurnId,
    pub position: u64,
    pub prompt: String,
}

/// The provider-neutral launch facts for one persistent structured conversation.
#[derive(Debug)]
pub struct TargetLiveStart {
    pub operation_id: TargetOperationId,
    pub working_directory: PathBuf,
    pub executable: PathBuf,
    pub expected_model: String,
    pub intent: TargetIntent,
    pub session_id: TargetSessionId,
    pub first_turn: TargetLiveTurn,
    /// Control sends permission only after the durable Running transition.
    pub running_permission: Receiver<Result<(), String>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TargetLiveStop {
    Graceful { through_position: u64 },
    Cancel,
}

/// Evidence about whether a failed live start left a possible session writer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TargetLiveStartError {
    /// No direct child was created, so no writer can own the session.
    NoWriter(String),
    /// Startup cleanup observed the direct child exit.
    CleanupProvenExited(String),
    /// Startup reached a child or writer state whose exit was not proved.
    CleanupUnproven(String),
}

impl TargetLiveStartError {
    pub fn message(&self) -> &str {
        match self {
            Self::NoWriter(message)
            | Self::CleanupProvenExited(message)
            | Self::CleanupUnproven(message) => message,
        }
    }
}

impl std::fmt::Display for TargetLiveStartError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.message())
    }
}

/// Correlated provider facts. Control owns their durable interpretation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TargetLiveObservation {
    Initialized {
        session_id: TargetSessionId,
        model: String,
        version: Option<String>,
    },
    TurnQueued {
        turn_id: TargetTurnId,
    },
    TurnStarted {
        turn_id: TargetTurnId,
    },
    TurnAcknowledged {
        turn_id: TargetTurnId,
    },
    AssistantTextDelta {
        turn_id: TargetTurnId,
        text: String,
    },
    TurnCompleted {
        turn_id: TargetTurnId,
        result: String,
    },
    TurnFailed {
        turn_id: TargetTurnId,
        message: String,
    },
    Cancelled,
    Exited,
    Failed(String),
    Indeterminate(String),
    /// Target could not prove that the direct writer exited during cleanup.
    UnclassifiedWriter(String),
}
