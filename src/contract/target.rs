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
}
