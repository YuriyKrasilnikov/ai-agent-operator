// Copyright 2026 Yuriy Krasilnikov
// SPDX-License-Identifier: Apache-2.0

//! Operation Control's public domain and durable-state contract.

use std::{
    fmt::{Display, Formatter},
    marker::PhantomData,
    path::PathBuf,
};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct ProjectId(String);

impl ProjectId {
    pub fn new(value: String) -> Result<Self, OperatorError> {
        if value.is_empty() {
            return Err(OperatorError::InvalidRequest(
                "project_id must not be empty".to_owned(),
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

pub trait IdentityKind {
    const FIELD: &'static str;
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct Identity<K: IdentityKind> {
    value: String,
    #[serde(skip)]
    marker: PhantomData<K>,
}

impl<K: IdentityKind> Identity<K> {
    pub fn new(value: String) -> Result<Self, OperatorError> {
        if value.is_empty() {
            return Err(OperatorError::InvalidRequest(format!(
                "{} must not be empty",
                K::FIELD
            )));
        }
        Ok(Self {
            value,
            marker: PhantomData,
        })
    }

    pub fn as_str(&self) -> &str {
        &self.value
    }
}

impl<K: IdentityKind> Display for Identity<K> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.value)
    }
}

impl<'de, K: IdentityKind> Deserialize<'de> for Identity<K> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
pub struct InitiatorSessionIdentityKind;
impl IdentityKind for InitiatorSessionIdentityKind {
    const FIELD: &'static str = "initiator_session_id";
}
pub type InitiatorSessionIdentity = Identity<InitiatorSessionIdentityKind>;

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
pub struct InitiatorAgentIdentityKind;
impl IdentityKind for InitiatorAgentIdentityKind {
    const FIELD: &'static str = "initiator_agent_id";
}
pub type InitiatorAgentIdentity = Identity<InitiatorAgentIdentityKind>;

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
pub struct RoleIdentityKind;
impl IdentityKind for RoleIdentityKind {
    const FIELD: &'static str = "role_id";
}
pub type RoleIdentity = Identity<RoleIdentityKind>;

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
pub struct TaskIdentityKind;
impl IdentityKind for TaskIdentityKind {
    const FIELD: &'static str = "task_id";
}
pub type TaskIdentity = Identity<TaskIdentityKind>;

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
pub struct SubjectIdentityKind;
impl IdentityKind for SubjectIdentityKind {
    const FIELD: &'static str = "subject_id";
}
pub type SubjectIdentity = Identity<SubjectIdentityKind>;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct InitiatorIdentity {
    pub initiator_session_id: InitiatorSessionIdentity,
    pub initiator_agent_id: InitiatorAgentIdentity,
    pub role_id: RoleIdentity,
    pub task_id: TaskIdentity,
    pub subject_id: SubjectIdentity,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionContinuity {
    ContinueBound,
    Independent,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SessionInventoryRequest {
    pub project_id: ProjectId,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SessionInspectRequest {
    pub project_id: ProjectId,
    pub target_session_id: SessionId,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct InitiatorBindingRequest {
    pub project_id: ProjectId,
    pub identity: InitiatorIdentity,
    pub target_session_id: SessionId,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SessionDecisionRequest {
    pub project_id: ProjectId,
    pub identity: InitiatorIdentity,
    pub continuity: SessionContinuity,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SessionEvidence {
    pub operation_id: OperationId,
    pub target_session_id: SessionId,
    pub observed_model: String,
    pub observed_claude_version: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct InitiatorBinding {
    pub project_id: ProjectId,
    pub identity: InitiatorIdentity,
    pub target_session_id: SessionId,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BindingRegistrationStatus {
    Inserted,
    Existing,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BindingRegistration {
    pub binding: InitiatorBinding,
    pub status: BindingRegistrationStatus,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionRefusalReason {
    IdentityMismatch,
    AmbiguousSessions,
    BindingRequired,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "data")]
pub enum SessionDecisionEvidence {
    Independent,
    IdentityBindings { bindings: Vec<InitiatorBinding> },
    CandidateSessions { target_session_ids: Vec<SessionId> },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "data")]
pub enum SessionDecision {
    New {
        evidence: SessionDecisionEvidence,
    },
    ResumeExact {
        target_session_id: SessionId,
        evidence_operation_ids: Vec<OperationId>,
    },
    Refuse {
        reason: SessionRefusalReason,
        evidence: SessionDecisionEvidence,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BindingPersistence {
    Inserted,
    Existing { target_session_id: SessionId },
}

impl Display for ProjectId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for ProjectId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct RequestId(Uuid);

impl RequestId {
    pub fn new(value: Uuid) -> Self {
        Self(value)
    }
    pub fn value(self) -> Uuid {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct OperationId(Uuid);

impl OperationId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
    pub fn new_exact(value: Uuid) -> Self {
        Self(value)
    }
    pub fn value(self) -> Uuid {
        self.0
    }
}

impl Default for OperationId {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct SessionId(Uuid);

impl SessionId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
    pub fn new_exact(value: Uuid) -> Self {
        Self(value)
    }
    pub fn value(self) -> Uuid {
        self.0
    }
}

impl Default for SessionId {
    fn default() -> Self {
        Self::new()
    }
}

impl Display for SessionId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProjectRegistration {
    pub project_id: ProjectId,
    pub working_directory: PathBuf,
    pub claude_executable: PathBuf,
    pub expected_opus_model: String,
}

impl ProjectRegistration {
    pub fn validate(&self) -> Result<(), OperatorError> {
        if self.expected_opus_model.is_empty() {
            return Err(OperatorError::InvalidRequest(
                "expected_opus_model must not be empty".to_owned(),
            ));
        }
        if self.working_directory.as_os_str().is_empty()
            || self.claude_executable.as_os_str().is_empty()
        {
            return Err(OperatorError::InvalidRequest(
                "project paths must not be empty".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum OperationIntent {
    New,
    ResumeExact { session_id: SessionId },
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReviewProfile {
    OpusReadOnly,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OperationStart {
    pub request_id: RequestId,
    pub project_id: ProjectId,
    pub intent: OperationIntent,
    pub prompt: String,
    pub review_profile: ReviewProfile,
}

impl OperationStart {
    pub fn validate(&self) -> Result<(), OperatorError> {
        if self.prompt.is_empty() {
            return Err(OperatorError::InvalidRequest(
                "prompt must not be empty".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationState {
    Accepted,
    Running,
    Succeeded,
    Failed,
    Cancelled,
    Indeterminate,
}

impl OperationState {
    pub fn terminal(self) -> bool {
        match self {
            Self::Accepted | Self::Running => false,
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::Indeterminate => true,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "message")]
pub enum TerminalOutcome {
    Succeeded(String),
    Failed(String),
    Cancelled(String),
    Indeterminate(String),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Operation {
    pub operation_id: OperationId,
    pub request_id: RequestId,
    pub project_id: ProjectId,
    pub intent: OperationIntent,
    pub session_id: SessionId,
    pub state: OperationState,
    pub observed_session_id: Option<SessionId>,
    pub observed_model: Option<String>,
    pub observed_claude_version: Option<String>,
    pub terminal_outcome: Option<TerminalOutcome>,
}

/// Facts atomically observed or persisted while admitting one operation request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OperationAdmission {
    Existing {
        operation: Operation,
        fingerprint: String,
    },
    MissingProject,
    ActiveSession {
        operation: Operation,
    },
    Inserted(Operation),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "data")]
pub enum DaemonRequest {
    ProjectRegister(ProjectRegistration),
    ProjectGet {
        project_id: ProjectId,
    },
    ProjectList,
    OperationStart(OperationStart),
    OperationGet {
        operation_id: OperationId,
    },
    OperationWait {
        operation_id: OperationId,
        wait_millis: u64,
    },
    OperationCancel {
        operation_id: OperationId,
    },
    SessionInventory(SessionInventoryRequest),
    SessionInspect(SessionInspectRequest),
    InitiatorBindingRegister(InitiatorBindingRequest),
    SessionDecide(SessionDecisionRequest),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "data")]
pub enum DaemonResponse {
    Project(ProjectRegistration),
    Projects(Vec<ProjectRegistration>),
    Operation(Operation),
    SessionInventory(Vec<SessionEvidence>),
    SessionEvidence(Vec<SessionEvidence>),
    BindingRegistration(BindingRegistration),
    SessionDecision(SessionDecision),
}

/// The local daemon response envelope. Its result preserves the causal refusal.
#[derive(Debug, Deserialize, Serialize)]
pub struct DaemonEnvelope {
    pub result: Result<DaemonResponse, OperatorError>,
}

#[derive(Clone, Debug, Error, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "message")]
pub enum OperatorError {
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("project is unknown: {0}")]
    UnknownProject(String),
    #[error("operation is unknown: {0}")]
    UnknownOperation(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("session cannot be classified after daemon restart: {0}")]
    UnclassifiedSession(String),
    #[error("daemon transport is unavailable: {0}")]
    TransportUnavailable(String),
    #[error("target execution failed: {0}")]
    ExecutionFailed(String),
    #[error("operation outcome is indeterminate: {0}")]
    Indeterminate(String),
    #[error("state failure: {0}")]
    State(String),
    #[error("protocol failure: {0}")]
    Protocol(String),
    #[error("target session is unknown in project {project_id}: {target_session_id}")]
    UnknownSession {
        project_id: ProjectId,
        target_session_id: SessionId,
    },
    #[error(
        "initiator binding conflicts: existing target session {existing_target_session_id}, requested {requested_target_session_id}"
    )]
    BindingConflict {
        existing_target_session_id: SessionId,
        requested_target_session_id: SessionId,
    },
    #[error("bound target session {target_session_id} has no qualifying operator evidence")]
    BoundSessionEvidenceMissing {
        binding: Box<InitiatorBinding>,
        target_session_id: SessionId,
    },
}

pub trait StatePort: Send + Sync {
    fn register_project(
        &self,
        project: ProjectRegistration,
    ) -> Result<ProjectRegistration, OperatorError>;
    fn get_project(&self, project_id: &ProjectId) -> Result<ProjectRegistration, OperatorError>;
    fn list_projects(&self) -> Result<Vec<ProjectRegistration>, OperatorError>;
    fn persist_operation_admission(
        &self,
        request: &OperationStart,
        session_id: SessionId,
        fingerprint: &str,
    ) -> Result<OperationAdmission, OperatorError>;
    fn get_operation(&self, operation_id: OperationId) -> Result<Operation, OperatorError>;
    fn transition(
        &self,
        operation_id: OperationId,
        next: OperationState,
        terminal: Option<TerminalOutcome>,
        observed_session: Option<SessionId>,
        observed_model: Option<String>,
        observed_version: Option<String>,
    ) -> Result<Operation, OperatorError>;
    fn recover_current_daemon_incomplete(&self) -> Result<(), OperatorError>;
    fn list_session_evidence(
        &self,
        project_id: &ProjectId,
    ) -> Result<Vec<SessionEvidence>, OperatorError>;
    fn inspect_session_evidence(
        &self,
        project_id: &ProjectId,
        target_session_id: SessionId,
    ) -> Result<Vec<SessionEvidence>, OperatorError>;
    fn persist_initiator_binding(
        &self,
        binding: &InitiatorBinding,
    ) -> Result<BindingPersistence, OperatorError>;
    fn get_initiator_binding(
        &self,
        project_id: &ProjectId,
        identity: &InitiatorIdentity,
    ) -> Result<Option<InitiatorBinding>, OperatorError>;
    fn list_initiator_bindings_for_initiator(
        &self,
        project_id: &ProjectId,
        initiator_session_id: &InitiatorSessionIdentity,
        initiator_agent_id: &InitiatorAgentIdentity,
    ) -> Result<Vec<InitiatorBinding>, OperatorError>;
}
