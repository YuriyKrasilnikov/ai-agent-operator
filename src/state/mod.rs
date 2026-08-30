// Copyright 2026 Yuriy Krasilnikov
// SPDX-License-Identifier: Apache-2.0

//! SQLite State composition for project, operation, claim, and binding records.

mod initiator_binding;
mod operation;
mod ownership;
mod project;
mod session_claim;
mod sqlite;

use std::path::Path;

use crate::contract::control::{
    BindingPersistence, InitiatorAgentIdentity, InitiatorBinding, InitiatorIdentity,
    InitiatorSessionIdentity, Operation, OperationAdmission, OperationId, OperationStart,
    OperationState, OperatorError, ProjectId, ProjectRegistration, SessionEvidence, SessionId,
    StatePort, TerminalOutcome,
};

#[derive(Clone)]
pub struct SqliteState {
    adapter: sqlite::Adapter,
}

impl SqliteState {
    pub fn open(path: &Path) -> Result<Self, OperatorError> {
        let state = Self {
            adapter: sqlite::Adapter::open(path)?,
        };
        operation::recover(&state.adapter)?;
        Ok(state)
    }
}

impl StatePort for SqliteState {
    fn register_project(
        &self,
        project: ProjectRegistration,
    ) -> Result<ProjectRegistration, OperatorError> {
        project::register(&self.adapter, project)
    }
    fn get_project(&self, project_id: &ProjectId) -> Result<ProjectRegistration, OperatorError> {
        project::get(&self.adapter, project_id)
    }
    fn list_projects(&self) -> Result<Vec<ProjectRegistration>, OperatorError> {
        project::list(&self.adapter)
    }
    fn persist_operation_admission(
        &self,
        request: &OperationStart,
        session_id: SessionId,
        fingerprint: &str,
    ) -> Result<OperationAdmission, OperatorError> {
        operation::persist_admission(&self.adapter, request, session_id, fingerprint)
    }
    fn get_operation(&self, operation_id: OperationId) -> Result<Operation, OperatorError> {
        operation::get(&self.adapter, operation_id)
    }
    fn transition(
        &self,
        operation_id: OperationId,
        next: OperationState,
        terminal: Option<TerminalOutcome>,
        observed_session: Option<SessionId>,
        observed_model: Option<String>,
        observed_version: Option<String>,
    ) -> Result<Operation, OperatorError> {
        operation::transition(
            &self.adapter,
            operation_id,
            next,
            terminal,
            observed_session,
            observed_model,
            observed_version,
        )
    }
    fn recover_current_daemon_incomplete(&self) -> Result<(), OperatorError> {
        operation::recover(&self.adapter)
    }
    fn list_session_evidence(
        &self,
        project_id: &ProjectId,
    ) -> Result<Vec<SessionEvidence>, OperatorError> {
        operation::list_session_evidence(&self.adapter, project_id)
    }
    fn inspect_session_evidence(
        &self,
        project_id: &ProjectId,
        target_session_id: SessionId,
    ) -> Result<Vec<SessionEvidence>, OperatorError> {
        operation::inspect_session_evidence(&self.adapter, project_id, target_session_id)
    }
    fn persist_initiator_binding(
        &self,
        binding: &InitiatorBinding,
    ) -> Result<BindingPersistence, OperatorError> {
        initiator_binding::persist(&self.adapter, binding)
    }
    fn get_initiator_binding(
        &self,
        project_id: &ProjectId,
        identity: &InitiatorIdentity,
    ) -> Result<Option<InitiatorBinding>, OperatorError> {
        initiator_binding::get(&self.adapter, project_id, identity)
    }
    fn list_initiator_bindings_for_initiator(
        &self,
        project_id: &ProjectId,
        initiator_session_id: &InitiatorSessionIdentity,
        initiator_agent_id: &InitiatorAgentIdentity,
    ) -> Result<Vec<InitiatorBinding>, OperatorError> {
        initiator_binding::list_for_initiator(
            &self.adapter,
            project_id,
            initiator_session_id,
            initiator_agent_id,
        )
    }
}
