// Copyright 2026 Yuriy Krasilnikov
// SPDX-License-Identifier: Apache-2.0

//! SQLite State composition; SQL behavior is owned by SS01–SS04 modules.

mod operation;
mod ownership;
mod project;
mod session_claim;
mod sqlite;

use std::path::Path;

use crate::contract::control::{
    Operation, OperationAdmission, OperationId, OperationStart, OperationState, OperatorError,
    ProjectId, ProjectRegistration, SessionId, StatePort, TerminalOutcome,
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
}
