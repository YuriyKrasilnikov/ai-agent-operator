// Copyright 2026 Yuriy Krasilnikov
// SPDX-License-Identifier: Apache-2.0

//! Resolves trusted project records for operation coordination.

use crate::contract::control::{OperatorError, ProjectId, ProjectRegistration, StatePort};

pub fn register(
    state: &dyn StatePort,
    project: ProjectRegistration,
) -> Result<ProjectRegistration, OperatorError> {
    project.validate()?;
    state.register_project(project)
}
pub fn get(
    state: &dyn StatePort,
    project_id: &ProjectId,
) -> Result<ProjectRegistration, OperatorError> {
    state.get_project(project_id)
}
pub fn list(state: &dyn StatePort) -> Result<Vec<ProjectRegistration>, OperatorError> {
    state.list_projects()
}

pub fn resolve(
    state: &dyn StatePort,
    project_id: &ProjectId,
) -> Result<ProjectRegistration, OperatorError> {
    let project = get(state, project_id)?;
    if !project.working_directory.is_dir() {
        return Err(OperatorError::InvalidRequest(
            "registered working_directory is not a directory".to_owned(),
        ));
    }
    Ok(project)
}
