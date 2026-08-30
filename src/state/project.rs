// Copyright 2026 Yuriy Krasilnikov
// SPDX-License-Identifier: Apache-2.0

//! SS02: project record persistence.
use super::sqlite::{Adapter, decode, encode, sql_error};
use crate::contract::control::{OperatorError, ProjectId, ProjectRegistration};
use ::sqlite::{ConnectionThreadSafe, State};
pub(crate) fn register(
    adapter: &Adapter,
    project: ProjectRegistration,
) -> Result<ProjectRegistration, OperatorError> {
    adapter.transaction(|c| match find(c, &project.project_id)? {
        Some(existing) if existing == project => Ok(existing),
        Some(_) => Err(OperatorError::Conflict(
            "project_id is already registered with different configuration".to_owned(),
        )),
        None => {
            let record = encode(&project)?;
            let mut s = c
                .prepare("INSERT INTO projects (project_id, record_json) VALUES (?, ?)")
                .map_err(sql_error)?;
            s.bind(&[(1, project.project_id.as_str()), (2, record.as_str())][..])
                .map_err(sql_error)?;
            s.next().map_err(sql_error)?;
            Ok(project)
        }
    })
}
pub(crate) fn get(adapter: &Adapter, id: &ProjectId) -> Result<ProjectRegistration, OperatorError> {
    adapter
        .read(|c| find(c, id)?.ok_or_else(|| OperatorError::UnknownProject(id.as_str().to_owned())))
}
pub(crate) fn list(adapter: &Adapter) -> Result<Vec<ProjectRegistration>, OperatorError> {
    adapter.read(|c| {
        let mut s = c
            .prepare("SELECT record_json FROM projects ORDER BY project_id")
            .map_err(sql_error)?;
        let mut out = Vec::new();
        loop {
            match s.next().map_err(sql_error)? {
                State::Row => out.push(decode(s.read::<String, _>(0).map_err(sql_error)?)?),
                State::Done => return Ok(out),
            }
        }
    })
}
pub(crate) fn find(
    c: &ConnectionThreadSafe,
    id: &ProjectId,
) -> Result<Option<ProjectRegistration>, OperatorError> {
    let mut s = c
        .prepare("SELECT record_json FROM projects WHERE project_id = ?")
        .map_err(sql_error)?;
    s.bind((1, id.as_str())).map_err(sql_error)?;
    match s.next().map_err(sql_error)? {
        State::Row => Ok(Some(decode(s.read::<String, _>(0).map_err(sql_error)?)?)),
        State::Done => Ok(None),
    }
}
