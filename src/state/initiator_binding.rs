// Copyright 2026 Yuriy Krasilnikov
// SPDX-License-Identifier: Apache-2.0

//! Persists exact initiator bindings without interpreting their business meaning.

use ::sqlite::{ConnectionThreadSafe, State};

use super::sqlite::{Adapter, decode, encode, sql_error};
use crate::contract::control::{
    BindingPersistence, InitiatorAgentIdentity, InitiatorBinding, InitiatorIdentity,
    InitiatorSessionIdentity, OperatorError, ProjectId,
};

pub(crate) fn persist(
    adapter: &Adapter,
    binding: &InitiatorBinding,
) -> Result<BindingPersistence, OperatorError> {
    adapter.transaction(|connection| {
        match find(connection, &binding.project_id, &binding.identity)? {
            Some(existing) => Ok(BindingPersistence::Existing {
                target_session_id: existing.target_session_id,
            }),
            None => {
                insert(connection, binding)?;
                Ok(BindingPersistence::Inserted)
            }
        }
    })
}

pub(crate) fn get(
    adapter: &Adapter,
    project_id: &ProjectId,
    identity: &InitiatorIdentity,
) -> Result<Option<InitiatorBinding>, OperatorError> {
    adapter.read(|connection| find(connection, project_id, identity))
}

pub(crate) fn list_for_initiator(
    adapter: &Adapter,
    project_id: &ProjectId,
    initiator_session_id: &InitiatorSessionIdentity,
    initiator_agent_id: &InitiatorAgentIdentity,
) -> Result<Vec<InitiatorBinding>, OperatorError> {
    adapter.read(|connection| {
        let mut statement = connection
            .prepare(
                "SELECT record_json FROM initiator_bindings WHERE project_id = ? AND initiator_session_id = ? AND initiator_agent_id = ? ORDER BY role_id, task_id, subject_id",
            )
            .map_err(sql_error)?;
        statement
            .bind(&[
                (1, project_id.as_str()),
                (2, initiator_session_id.as_str()),
                (3, initiator_agent_id.as_str()),
            ][..])
            .map_err(sql_error)?;
        let mut bindings = Vec::new();
        while let State::Row = statement.next().map_err(sql_error)? {
            bindings.push(decode(statement.read::<String, _>(0).map_err(sql_error)?)?);
        }
        Ok(bindings)
    })
}

fn find(
    connection: &ConnectionThreadSafe,
    project_id: &ProjectId,
    identity: &InitiatorIdentity,
) -> Result<Option<InitiatorBinding>, OperatorError> {
    let mut statement = connection
        .prepare(
            "SELECT record_json FROM initiator_bindings WHERE project_id = ? AND initiator_session_id = ? AND initiator_agent_id = ? AND role_id = ? AND task_id = ? AND subject_id = ?",
        )
        .map_err(sql_error)?;
    statement
        .bind(
            &[
                (1, project_id.as_str()),
                (2, identity.initiator_session_id.as_str()),
                (3, identity.initiator_agent_id.as_str()),
                (4, identity.role_id.as_str()),
                (5, identity.task_id.as_str()),
                (6, identity.subject_id.as_str()),
            ][..],
        )
        .map_err(sql_error)?;
    match statement.next().map_err(sql_error)? {
        State::Row => Ok(Some(decode(
            statement.read::<String, _>(0).map_err(sql_error)?,
        )?)),
        State::Done => Ok(None),
    }
}

fn insert(
    connection: &ConnectionThreadSafe,
    binding: &InitiatorBinding,
) -> Result<(), OperatorError> {
    let record = encode(binding)?;
    let target_session_id = binding.target_session_id.value().to_string();
    let mut statement = connection
        .prepare(
            "INSERT INTO initiator_bindings (project_id, initiator_session_id, initiator_agent_id, role_id, task_id, subject_id, target_session_id, record_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .map_err(sql_error)?;
    statement
        .bind(
            &[
                (1, binding.project_id.as_str()),
                (2, binding.identity.initiator_session_id.as_str()),
                (3, binding.identity.initiator_agent_id.as_str()),
                (4, binding.identity.role_id.as_str()),
                (5, binding.identity.task_id.as_str()),
                (6, binding.identity.subject_id.as_str()),
                (7, target_session_id.as_str()),
                (8, record.as_str()),
            ][..],
        )
        .map_err(sql_error)?;
    statement.next().map_err(sql_error)?;
    Ok(())
}
