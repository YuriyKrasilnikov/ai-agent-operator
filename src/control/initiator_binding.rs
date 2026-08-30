// Copyright 2026 Yuriy Krasilnikov
// SPDX-License-Identifier: Apache-2.0

//! Proves evidence and assigns one explicit initiator identity binding.

use crate::contract::control::{
    BindingPersistence, BindingRegistration, BindingRegistrationStatus, InitiatorBinding,
    InitiatorBindingRequest, OperatorError, StatePort,
};

use super::{project, session_inventory};

pub(crate) fn register(
    state: &dyn StatePort,
    request: InitiatorBindingRequest,
) -> Result<BindingRegistration, OperatorError> {
    project::get(state, &request.project_id)?;
    let evidence = session_inventory::for_exact_session(
        state,
        &request.project_id,
        request.target_session_id,
    )?;
    if evidence.is_empty() {
        return Err(OperatorError::UnknownSession {
            project_id: request.project_id,
            target_session_id: request.target_session_id,
        });
    }
    let binding = InitiatorBinding {
        project_id: request.project_id,
        identity: request.identity,
        target_session_id: request.target_session_id,
    };
    match state.persist_initiator_binding(&binding)? {
        BindingPersistence::Inserted => Ok(BindingRegistration {
            binding,
            status: BindingRegistrationStatus::Inserted,
        }),
        BindingPersistence::Existing { target_session_id }
            if target_session_id == binding.target_session_id =>
        {
            Ok(BindingRegistration {
                binding,
                status: BindingRegistrationStatus::Existing,
            })
        }
        BindingPersistence::Existing { target_session_id } => Err(OperatorError::BindingConflict {
            existing_target_session_id: target_session_id,
            requested_target_session_id: binding.target_session_id,
        }),
    }
}
