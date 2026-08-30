// Copyright 2026 Yuriy Krasilnikov
// SPDX-License-Identifier: Apache-2.0

//! Typed MCP input for evidence, binding, and exact-session decisions.

use schemars::JsonSchema;
use serde::Deserialize;
use uuid::Uuid;

use crate::{
    contract::control::{
        InitiatorAgentIdentity, InitiatorBindingRequest, InitiatorIdentity,
        InitiatorSessionIdentity, ProjectId, RoleIdentity, SessionContinuity,
        SessionDecisionRequest, SessionId, SessionInspectRequest, SessionInventoryRequest,
        SubjectIdentity, TaskIdentity,
    },
    gateway::GatewayError,
};

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct SessionInventoryInput {
    pub(crate) project_id: String,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct SessionInspectInput {
    pub(crate) project_id: String,
    pub(crate) target_session_id: String,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct InitiatorBindingRegisterInput {
    pub(crate) project_id: String,
    pub(crate) initiator_session_id: String,
    pub(crate) initiator_agent_id: String,
    pub(crate) role_id: String,
    pub(crate) task_id: String,
    pub(crate) subject_id: String,
    pub(crate) target_session_id: String,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct SessionDecideInput {
    pub(crate) project_id: String,
    pub(crate) initiator_session_id: String,
    pub(crate) initiator_agent_id: String,
    pub(crate) role_id: String,
    pub(crate) task_id: String,
    pub(crate) subject_id: String,
    pub(crate) continuity: McpSessionContinuity,
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum McpSessionContinuity {
    ContinueBound,
    Independent,
}

pub(crate) fn inventory(
    input: SessionInventoryInput,
) -> Result<crate::contract::control::DaemonRequest, GatewayError> {
    Ok(crate::contract::control::DaemonRequest::SessionInventory(
        SessionInventoryRequest {
            project_id: project_id(input.project_id)?,
        },
    ))
}

pub(crate) fn inspect(
    input: SessionInspectInput,
) -> Result<crate::contract::control::DaemonRequest, GatewayError> {
    Ok(crate::contract::control::DaemonRequest::SessionInspect(
        SessionInspectRequest {
            project_id: project_id(input.project_id)?,
            target_session_id: session_id(input.target_session_id)?,
        },
    ))
}

pub(crate) fn register(
    input: InitiatorBindingRegisterInput,
) -> Result<crate::contract::control::DaemonRequest, GatewayError> {
    Ok(
        crate::contract::control::DaemonRequest::InitiatorBindingRegister(
            InitiatorBindingRequest {
                project_id: project_id(input.project_id)?,
                identity: identity(
                    input.initiator_session_id,
                    input.initiator_agent_id,
                    input.role_id,
                    input.task_id,
                    input.subject_id,
                )?,
                target_session_id: session_id(input.target_session_id)?,
            },
        ),
    )
}

pub(crate) fn decide(
    input: SessionDecideInput,
) -> Result<crate::contract::control::DaemonRequest, GatewayError> {
    let continuity = match input.continuity {
        McpSessionContinuity::ContinueBound => SessionContinuity::ContinueBound,
        McpSessionContinuity::Independent => SessionContinuity::Independent,
    };
    Ok(crate::contract::control::DaemonRequest::SessionDecide(
        SessionDecisionRequest {
            project_id: project_id(input.project_id)?,
            identity: identity(
                input.initiator_session_id,
                input.initiator_agent_id,
                input.role_id,
                input.task_id,
                input.subject_id,
            )?,
            continuity,
        },
    ))
}

fn project_id(value: String) -> Result<ProjectId, GatewayError> {
    ProjectId::new(value).map_err(|error| GatewayError::invalid(error.to_string()))
}

fn session_id(value: String) -> Result<SessionId, GatewayError> {
    value
        .parse::<Uuid>()
        .map(SessionId::new_exact)
        .map_err(|error| {
            GatewayError::invalid(format!("target_session_id was not a UUID: {error}"))
        })
}

fn identity(
    initiator_session_id: String,
    initiator_agent_id: String,
    role_id: String,
    task_id: String,
    subject_id: String,
) -> Result<InitiatorIdentity, GatewayError> {
    Ok(InitiatorIdentity {
        initiator_session_id: InitiatorSessionIdentity::new(initiator_session_id)
            .map_err(|error| GatewayError::invalid(error.to_string()))?,
        initiator_agent_id: InitiatorAgentIdentity::new(initiator_agent_id)
            .map_err(|error| GatewayError::invalid(error.to_string()))?,
        role_id: RoleIdentity::new(role_id)
            .map_err(|error| GatewayError::invalid(error.to_string()))?,
        task_id: TaskIdentity::new(task_id)
            .map_err(|error| GatewayError::invalid(error.to_string()))?,
        subject_id: SubjectIdentity::new(subject_id)
            .map_err(|error| GatewayError::invalid(error.to_string()))?,
    })
}
