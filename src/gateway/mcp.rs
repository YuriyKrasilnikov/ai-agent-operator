// Copyright 2026 Yuriy Krasilnikov
// SPDX-License-Identifier: Apache-2.0

//! Explicit rmcp tool projection for the V0.1 local operator contract.

use std::path::PathBuf;

use rmcp::{
    RoleServer, ServerHandler,
    handler::server::tool::schema_for_input,
    model::{
        CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock, Implementation,
        ListToolsResult, ServerCapabilities, ServerInfo, Tool,
    },
    service::{MaybeSendFuture, RequestContext},
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::Value;
use uuid::Uuid;

use crate::contract::control::{
    DaemonRequest, OperationIntent, OperationStart, ProjectId, ProjectRegistration, RequestId,
    ReviewProfile, SessionId,
};

use super::client;

#[derive(Clone)]
pub struct McpGateway {
    endpoint: PathBuf,
}

impl McpGateway {
    pub fn new(endpoint: PathBuf) -> Self {
        Self { endpoint }
    }
}

impl ServerHandler for McpGateway {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("aiop-mcp", env!("CARGO_PKG_VERSION")))
    }

    fn list_tools(
        &self,
        _: Option<rmcp::model::PaginatedRequestParams>,
        _: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<ListToolsResult, rmcp::ErrorData>> + MaybeSendFuture + '_ {
        std::future::ready(tools())
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, rmcp::ErrorData> {
        let endpoint = self.endpoint.clone();
        let result = tokio::task::spawn_blocking(move || dispatch(&endpoint, request)).await;
        match result {
            Ok(result) => Ok(result.into()),
            Err(error) => Ok(tool_error(format!("gateway worker failed: {error}")).into()),
        }
    }
}

fn tools() -> Result<ListToolsResult, rmcp::ErrorData> {
    Ok(ListToolsResult {
        tools: vec![
            tool::<ProjectRegisterInput>("project_register", "Register one explicit project.")?,
            tool::<ProjectGetInput>("project_get", "Inspect one registered project.")?,
            tool::<EmptyInput>("project_list", "List registered projects.")?,
            tool::<OperationStartInput>(
                "operation_start",
                "Start one new or exact-resume review.",
            )?,
            tool::<OperationGetInput>("operation_get", "Inspect one operation.")?,
            tool::<OperationWaitInput>(
                "operation_wait",
                "Wait for current or terminal operation state.",
            )?,
            tool::<OperationCancelInput>(
                "operation_cancel",
                "Request direct-child cancellation and observe its terminal state.",
            )?,
        ],
        ..ListToolsResult::default()
    })
}

fn tool<T: JsonSchema + 'static>(
    name: &'static str,
    description: &'static str,
) -> Result<Tool, rmcp::ErrorData> {
    schema_for_input::<T>()
        .map(|schema| Tool::new(name, description, schema))
        .map_err(|error| rmcp::ErrorData::internal_error(error, None))
}

fn dispatch(endpoint: &std::path::Path, request: CallToolRequestParams) -> CallToolResult {
    let request_result = match request.name.as_ref() {
        "project_register" => decode::<ProjectRegisterInput>(&request.arguments).and_then(register),
        "project_get" => decode::<ProjectGetInput>(&request.arguments).and_then(get_project),
        "project_list" => empty(&request.arguments).map(|()| DaemonRequest::ProjectList),
        "operation_start" => decode::<OperationStartInput>(&request.arguments).and_then(start),
        "operation_get" => decode::<OperationGetInput>(&request.arguments).and_then(get_operation),
        "operation_wait" => {
            decode::<OperationWaitInput>(&request.arguments).and_then(wait_operation)
        }
        "operation_cancel" => {
            decode::<OperationCancelInput>(&request.arguments).and_then(cancel_operation)
        }
        _ => return tool_error("unknown aiop tool".to_owned()),
    };
    match request_result
        .and_then(|request| client::call(endpoint, request).map_err(|error| error.to_string()))
    {
        Ok(response) => match serde_json::to_string(&response) {
            Ok(value) => CallToolResult::success(vec![ContentBlock::text(value)]),
            Err(error) => tool_error(format!("operator response could not be encoded: {error}")),
        },
        Err(error) => tool_error(error.to_string()),
    }
}

fn decode<T: for<'de> Deserialize<'de>>(
    arguments: &Option<rmcp::model::JsonObject>,
) -> Result<T, String> {
    let value = match arguments {
        Some(arguments) => Value::Object(arguments.clone()),
        None => return Err("tool arguments are required".to_owned()),
    };
    serde_json::from_value(value).map_err(|error| format!("tool arguments were invalid: {error}"))
}

fn register(input: ProjectRegisterInput) -> Result<DaemonRequest, String> {
    Ok(DaemonRequest::ProjectRegister(ProjectRegistration {
        project_id: project_id(input.project_id)?,
        working_directory: input.working_directory.into(),
        claude_executable: input.claude_executable.into(),
        expected_opus_model: input.expected_opus_model,
    }))
}
fn get_project(input: ProjectGetInput) -> Result<DaemonRequest, String> {
    Ok(DaemonRequest::ProjectGet {
        project_id: project_id(input.project_id)?,
    })
}
fn get_operation(input: OperationGetInput) -> Result<DaemonRequest, String> {
    Ok(DaemonRequest::OperationGet {
        operation_id: crate::contract::control::OperationId::new_exact(parse_uuid(
            &input.operation_id,
            "operation_id",
        )?),
    })
}
fn wait_operation(input: OperationWaitInput) -> Result<DaemonRequest, String> {
    Ok(DaemonRequest::OperationWait {
        operation_id: crate::contract::control::OperationId::new_exact(parse_uuid(
            &input.operation_id,
            "operation_id",
        )?),
        wait_millis: input.wait_millis,
    })
}
fn cancel_operation(input: OperationCancelInput) -> Result<DaemonRequest, String> {
    Ok(DaemonRequest::OperationCancel {
        operation_id: crate::contract::control::OperationId::new_exact(parse_uuid(
            &input.operation_id,
            "operation_id",
        )?),
    })
}
fn start(input: OperationStartInput) -> Result<DaemonRequest, String> {
    let intent = match input.intent {
        McpIntent::New => OperationIntent::New,
        McpIntent::ResumeExact { session_id } => OperationIntent::ResumeExact {
            session_id: SessionId::new_exact(parse_uuid(&session_id, "session_id")?),
        },
    };
    Ok(DaemonRequest::OperationStart(OperationStart {
        request_id: RequestId::new(parse_uuid(&input.request_id, "request_id")?),
        project_id: project_id(input.project_id)?,
        intent,
        prompt: input.prompt,
        review_profile: review_profile(input.review_profile)?,
    }))
}
fn parse_uuid(value: &str, field: &str) -> Result<Uuid, String> {
    value
        .parse()
        .map_err(|error| format!("{field} was not a UUID: {error}"))
}
fn project_id(value: String) -> Result<ProjectId, String> {
    ProjectId::new(value).map_err(|error| error.to_string())
}
fn review_profile(value: McpReviewProfile) -> Result<ReviewProfile, String> {
    match value {
        McpReviewProfile::OpusReadOnly => Ok(ReviewProfile::OpusReadOnly),
    }
}
fn empty(arguments: &Option<rmcp::model::JsonObject>) -> Result<(), String> {
    match arguments {
        None => Ok(()),
        Some(arguments) if arguments.is_empty() => Ok(()),
        Some(_) => Err("project_list does not accept arguments".to_owned()),
    }
}
fn tool_error(message: String) -> CallToolResult {
    CallToolResult::error(vec![ContentBlock::text(message)])
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct EmptyInput {}
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ProjectRegisterInput {
    project_id: String,
    working_directory: String,
    claude_executable: String,
    expected_opus_model: String,
}
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ProjectGetInput {
    project_id: String,
}
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct OperationGetInput {
    operation_id: String,
}
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct OperationWaitInput {
    operation_id: String,
    wait_millis: u64,
}
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct OperationCancelInput {
    operation_id: String,
}
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct OperationStartInput {
    request_id: String,
    project_id: String,
    intent: McpIntent,
    prompt: String,
    review_profile: McpReviewProfile,
}
#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum McpReviewProfile {
    OpusReadOnly,
}
#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "kind", deny_unknown_fields)]
enum McpIntent {
    New,
    ResumeExact { session_id: String },
}
