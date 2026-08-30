// Copyright 2026 Yuriy Krasilnikov
// SPDX-License-Identifier: Apache-2.0

//! Stdio composition root for the official MCP gateway server.

use std::{path::PathBuf, process::ExitCode};

use aiop::gateway::mcp::McpGateway;
use rmcp::ServiceExt;

#[tokio::main]
async fn main() -> ExitCode {
    let endpoint = match endpoint() {
        Ok(endpoint) => endpoint,
        Err(error) => {
            eprintln!("aiop-mcp: {error}");
            return ExitCode::FAILURE;
        }
    };
    let server = McpGateway::new(endpoint);
    match server.serve(rmcp::transport::stdio()).await {
        Ok(running) => match running.waiting().await {
            Ok(_) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("aiop-mcp: MCP service failed: {error}");
                ExitCode::FAILURE
            }
        },
        Err(error) => {
            eprintln!("aiop-mcp: MCP initialization failed: {error}");
            ExitCode::FAILURE
        }
    }
}

fn endpoint() -> Result<PathBuf, String> {
    let mut arguments = std::env::args_os();
    let _program = arguments.next();
    let flag = arguments
        .next()
        .ok_or_else(|| "missing --socket argument".to_owned())?;
    if flag != "--socket" {
        return Err("expected --socket argument".to_owned());
    }
    let path = arguments
        .next()
        .ok_or_else(|| "missing socket path".to_owned())?;
    if arguments.next().is_some() {
        return Err("unexpected additional arguments".to_owned());
    }
    Ok(path.into())
}
