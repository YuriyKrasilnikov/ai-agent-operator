// Copyright 2026 Yuriy Krasilnikov
// SPDX-License-Identifier: Apache-2.0

//! Owns real local daemon and MCP process custody for integration scenarios.

use std::{
    io::{BufRead, Read, Write},
    path::Path,
    process::{Child, ChildStdin, ChildStdout, Command, Stdio},
    thread,
};

pub struct ManagedProcess {
    child: Child,
    name: &'static str,
}

impl ManagedProcess {
    fn spawn(mut command: Command, name: &'static str) -> Self {
        let child = match command.spawn() {
            Ok(child) => child,
            Err(error) => panic!("{name} process starts: {error}"),
        };
        Self { child, name }
    }

    pub fn take_stdin(&mut self) -> ChildStdin {
        match self.child.stdin.take() {
            Some(stdin) => stdin,
            None => panic!("{} stdin is piped", self.name),
        }
    }

    pub fn take_stdout(&mut self) -> ChildStdout {
        match self.child.stdout.take() {
            Some(stdout) => stdout,
            None => panic!("{} stdout is piped", self.name),
        }
    }

    pub fn terminate(&mut self) {
        match self.child.kill() {
            Ok(()) => {}
            Err(error) => panic!("{} process could not stop: {error}", self.name),
        }
        match self.child.wait() {
            Ok(_) => {}
            Err(error) => panic!("{} process exit is observed: {error}", self.name),
        }
    }
}

impl Drop for ManagedProcess {
    fn drop(&mut self) {
        match self.child.try_wait() {
            Ok(Some(_)) => {}
            Ok(None) => {
                if let Err(error) = self.child.kill() {
                    eprintln!("{} process cleanup kill failed: {error}", self.name);
                }
                if let Err(error) = self.child.wait() {
                    eprintln!("{} process cleanup wait failed: {error}", self.name);
                }
            }
            Err(error) => eprintln!("{} process cleanup status failed: {error}", self.name),
        }
    }
}

pub fn start_daemon(database: &Path, socket: &Path) -> ManagedProcess {
    let mut command = Command::new(env!("CARGO_BIN_EXE_aiopd"));
    command
        .arg("--state")
        .arg(database)
        .arg("--socket")
        .arg(socket)
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    let mut daemon = ManagedProcess::spawn(command, "daemon");
    wait_for_socket(socket, &mut daemon.child);
    daemon
}

pub fn start_mcp(socket: &Path) -> ManagedProcess {
    let mut command = Command::new(env!("CARGO_BIN_EXE_aiop-mcp"));
    command
        .arg("--socket")
        .arg(socket)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    ManagedProcess::spawn(command, "MCP")
}

pub fn initialize_mcp(input: &mut impl Write, output: &mut impl BufRead) {
    send_mcp(
        input,
        serde_json::json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"acceptance","version":"1"}}}),
    );
    let response = receive_mcp(output);
    assert_eq!(response["id"], 1);
    send_mcp(
        input,
        serde_json::json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
    );
}

pub fn call_tool(
    input: &mut impl Write,
    output: &mut impl BufRead,
    id: u64,
    name: &str,
    arguments: serde_json::Value,
) -> serde_json::Value {
    send_mcp(
        input,
        serde_json::json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":{"name":name,"arguments":arguments}}),
    );
    let response = receive_mcp(output);
    assert_eq!(response["id"], id);
    response
}

pub fn send_mcp(input: &mut impl Write, value: serde_json::Value) {
    writeln!(input, "{value}").expect("MCP request writes");
    input.flush().expect("MCP request flushes");
}

pub fn receive_mcp(output: &mut impl BufRead) -> serde_json::Value {
    let mut line = String::new();
    output.read_line(&mut line).expect("MCP response reads");
    serde_json::from_str(&line).expect("MCP response is JSON")
}

fn wait_for_socket(path: &Path, daemon: &mut Child) {
    loop {
        if path.exists() {
            return;
        }
        if let Some(status) = daemon.try_wait().expect("daemon status is observable") {
            panic!(
                "daemon ended before creating its endpoint: {status}; {}",
                daemon_diagnostic(daemon)
            );
        }
        thread::yield_now();
    }
}

fn daemon_diagnostic(daemon: &mut Child) -> String {
    let Some(mut stderr) = daemon.stderr.take() else {
        return "daemon standard error was unavailable".to_owned();
    };
    let mut message = String::new();
    match stderr.read_to_string(&mut message) {
        Ok(_) if message.is_empty() => "daemon wrote no standard error".to_owned(),
        Ok(_) => format!("daemon standard error: {message}"),
        Err(error) => format!("daemon standard error could not be read: {error}"),
    }
}
