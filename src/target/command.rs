// Copyright 2026 Yuriy Krasilnikov
// SPDX-License-Identifier: Apache-2.0

//! Builds the one supported Claude command line.

use std::process::Command;

use crate::contract::target::{TargetCommand, TargetIntent};

pub fn build(command: &TargetCommand) -> Command {
    let mut process = Command::new(&command.executable);
    process.current_dir(&command.working_directory);
    process
        .arg("--print")
        .arg("--verbose")
        .arg("--output-format")
        .arg("stream-json")
        .arg("--model")
        .arg("opus")
        .arg("--effort")
        .arg("max")
        .arg("--permission-mode")
        .arg("dontAsk")
        .arg("--restricted")
        .arg("--safe-mode")
        .arg("--tools")
        .arg("Read,Glob,Grep")
        .arg("--allowedTools")
        .arg("Read,Glob,Grep");
    match command.intent {
        TargetIntent::New => {
            process
                .arg("--session-id")
                .arg(command.session_id.0.to_string());
        }
        TargetIntent::ResumeExact { session_id } => {
            process.arg("--resume").arg(session_id.0.to_string());
        }
    }
    process
}
