// Copyright 2026 Yuriy Krasilnikov
// SPDX-License-Identifier: Apache-2.0

//! Builds the one supported Claude command line.

use std::process::Command;

use crate::contract::target::{TargetCommand, TargetIntent, TargetLiveStart};

pub fn build(command: &TargetCommand) -> Command {
    let mut process = Command::new(&command.executable);
    process.current_dir(&command.working_directory);
    profile(&mut process, InvocationMode::OneShot);
    session(&mut process, &command.intent, command.session_id.0);
    process
}

pub fn build_live(start: &TargetLiveStart) -> Command {
    let mut process = Command::new(&start.executable);
    process.current_dir(&start.working_directory);
    profile(&mut process, InvocationMode::Live);
    session(&mut process, &start.intent, start.session_id.0);
    process
}

enum InvocationMode {
    OneShot,
    Live,
}

fn profile(process: &mut Command, mode: InvocationMode) {
    process.arg("--print");
    match mode {
        InvocationMode::OneShot => {
            process
                .arg("--verbose")
                .arg("--output-format")
                .arg("stream-json");
        }
        InvocationMode::Live => {
            process
                .arg("--input-format")
                .arg("stream-json")
                .arg("--output-format")
                .arg("stream-json")
                .arg("--include-partial-messages")
                .arg("--replay-user-messages")
                .arg("--verbose");
        }
    }
    process
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
}

fn session(process: &mut Command, intent: &TargetIntent, intended_session: uuid::Uuid) {
    match intent {
        TargetIntent::New => {
            process
                .arg("--session-id")
                .arg(intended_session.to_string());
        }
        TargetIntent::ResumeExact { session_id } => {
            process.arg("--resume").arg(session_id.0.to_string());
        }
    }
}
