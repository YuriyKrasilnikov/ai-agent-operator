// Copyright 2026 Yuriy Krasilnikov
// SPDX-License-Identifier: Apache-2.0

//! Spawns a shell-free direct child with owned standard streams.

use std::process::{Child, Stdio};

use crate::contract::target::TargetCommand;

use super::command;

pub fn spawn(command: &TargetCommand) -> Result<Child, String> {
    command::build(command)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| format!("configured Claude executable could not start: {error}"))
}
