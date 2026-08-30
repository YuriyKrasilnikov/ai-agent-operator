// Copyright 2026 Yuriy Krasilnikov
// SPDX-License-Identifier: Apache-2.0

//! Starts the persistent local daemon in the Owner-managed Claude environment.

use std::{env, path::PathBuf, process::ExitCode, sync::Arc};

fn main() -> ExitCode {
    let arguments: Vec<String> = env::args().skip(1).collect();
    let (state, socket) = match arguments.as_slice() {
        [state_flag, state, socket_flag, socket]
            if state_flag == "--state" && socket_flag == "--socket" =>
        {
            (PathBuf::from(state), PathBuf::from(socket))
        }
        _ => {
            eprintln!("usage: aiopd --state <sqlite-path> --socket <unix-socket>");
            return ExitCode::FAILURE;
        }
    };
    let state_owner = match aiop::state::SqliteState::open(&state) {
        Ok(state_owner) => Arc::new(state_owner),
        Err(error) => {
            eprintln!("aiopd: {error}");
            return ExitCode::FAILURE;
        }
    };
    let target_owner = Arc::new(aiop::target::ClaudeTarget::default());
    let control = aiop::control::OperationControl::new(state_owner, target_owner);
    match aiop::control::ingress::serve(&socket, control) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("aiopd: {error}");
            ExitCode::FAILURE
        }
    }
}
