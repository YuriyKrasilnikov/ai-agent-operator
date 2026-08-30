// Copyright 2026 Yuriy Krasilnikov
// SPDX-License-Identifier: Apache-2.0

//! Writes the exact prompt and reports its one causal completion result.

use std::{io::Write, process::ChildStdin, thread};

pub fn start(stdin: ChildStdin, prompt: String) -> thread::JoinHandle<Result<(), String>> {
    thread::spawn(move || {
        let mut stdin = stdin;
        stdin
            .write_all(prompt.as_bytes())
            .map_err(|error| format!("Claude prompt could not be written: {error}"))
    })
}
