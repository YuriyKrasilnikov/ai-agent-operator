// Copyright 2026 Yuriy Krasilnikov
// SPDX-License-Identifier: Apache-2.0

//! Drains direct stdout and stderr without waiting for descendant-held pipes.

use std::{
    io::{ErrorKind, Read},
    process::{ChildStderr, ChildStdout},
};

use rustix::{
    event::{PollFd, PollFlags, Timespec},
    io::ioctl_fionbio,
};

const CHILD_STATUS_OBSERVATION_INTERVAL: Timespec = Timespec {
    tv_sec: 0,
    tv_nsec: 10_000_000,
};

pub struct OutputPump {
    stdout: ChildStdout,
    stderr: ChildStderr,
    stdout_bytes: Vec<u8>,
    stderr_bytes: Vec<u8>,
}

impl OutputPump {
    pub fn new(stdout: ChildStdout, stderr: ChildStderr) -> Result<Self, String> {
        ioctl_fionbio(&stdout, true)
            .map_err(|error| format!("Claude stdout could not become nonblocking: {error}"))?;
        ioctl_fionbio(&stderr, true)
            .map_err(|error| format!("Claude stderr could not become nonblocking: {error}"))?;
        Ok(Self {
            stdout,
            stderr,
            stdout_bytes: Vec::new(),
            stderr_bytes: Vec::new(),
        })
    }

    pub fn poll(&mut self) -> Result<(), String> {
        let mut descriptors = [
            PollFd::new(
                &self.stdout,
                PollFlags::IN | PollFlags::HUP | PollFlags::ERR,
            ),
            PollFd::new(
                &self.stderr,
                PollFlags::IN | PollFlags::HUP | PollFlags::ERR,
            ),
        ];
        rustix::event::poll(&mut descriptors, Some(&CHILD_STATUS_OBSERVATION_INTERVAL))
            .map_err(|error| format!("Claude output poll failed: {error}"))?;
        self.drain_ready()
    }

    pub fn drain_ready(&mut self) -> Result<(), String> {
        drain(&mut self.stdout, &mut self.stdout_bytes, "stdout")?;
        drain(&mut self.stderr, &mut self.stderr_bytes, "stderr")
    }

    pub fn take_stdout_lines(&mut self) -> Result<Vec<String>, String> {
        let Some(last_newline) = self.stdout_bytes.iter().rposition(|byte| *byte == b'\n') else {
            return Ok(Vec::new());
        };
        let suffix = self.stdout_bytes.split_off(last_newline + 1);
        let complete = std::mem::replace(&mut self.stdout_bytes, suffix);
        let text = std::str::from_utf8(&complete)
            .map_err(|error| format!("Claude stdout was not UTF-8 stream JSON: {error}"))?;
        Ok(text.lines().map(ToOwned::to_owned).collect())
    }

    pub fn stderr(&self) -> String {
        String::from_utf8_lossy(&self.stderr_bytes).into_owned()
    }
}

fn drain(stream: &mut impl Read, destination: &mut Vec<u8>, name: &str) -> Result<(), String> {
    let mut buffer = [0_u8; 16_384];
    loop {
        match stream.read(&mut buffer) {
            Ok(0) => return Ok(()),
            Ok(read) => destination.extend_from_slice(&buffer[..read]),
            Err(error) if error.kind() == ErrorKind::WouldBlock => return Ok(()),
            Err(error) if error.kind() == ErrorKind::Interrupted => {}
            Err(error) => return Err(format!("Claude {name} could not be read: {error}")),
        }
    }
}
