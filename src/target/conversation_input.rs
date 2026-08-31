// Copyright 2026 Yuriy Krasilnikov
// SPDX-License-Identifier: Apache-2.0

//! Serializes durably ordered structured user turns to retained child input.

use std::{
    collections::BTreeMap,
    io::Write,
    process::ChildStdin,
    sync::mpsc::{Receiver, Sender},
    thread,
};

use crate::contract::target::TargetLiveTurn;

#[derive(Debug)]
pub(crate) enum WriterCommand {
    Turn(TargetLiveTurn),
    Close { through_position: u64 },
    Cancel,
}

#[derive(Debug)]
pub(crate) enum WriterReport {
    Prepared(TargetLiveTurn),
    Closed,
    Failed(String),
}

pub(crate) fn start(
    stdin: ChildStdin,
    first_turn: TargetLiveTurn,
    running_permission: Receiver<Result<(), String>>,
    commands: Receiver<WriterCommand>,
    reports: Sender<WriterReport>,
) -> Result<thread::JoinHandle<()>, String> {
    thread::Builder::new()
        .name("aiop-live-input".to_owned())
        .spawn(move || {
            if let Err(error) = run(
                stdin,
                first_turn,
                running_permission,
                commands,
                reports.clone(),
            ) && reports.send(WriterReport::Failed(error.clone())).is_err()
            {
                eprintln!("aiop live input failure could not reach its observer: {error}");
            }
        })
        .map_err(|error| format!("live input thread could not start: {error}"))
}

fn run(
    mut stdin: ChildStdin,
    first_turn: TargetLiveTurn,
    running_permission: Receiver<Result<(), String>>,
    commands: Receiver<WriterCommand>,
    reports: Sender<WriterReport>,
) -> Result<(), String> {
    let mut pending = BTreeMap::new();
    let mut next_position = first_turn.position;
    pending.insert(first_turn.position, first_turn);
    let mut close_through = None;
    wait_for_running_permission(&running_permission)?;
    loop {
        write_ready(&mut stdin, &mut pending, &mut next_position, &reports)?;
        if close_through.is_some_and(|position| next_position > position) {
            reports.send(WriterReport::Closed).map_err(|_| {
                "live runtime stopped before graceful close was observed".to_owned()
            })?;
            return Ok(());
        }
        if !receive_command(&commands, &mut pending, &mut close_through)? {
            return Ok(());
        }
    }
}

fn wait_for_running_permission(
    running_permission: &Receiver<Result<(), String>>,
) -> Result<(), String> {
    match running_permission.recv() {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(error),
        Err(_) => {
            Err("Control did not grant durable Running permission to the live child".to_owned())
        }
    }
}

fn receive_command(
    commands: &Receiver<WriterCommand>,
    pending: &mut BTreeMap<u64, TargetLiveTurn>,
    close_through: &mut Option<u64>,
) -> Result<bool, String> {
    match commands.recv() {
        Ok(WriterCommand::Turn(turn)) => {
            if pending.insert(turn.position, turn).is_some() {
                return Err("live input received two turns at one durable position".to_owned());
            }
            Ok(true)
        }
        Ok(WriterCommand::Close { through_position }) => {
            *close_through = Some(through_position);
            Ok(true)
        }
        Ok(WriterCommand::Cancel) => Ok(false),
        Err(_) => Err("live runtime dropped the input command channel".to_owned()),
    }
}

fn write_ready(
    stdin: &mut ChildStdin,
    pending: &mut BTreeMap<u64, TargetLiveTurn>,
    next_position: &mut u64,
    reports: &Sender<WriterReport>,
) -> Result<(), String> {
    while let Some(turn) = pending.remove(&*next_position) {
        reports
            .send(WriterReport::Prepared(turn.clone()))
            .map_err(|_| "live runtime stopped before turn input could be observed".to_owned())?;
        let frame = encode(&turn)?;
        stdin
            .write_all(frame.as_bytes())
            .map_err(|error| format!("Claude live turn could not be written: {error}"))?;
        stdin
            .write_all(b"\n")
            .map_err(|error| format!("Claude live turn newline could not be written: {error}"))?;
        stdin
            .flush()
            .map_err(|error| format!("Claude live turn could not be flushed: {error}"))?;
        *next_position = next_position
            .checked_add(1)
            .ok_or_else(|| "live turn position is exhausted".to_owned())?;
    }
    Ok(())
}

fn encode(turn: &TargetLiveTurn) -> Result<String, String> {
    serde_json::to_string(&serde_json::json!({
        "type": "user",
        "uuid": turn.turn_id.0.to_string(),
        "message": {
            "role": "user",
            "content": [{"type": "text", "text": turn.prompt}],
        },
    }))
    .map_err(|error| format!("live turn could not become JSON: {error}"))
}
