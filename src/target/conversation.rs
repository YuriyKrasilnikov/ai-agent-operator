// Copyright 2026 Yuriy Krasilnikov
// SPDX-License-Identifier: Apache-2.0

//! Owns one persistent structured child and its provider-neutral observations.

use std::{
    collections::HashMap,
    process::{ChildStderr, ChildStdout},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, Sender, TryRecvError},
    },
    thread,
};

use crate::contract::target::{
    TargetLiveObservation, TargetLiveStart, TargetLiveStartError, TargetLiveStop, TargetLiveTurn,
    TargetOperationId, TargetTurnId,
};

use super::{
    child::{ChildExitEvidence, Children},
    conversation_codec::Codec,
    conversation_input::{self, WriterCommand, WriterReport},
    launch,
    output::OutputPump,
};

#[derive(Clone, Default)]
pub(crate) struct LiveRuntimes {
    handles: Arc<Mutex<HashMap<TargetOperationId, Arc<LiveHandle>>>>,
}

struct LiveHandle {
    commands: Sender<WriterCommand>,
    admitted: Mutex<AdmittedTurns>,
    graceful: AtomicBool,
    cancelled: AtomicBool,
}

struct AdmittedTurns {
    turns: HashMap<TargetTurnId, TargetLiveTurn>,
    positions: HashMap<u64, TargetTurnId>,
    graceful_through: Option<u64>,
    broken: bool,
}

struct RuntimeObserver {
    children: Children,
    runtimes: LiveRuntimes,
    operation_id: TargetOperationId,
    expected_model: String,
    session_id: crate::contract::target::TargetSessionId,
    handle: Arc<LiveHandle>,
    observations: Sender<TargetLiveObservation>,
    writer: thread::JoinHandle<()>,
    reports: Receiver<WriterReport>,
    stdout: ChildStdout,
    stderr: ChildStderr,
}

impl LiveRuntimes {
    pub(crate) fn start(
        &self,
        children: Children,
        start: TargetLiveStart,
        observations: Sender<TargetLiveObservation>,
    ) -> Result<(), TargetLiveStartError> {
        if start.first_turn.position != 1 {
            return Err(TargetLiveStartError::NoWriter(
                "first live turn must have durable position 1".to_owned(),
            ));
        }
        let mut child = launch::spawn_live(&start).map_err(TargetLiveStartError::NoWriter)?;
        let stdin = match child.stdin.take() {
            Some(stdin) => stdin,
            None => {
                return Err(stop_spawned(
                    child,
                    "direct live child stdin was not captured",
                ));
            }
        };
        let stdout = match child.stdout.take() {
            Some(stdout) => stdout,
            None => {
                return Err(stop_spawned(
                    child,
                    "direct live child stdout was not captured",
                ));
            }
        };
        let stderr = match child.stderr.take() {
            Some(stderr) => stderr,
            None => {
                return Err(stop_spawned(
                    child,
                    "direct live child stderr was not captured",
                ));
            }
        };
        let (commands, command_receiver) = mpsc::channel();
        let handle = Arc::new(LiveHandle {
            commands,
            admitted: Mutex::new(AdmittedTurns::new(start.first_turn.clone())),
            graceful: AtomicBool::new(false),
            cancelled: AtomicBool::new(false),
        });
        children
            .insert(start.operation_id, child)
            .map_err(live_start_error_from_registration)?;
        if let Err(error) = self.insert(start.operation_id, Arc::clone(&handle)) {
            return Err(stop_registered_child(&children, start.operation_id, error));
        }
        let TargetLiveStart {
            operation_id,
            working_directory: _,
            executable: _,
            expected_model,
            intent: _,
            session_id,
            first_turn,
            running_permission,
        } = start;
        let (observer_sender, observer_receiver) = mpsc::sync_channel(1);
        let observer_thread = match thread::Builder::new()
            .name("aiop-live-observer".to_owned())
            .spawn(move || match observer_receiver.recv() {
                Ok(observer) => {
                    if let Err(error) = observe(observer) {
                        eprintln!("aiop live observer failed after cleanup: {error}");
                    }
                }
                Err(_) => eprintln!("aiop live observer was not given its runtime"),
            }) {
            Ok(thread) => thread,
            Err(error) => {
                let root = format!("live observer thread could not start: {error}");
                let child_cleanup = stop_registered_child(&children, operation_id, root.clone());
                let runtime_cleanup = self.remove(operation_id);
                return Err(append_startup_cleanup(child_cleanup, runtime_cleanup));
            }
        };
        let (reports, report_receiver) = mpsc::channel();
        let writer = match conversation_input::start(
            stdin,
            first_turn.clone(),
            running_permission,
            command_receiver,
            reports,
        ) {
            Ok(writer) => writer,
            Err(error) => {
                let cleanup = stop_registered_child(&children, operation_id, error.clone());
                let runtime_cleanup = self.remove(operation_id);
                drop(observer_sender);
                let observer_exit = observer_thread
                    .join()
                    .map_err(|_| "live observer panicked during startup cleanup".to_owned());
                let cleanup = append_startup_cleanup(cleanup, runtime_cleanup);
                return Err(append_startup_cleanup(cleanup, observer_exit));
            }
        };
        let runtimes = self.clone();
        let observer_children = children.clone();
        let observer_runtimes = runtimes.clone();
        let observer_operation_id = operation_id;
        let observer = RuntimeObserver {
            children: observer_children,
            runtimes: observer_runtimes,
            operation_id,
            expected_model,
            session_id,
            handle: Arc::clone(&handle),
            observations,
            writer,
            reports: report_receiver,
            stdout,
            stderr,
        };
        if let Err(error) = observer_sender.send(observer) {
            let observer = error.0;
            let cleanup = cleanup(
                &children,
                &runtimes,
                observer_operation_id,
                &observer.handle,
                observer.writer,
                CleanupMode::Terminate,
            );
            let observer_exit = observer_thread
                .join()
                .map_err(|_| "live observer panicked during startup cleanup".to_owned());
            return startup_error(
                "live observer stopped before it received the runtime".to_owned(),
                CleanupResult {
                    result: combine_results(
                        "live observer cleanup".to_owned(),
                        cleanup.result,
                        observer_exit,
                    ),
                    proof: cleanup.proof,
                },
            );
        }
        Ok(())
    }

    pub(crate) fn send(
        &self,
        operation_id: TargetOperationId,
        turn: TargetLiveTurn,
    ) -> Result<(), String> {
        let handle = self.handle(operation_id)?;
        if !handle.admit(turn.clone())? {
            return Ok(());
        }
        if handle.commands.send(WriterCommand::Turn(turn)).is_err() {
            handle.mark_broken()?;
            return Err("live input writer stopped after accepting a turn".to_owned());
        }
        Ok(())
    }

    pub(crate) fn stop(
        &self,
        children: &Children,
        operation_id: TargetOperationId,
        stop: TargetLiveStop,
    ) -> Result<(), String> {
        let handle = self.handle(operation_id)?;
        match stop {
            TargetLiveStop::Graceful { through_position } => {
                if handle.close(through_position)? {
                    handle
                        .commands
                        .send(WriterCommand::Close { through_position })
                        .map_err(|_| {
                            "live input writer stopped before graceful close".to_owned()
                        })?;
                }
                Ok(())
            }
            TargetLiveStop::Cancel => {
                if !handle.cancelled.swap(true, Ordering::SeqCst) {
                    handle.stop_writer_cooperatively()?;
                    let child_stop = children.terminate(operation_id);
                    child_stop?;
                }
                Ok(())
            }
        }
    }

    fn insert(
        &self,
        operation_id: TargetOperationId,
        handle: Arc<LiveHandle>,
    ) -> Result<(), String> {
        let mut handles = self
            .handles
            .lock()
            .map_err(|_| "live runtime registry was poisoned".to_owned())?;
        if handles.insert(operation_id, handle).is_some() {
            return Err("operation already owns a live runtime".to_owned());
        }
        Ok(())
    }

    fn remove(&self, operation_id: TargetOperationId) -> Result<(), String> {
        let mut handles = self
            .handles
            .lock()
            .map_err(|_| "live runtime registry was poisoned".to_owned())?;
        if handles.remove(&operation_id).is_none() {
            return Err("live runtime disappeared before completion".to_owned());
        }
        Ok(())
    }

    fn handle(&self, operation_id: TargetOperationId) -> Result<Arc<LiveHandle>, String> {
        let handles = self
            .handles
            .lock()
            .map_err(|_| "live runtime registry was poisoned".to_owned())?;
        handles
            .get(&operation_id)
            .cloned()
            .ok_or_else(|| "live runtime is not active".to_owned())
    }
}

impl AdmittedTurns {
    fn new(first_turn: TargetLiveTurn) -> Self {
        let mut turns = HashMap::new();
        turns.insert(first_turn.turn_id, first_turn.clone());
        let mut positions = HashMap::new();
        positions.insert(first_turn.position, first_turn.turn_id);
        Self {
            turns,
            positions,
            graceful_through: None,
            broken: false,
        }
    }
}

impl LiveHandle {
    /// Stops the input writer when present; a disconnected receiver already stopped.
    ///
    /// Direct-child termination supplies cancellation when the writer already
    /// completed or disconnected, so no input-channel delivery is required.
    fn stop_writer_cooperatively(&self) -> Result<(), String> {
        match self.commands.send(WriterCommand::Cancel) {
            Ok(()) | Err(_) => Ok(()),
        }
    }

    fn admit(&self, turn: TargetLiveTurn) -> Result<bool, String> {
        let mut admitted = self
            .admitted
            .lock()
            .map_err(|_| "live turn admission was poisoned".to_owned())?;
        if admitted.broken {
            return Err("live input delivery is indeterminate".to_owned());
        }
        if let Some(existing) = admitted.turns.get(&turn.turn_id) {
            if existing == &turn {
                return Ok(false);
            }
            return Err("live turn UUID was reused with different content".to_owned());
        }
        if let Some(existing_turn_id) = admitted.positions.get(&turn.position) {
            return Err(format!(
                "live turn position is already owned by {existing_turn_id:?}"
            ));
        }
        if self.cancelled.load(Ordering::SeqCst) {
            return Err("live conversation was cancelled before turn delivery".to_owned());
        }
        if let Some(through_position) = admitted.graceful_through
            && turn.position > through_position
        {
            return Err(
                "live conversation no longer admits turns beyond graceful close".to_owned(),
            );
        }
        admitted.positions.insert(turn.position, turn.turn_id);
        admitted.turns.insert(turn.turn_id, turn);
        Ok(true)
    }

    fn close(&self, through_position: u64) -> Result<bool, String> {
        let mut admitted = self
            .admitted
            .lock()
            .map_err(|_| "live turn admission was poisoned".to_owned())?;
        if admitted.broken {
            return Err("live input delivery is indeterminate".to_owned());
        }
        if self.cancelled.load(Ordering::SeqCst) {
            return Err("live conversation was cancelled before graceful close".to_owned());
        }
        if through_position == 0 {
            return Err("graceful close position must start at one".to_owned());
        }
        if let Some(existing) = admitted.graceful_through {
            return if existing == through_position {
                Ok(false)
            } else {
                Err("graceful close contradicted its durable admitted position".to_owned())
            };
        }
        admitted.graceful_through = Some(through_position);
        self.graceful.store(true, Ordering::SeqCst);
        Ok(true)
    }

    fn mark_broken(&self) -> Result<(), String> {
        let mut admitted = self
            .admitted
            .lock()
            .map_err(|_| "live turn admission was poisoned".to_owned())?;
        admitted.broken = true;
        Ok(())
    }
}

fn observe(observer: RuntimeObserver) -> Result<(), String> {
    let RuntimeObserver {
        children,
        runtimes,
        operation_id,
        expected_model,
        session_id,
        handle,
        observations,
        writer,
        reports,
        stdout,
        stderr,
    } = observer;
    let mut pump = match OutputPump::new(stdout, stderr) {
        Ok(pump) => pump,
        Err(error) => {
            let cleanup = cleanup(
                &children,
                &runtimes,
                operation_id,
                &handle,
                writer,
                CleanupMode::Terminate,
            );
            return publish_after_cleanup(
                &observations,
                TargetLiveObservation::Indeterminate(error),
                cleanup,
            );
        }
    };
    let mut codec = Codec::default();
    let terminal = loop {
        let result = consume_reports(&reports, &mut codec)
            .and_then(|()| pump.poll().map_err(ConversationError::from))
            .and_then(|()| consume_reports(&reports, &mut codec))
            .and_then(|()| {
                consume_output(
                    &mut pump,
                    &mut codec,
                    &expected_model,
                    session_id,
                    &observations,
                )
            });
        if let Err(error) = result {
            let terminal = terminal_from_error(&observations, error)?;
            let cleanup = cleanup(
                &children,
                &runtimes,
                operation_id,
                &handle,
                writer,
                CleanupMode::Terminate,
            );
            return publish_after_cleanup(&observations, terminal, cleanup);
        }
        match children.status(operation_id) {
            Ok(Some(status)) => {
                let drain = pump
                    .drain_ready()
                    .map_err(ConversationError::from)
                    .and_then(|()| {
                        consume_reports(&reports, &mut codec).and_then(|()| {
                            consume_output(
                                &mut pump,
                                &mut codec,
                                &expected_model,
                                session_id,
                                &observations,
                            )
                        })
                    });
                if let Err(error) = drain {
                    let terminal = terminal_from_error(&observations, error)?;
                    let cleanup = cleanup(
                        &children,
                        &runtimes,
                        operation_id,
                        &handle,
                        writer,
                        CleanupMode::ObservedExit,
                    );
                    return publish_after_cleanup(&observations, terminal, cleanup);
                }
                let observation = if handle.cancelled.load(Ordering::SeqCst) {
                    TargetLiveObservation::Cancelled
                } else if status.success()
                    && handle.graceful.load(Ordering::SeqCst)
                    && codec.all_completed()
                {
                    TargetLiveObservation::Exited
                } else {
                    TargetLiveObservation::Indeterminate(typed_message(
                        "direct live child exited before a graceful completed conversation"
                            .to_owned(),
                    ))
                };
                break observation;
            }
            Ok(None) => {}
            Err(error) => {
                let cleanup = cleanup(
                    &children,
                    &runtimes,
                    operation_id,
                    &handle,
                    writer,
                    CleanupMode::Terminate,
                );
                return publish_after_cleanup(
                    &observations,
                    TargetLiveObservation::Indeterminate(error),
                    cleanup,
                );
            }
        }
    };
    let cleanup = cleanup(
        &children,
        &runtimes,
        operation_id,
        &handle,
        writer,
        CleanupMode::ObservedExit,
    );
    publish_after_cleanup(&observations, terminal, cleanup)
}

fn publish_after_cleanup(
    observations: &Sender<TargetLiveObservation>,
    terminal: TargetLiveObservation,
    cleanup: CleanupResult,
) -> Result<(), String> {
    let terminal = match cleanup.result {
        Ok(()) => terminal,
        Err(error) => match cleanup.proof {
            ChildExitEvidence::Proven => TargetLiveObservation::Indeterminate(format!(
                "direct child exited but live runtime cleanup failed: {error}"
            )),
            ChildExitEvidence::Unproven => TargetLiveObservation::UnclassifiedWriter(format!(
                "live provider terminal classification lost direct-child cleanup proof: {error}"
            )),
        },
    };
    publish(observations, terminal)
}

struct CleanupResult {
    result: Result<(), String>,
    proof: ChildExitEvidence,
}

fn combine_results(
    root: String,
    first: Result<(), String>,
    second: Result<(), String>,
) -> Result<(), String> {
    match (first, second) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => {
            Err(format!("{root}; cleanup failed: {error}"))
        }
        (Err(first), Err(second)) => Err(format!(
            "{root}; first cleanup failure: {first}; second cleanup failure: {second}"
        )),
    }
}

fn combine_startup_errors(root: String, cleanup: Result<(), String>) -> String {
    match cleanup {
        Ok(()) => root,
        Err(error) => format!("{root}; startup cleanup failed: {error}"),
    }
}

fn startup_error(root: String, cleanup: CleanupResult) -> Result<(), TargetLiveStartError> {
    let message = combine_startup_errors(root, cleanup.result);
    let error = match cleanup.proof {
        ChildExitEvidence::Proven => TargetLiveStartError::CleanupProvenExited(message),
        ChildExitEvidence::Unproven => TargetLiveStartError::CleanupUnproven(message),
    };
    Err(error)
}

enum CleanupMode {
    Terminate,
    ObservedExit,
}

fn cleanup(
    children: &Children,
    runtimes: &LiveRuntimes,
    operation_id: TargetOperationId,
    handle: &LiveHandle,
    writer: thread::JoinHandle<()>,
    mode: CleanupMode,
) -> CleanupResult {
    let (writer_stop, child_exit, proof) = match mode {
        CleanupMode::Terminate => {
            let writer_stop = handle.stop_writer_cooperatively();
            let termination = children.terminate(operation_id).map(|_| ());
            let wait = children.wait(operation_id).map(|_| ());
            let proof = match &wait {
                Ok(()) => ChildExitEvidence::Proven,
                Err(_) => ChildExitEvidence::Unproven,
            };
            (
                writer_stop,
                combine_many("direct live child cleanup", [termination, wait]),
                proof,
            )
        }
        CleanupMode::ObservedExit => (
            handle.stop_writer_cooperatively(),
            Ok(()),
            ChildExitEvidence::Proven,
        ),
    };
    let writer_exit = writer
        .join()
        .map_err(|_| "live input writer panicked".to_owned());
    let child_remove = children.remove(operation_id);
    let runtime_remove = runtimes.remove(operation_id);
    CleanupResult {
        result: combine_many(
            "live runtime cleanup",
            [
                writer_stop,
                child_exit,
                writer_exit,
                child_remove,
                runtime_remove,
            ],
        ),
        proof,
    }
}

fn combine_many<const COUNT: usize>(
    root: &str,
    results: [Result<(), String>; COUNT],
) -> Result<(), String> {
    let errors: Vec<String> = results.into_iter().filter_map(Result::err).collect();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(format!("{root}: {}", errors.join("; ")))
    }
}

fn terminal_from_error(
    observations: &Sender<TargetLiveObservation>,
    error: ConversationError,
) -> Result<TargetLiveObservation, String> {
    match error {
        ConversationError::Failed { turn_id, message } => {
            let causal = typed_message(message);
            publish(
                observations,
                TargetLiveObservation::TurnFailed {
                    turn_id,
                    message: causal.clone(),
                },
            )?;
            Ok(TargetLiveObservation::Failed(causal))
        }
        ConversationError::Indeterminate(error) => {
            Ok(TargetLiveObservation::Indeterminate(typed_message(error)))
        }
    }
}

enum ConversationError {
    Failed {
        turn_id: TargetTurnId,
        message: String,
    },
    Indeterminate(String),
}

impl From<String> for ConversationError {
    fn from(error: String) -> Self {
        Self::Indeterminate(error)
    }
}

impl From<super::conversation_codec::CodecError> for ConversationError {
    fn from(error: super::conversation_codec::CodecError) -> Self {
        match error {
            super::conversation_codec::CodecError::Failed { turn_id, message } => {
                Self::Failed { turn_id, message }
            }
            super::conversation_codec::CodecError::Indeterminate(error) => {
                Self::Indeterminate(error)
            }
        }
    }
}

fn consume_reports(
    reports: &Receiver<WriterReport>,
    codec: &mut Codec,
) -> Result<(), ConversationError> {
    loop {
        match reports.try_recv() {
            Ok(WriterReport::Prepared(turn)) => codec.register_turn(turn)?,
            Ok(WriterReport::Closed) => {}
            Ok(WriterReport::Failed(error)) => return Err(error.into()),
            Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => return Ok(()),
        }
    }
}

fn consume_output(
    pump: &mut OutputPump,
    codec: &mut Codec,
    expected_model: &str,
    session_id: crate::contract::target::TargetSessionId,
    observations: &Sender<TargetLiveObservation>,
) -> Result<(), ConversationError> {
    for line in pump.take_stdout_lines()? {
        for observation in codec.observe(&line, expected_model, session_id)? {
            publish(observations, observation)?;
        }
    }
    Ok(())
}

fn publish(
    observations: &Sender<TargetLiveObservation>,
    observation: TargetLiveObservation,
) -> Result<(), String> {
    observations
        .send(observation)
        .map_err(|_| "Control stopped receiving live target observations".to_owned())
}

fn typed_message(message: String) -> String {
    message
}

fn stop_spawned(mut child: std::process::Child, root: &str) -> TargetLiveStartError {
    let termination = child.kill();
    let exit = child.wait();
    match (termination, exit) {
        (Ok(()), Ok(_)) => TargetLiveStartError::CleanupProvenExited(root.to_owned()),
        (Err(termination), Ok(_)) => TargetLiveStartError::CleanupProvenExited(format!(
            "{root}; direct child termination request failed after observed exit: {termination}"
        )),
        (Ok(()), Err(exit)) => TargetLiveStartError::CleanupUnproven(format!(
            "{root}; direct child exit could not be observed: {exit}"
        )),
        (Err(termination), Err(exit)) => TargetLiveStartError::CleanupUnproven(format!(
            "{root}; direct child termination failed: {termination}; direct child exit could not be observed: {exit}"
        )),
    }
}

fn live_start_error_from_registration(
    error: super::child::ChildRegistrationError,
) -> TargetLiveStartError {
    match error.exit_evidence() {
        ChildExitEvidence::Proven => {
            TargetLiveStartError::CleanupProvenExited(error.message().to_owned())
        }
        ChildExitEvidence::Unproven => {
            TargetLiveStartError::CleanupUnproven(error.message().to_owned())
        }
    }
}

fn stop_registered_child(
    children: &Children,
    operation_id: TargetOperationId,
    root: String,
) -> TargetLiveStartError {
    let termination = children.terminate(operation_id);
    let wait = children.wait(operation_id).map(|_| ());
    let removal = children.remove(operation_id);
    let observed_exit = wait.is_ok();
    let errors = [termination.map(|_| ()), wait, removal]
        .into_iter()
        .filter_map(Result::err)
        .collect::<Vec<_>>();
    let message = if errors.is_empty() {
        root
    } else {
        format!("{root}; direct-child cleanup failed: {}", errors.join("; "))
    };
    if observed_exit {
        TargetLiveStartError::CleanupProvenExited(message)
    } else {
        TargetLiveStartError::CleanupUnproven(message)
    }
}

fn append_startup_cleanup(
    cleanup: TargetLiveStartError,
    follow_up: Result<(), String>,
) -> TargetLiveStartError {
    let message = combine_startup_errors(cleanup.message().to_owned(), follow_up);
    match cleanup {
        TargetLiveStartError::NoWriter(_) | TargetLiveStartError::CleanupProvenExited(_) => {
            TargetLiveStartError::CleanupProvenExited(message)
        }
        TargetLiveStartError::CleanupUnproven(_) => TargetLiveStartError::CleanupUnproven(message),
    }
}
