// Copyright 2026 Yuriy Krasilnikov
// SPDX-License-Identifier: Apache-2.0

//! Orders durable acceptance, direct execution, cancellation, and terminalization.

use std::{
    sync::mpsc,
    sync::{Arc, OnceLock},
    thread,
    time::Duration,
};

use sha2::{Digest, Sha256};

use crate::contract::{
    control::{
        ConversationId, ConversationStopMode, Operation, OperationId, OperationStart,
        OperationState, OperatorError, ProjectRegistration, SessionId, StatePort, TerminalOutcome,
    },
    target::{
        TargetCommand, TargetIntent, TargetLaunch, TargetOperationId, TargetOutcome, TargetPort,
        TargetSessionId,
    },
};

use super::{
    admission, conversation, project,
    runtime::{CancellationOutcome, RuntimeGate},
    session_writer,
};

const OPERATION_STATE_POLL: Duration = Duration::from_millis(10);

#[derive(Clone)]
pub struct OperationControl {
    pub(super) state: Arc<dyn StatePort>,
    pub(super) target: Arc<dyn TargetPort>,
    pub(super) runtime: RuntimeGate,
    pub(super) refusal: Arc<OnceLock<OperatorError>>,
}

enum WorkerCompletion {
    Normal,
    CancelledBeforeLaunch(Operation),
}

struct WorkerRequest {
    operation_id: OperationId,
    project: ProjectRegistration,
    start: OperationStart,
    cancel: Arc<std::sync::atomic::AtomicBool>,
    launch_report: mpsc::Sender<TargetLaunch>,
    running_permission: mpsc::Receiver<Result<(), String>>,
    completion: mpsc::Sender<Result<Operation, OperatorError>>,
}

impl OperationControl {
    pub(super) fn start(&self, start: OperationStart) -> Result<Operation, OperatorError> {
        admission::validate_start(&start)?;
        let project = project::resolve(self.state.as_ref(), &start.project_id)?;
        let fingerprint = fingerprint(&start)?;
        let (operation, inserted) =
            session_writer::admit(self.state.as_ref(), &start, &fingerprint)?;
        if !inserted {
            return Ok(operation);
        }
        let cancel = self.runtime.admit(operation.operation_id)?;
        let (launch_sender, launch_receiver) = mpsc::channel();
        let (running_sender, running_receiver) = mpsc::channel();
        let (completion_sender, completion_receiver) = mpsc::channel();
        let control = self.clone();
        let worker_start = start.clone();
        let worker_operation = operation.operation_id;
        let spawn = thread::Builder::new()
            .name(format!("aiop-operation-{}", worker_operation.value()))
            .spawn(move || {
                control.execute(WorkerRequest {
                    operation_id: worker_operation,
                    project,
                    start: worker_start,
                    cancel,
                    launch_report: launch_sender,
                    running_permission: running_receiver,
                    completion: completion_sender,
                });
            });
        if let Err(error) = spawn {
            let transition = self.state.transition(
                operation.operation_id,
                OperationState::Failed,
                Some(TerminalOutcome::Failed(format!(
                    "operation worker could not start: {error}"
                ))),
                None,
                None,
                None,
            );
            self.runtime.release(operation.operation_id)?;
            return transition;
        }
        match launch_receiver.recv() {
            Ok(TargetLaunch::Launched) => match self.state.transition(
                operation.operation_id,
                OperationState::Running,
                None,
                None,
                None,
                None,
            ) {
                Ok(running) => match running_sender.send(Ok(())) {
                    Ok(()) => Ok(running),
                    Err(_) => Err(OperatorError::Indeterminate(
                        "target stopped after direct child launch before durable execution"
                            .to_owned(),
                    )),
                },
                Err(error) => {
                    match running_sender.send(Err(error.to_string())) {
                        Ok(()) | Err(_) => {}
                    }
                    Err(error)
                }
            },
            Ok(TargetLaunch::SpawnFailed(message)) => {
                let failed = self.state.transition(
                    operation.operation_id,
                    OperationState::Failed,
                    Some(TerminalOutcome::Failed(message)),
                    None,
                    None,
                    None,
                )?;
                Ok(failed)
            }
            Ok(TargetLaunch::CancelledBeforeLaunch) => match completion_receiver.recv() {
                Ok(Ok(cancelled)) => Ok(cancelled),
                Ok(Err(error)) => Err(error),
                Err(_) => Err(OperatorError::Indeterminate(
                    "pre-launch cancellation was reported without a durable worker result"
                        .to_owned(),
                )),
            },
            Err(_) => {
                let outcome = self.state.transition(
                    operation.operation_id,
                    OperationState::Indeterminate,
                    Some(TerminalOutcome::Indeterminate(
                        "target ended before proving launch state".to_owned(),
                    )),
                    None,
                    None,
                    None,
                )?;
                Ok(outcome)
            }
        }
    }

    fn execute(&self, worker: WorkerRequest) {
        let completion = worker.completion;
        let operation_id = worker.operation_id;
        let terminal = if RuntimeGate::cancelled(&worker.cancel) {
            let cancellation = self.transition(
                operation_id,
                OperationState::Cancelled,
                TerminalOutcome::Cancelled(
                    "operation cancellation was observed before target launch".to_owned(),
                ),
                None,
            );
            match cancellation {
                Ok(()) => match self.state.get_operation(operation_id) {
                    Ok(operation) => {
                        match worker.launch_report.send(TargetLaunch::CancelledBeforeLaunch) {
                            Ok(()) => Ok(WorkerCompletion::CancelledBeforeLaunch(operation)),
                            Err(_) => Err(OperatorError::State(
                                "operation starter stopped before durable cancellation acknowledgement"
                                    .to_owned(),
                            )),
                        }
                    }
                    Err(error) => Err(error),
                },
                Err(error) => Err(error),
            }
        } else {
            match self.state.get_operation(operation_id) {
                Ok(operation) => self.run_target(
                    operation,
                    worker.project,
                    worker.start,
                    worker.cancel,
                    worker.launch_report,
                    worker.running_permission,
                ),
                Err(error) => Err(error),
            }
        };
        match terminal {
            Ok(WorkerCompletion::Normal) => {}
            Ok(WorkerCompletion::CancelledBeforeLaunch(operation)) => {
                if completion.send(Ok(operation)).is_err() {
                    eprintln!("operation starter stopped before durable cancellation result");
                }
            }
            Err(error) => {
                if completion.send(Err(error.clone())).is_err() {
                    eprintln!("operation starter stopped before worker failure result");
                }
                self.record_refusal(error);
            }
        }
        if let Err(error) = self.runtime.release(operation_id) {
            self.record_refusal(error);
        }
    }

    fn run_target(
        &self,
        operation: Operation,
        project: ProjectRegistration,
        start: OperationStart,
        cancel: Arc<std::sync::atomic::AtomicBool>,
        launch_report: mpsc::Sender<TargetLaunch>,
        running_permission: mpsc::Receiver<Result<(), String>>,
    ) -> Result<WorkerCompletion, OperatorError> {
        let command = TargetCommand {
            operation_id: TargetOperationId(operation.operation_id.value()),
            working_directory: project.working_directory,
            executable: project.claude_executable,
            expected_model: project.expected_opus_model,
            intent: map_intent(&start.intent),
            session_id: TargetSessionId(operation.session_id.value()),
            prompt: start.prompt,
            cancel_requested: cancel,
            launch_report,
            running_permission,
        };
        match self.target.execute(command) {
            TargetOutcome::SpawnFailed(_) => Ok(WorkerCompletion::Normal),
            TargetOutcome::Success(success) => self
                .transition(
                    operation.operation_id,
                    OperationState::Succeeded,
                    TerminalOutcome::Succeeded(success.result),
                    Some((
                        SessionId::new_exact(success.observed_session_id.0),
                        success.observed_model,
                        success.observed_version,
                    )),
                )
                .map(|()| WorkerCompletion::Normal),
            TargetOutcome::Failed(message) => self
                .transition(
                    operation.operation_id,
                    OperationState::Failed,
                    TerminalOutcome::Failed(message),
                    None,
                )
                .map(|()| WorkerCompletion::Normal),
            TargetOutcome::Cancelled(message) => self
                .transition(
                    operation.operation_id,
                    OperationState::Cancelled,
                    TerminalOutcome::Cancelled(message),
                    None,
                )
                .map(|()| WorkerCompletion::Normal),
            TargetOutcome::CancelledBeforeLaunch(message) => {
                self.transition(
                    operation.operation_id,
                    OperationState::Cancelled,
                    TerminalOutcome::Cancelled(message),
                    None,
                )?;
                self.state
                    .get_operation(operation.operation_id)
                    .map(WorkerCompletion::CancelledBeforeLaunch)
            }
            TargetOutcome::Indeterminate(message) => self
                .transition(
                    operation.operation_id,
                    OperationState::Indeterminate,
                    TerminalOutcome::Indeterminate(message),
                    None,
                )
                .map(|()| WorkerCompletion::Normal),
        }
    }

    fn transition(
        &self,
        operation: OperationId,
        state: OperationState,
        terminal: TerminalOutcome,
        observed: Option<(SessionId, String, Option<String>)>,
    ) -> Result<(), OperatorError> {
        let (session, model, version) = match observed {
            Some((session, model, version)) => (Some(session), Some(model), version),
            None => (None, None, None),
        };
        self.state
            .transition(operation, state, Some(terminal), session, model, version)
            .map(|_| ())
    }

    pub(super) fn cancel(&self, operation: OperationId) -> Result<Operation, OperatorError> {
        let current = self.state.get_operation(operation)?;
        if current.state.terminal() {
            return Ok(current);
        }
        match self.state.get_conversation(ConversationId::new(operation)) {
            Ok(_) => self.cancel_live_conversation(operation),
            Err(OperatorError::UnknownOperation(_)) => self.cancel_one_shot(operation),
            Err(error) => Err(error),
        }
    }

    fn cancel_live_conversation(&self, operation: OperationId) -> Result<Operation, OperatorError> {
        conversation::stop(
            self,
            ConversationId::new(operation),
            ConversationStopMode::Cancel,
        )?;
        self.wait_for_terminal_cancellation(operation)
    }

    fn cancel_one_shot(&self, operation: OperationId) -> Result<Operation, OperatorError> {
        match self.runtime.cancel(operation)? {
            CancellationOutcome::Signalled => self.wait_for_terminal_cancellation(operation),
            CancellationOutcome::GateAbsent => {
                let observed = self.state.get_operation(operation)?;
                if observed.state.terminal() {
                    Ok(observed)
                } else {
                    Err(OperatorError::State(
                        "nonterminal operation has no runtime cancellation gate".to_owned(),
                    ))
                }
            }
        }
    }

    fn wait_for_terminal_cancellation(
        &self,
        operation: OperationId,
    ) -> Result<Operation, OperatorError> {
        loop {
            self.refusal()?;
            let observed = self.state.get_operation(operation)?;
            if observed.state.terminal() {
                return Ok(observed);
            }
            thread::sleep(OPERATION_STATE_POLL);
        }
    }

    pub(super) fn wait(
        &self,
        operation: OperationId,
        wait_millis: u64,
    ) -> Result<Operation, OperatorError> {
        let deadline = std::time::Instant::now()
            .checked_add(Duration::from_millis(wait_millis))
            .ok_or_else(|| {
                OperatorError::InvalidRequest("wait duration cannot be represented".to_owned())
            })?;
        loop {
            self.refusal()?;
            let current = self.state.get_operation(operation)?;
            if current.state.terminal() || std::time::Instant::now() >= deadline {
                return Ok(current);
            }
            thread::sleep(OPERATION_STATE_POLL);
        }
    }

    pub(super) fn refusal(&self) -> Result<(), OperatorError> {
        match self.refusal.get() {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        }
    }

    pub(super) fn record_refusal(&self, error: OperatorError) {
        match self.refusal.set(error) {
            Ok(()) | Err(_) => {}
        }
    }
}

fn map_intent(intent: &crate::contract::control::OperationIntent) -> TargetIntent {
    match intent {
        crate::contract::control::OperationIntent::New => TargetIntent::New,
        crate::contract::control::OperationIntent::ResumeExact { session_id } => {
            TargetIntent::ResumeExact {
                session_id: TargetSessionId(session_id.value()),
            }
        }
    }
}

fn fingerprint(start: &OperationStart) -> Result<String, OperatorError> {
    let encoded =
        serde_json::to_vec(start).map_err(|error| OperatorError::State(error.to_string()))?;
    Ok(format!("{:x}", Sha256::digest(encoded)))
}
