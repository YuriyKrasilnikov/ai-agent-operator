// Copyright 2026 Yuriy Krasilnikov
// SPDX-License-Identifier: Apache-2.0

//! Coordinates one launch, input, output interpretation, and direct-child exit.

use std::{sync::atomic::Ordering, thread};

use crate::contract::target::{TargetCommand, TargetOperationId, TargetOutcome, TargetPort};

use super::{
    child::Children,
    launch,
    output::OutputPump,
    prompt_input,
    stream::{Evidence, InterpretationError},
};

#[derive(Clone, Default)]
pub struct ClaudeTarget {
    children: Children,
}

impl TargetPort for ClaudeTarget {
    fn execute(&self, command: TargetCommand) -> TargetOutcome {
        if command.cancel_requested.load(Ordering::SeqCst) {
            return match command
                .launch_report
                .send(crate::contract::target::TargetLaunch::CancelledBeforeLaunch)
            {
                Ok(()) => TargetOutcome::CancelledBeforeLaunch(
                    "cancellation was observed before direct Claude launch".to_owned(),
                ),
                Err(_) => TargetOutcome::Indeterminate(
                    "Control stopped waiting before pre-launch cancellation could be reported"
                        .to_owned(),
                ),
            };
        }
        let mut child = match launch::spawn(&command) {
            Ok(child) => child,
            Err(error) => {
                match command.launch_report.send(
                    crate::contract::target::TargetLaunch::SpawnFailed(error.clone()),
                ) {
                    Ok(()) => {}
                    Err(_) => return TargetOutcome::Indeterminate(
                        "Control stopped waiting before target launch failure could be reported"
                            .to_owned(),
                    ),
                }
                return TargetOutcome::SpawnFailed(error);
            }
        };
        let stdin = match child.stdin.take() {
            Some(stdin) => stdin,
            None => {
                return TargetOutcome::Indeterminate(
                    "direct child stdin was not captured".to_owned(),
                );
            }
        };
        let stdout = match child.stdout.take() {
            Some(stdout) => stdout,
            None => {
                return TargetOutcome::Indeterminate(
                    "direct child stdout was not captured".to_owned(),
                );
            }
        };
        let stderr = match child.stderr.take() {
            Some(stderr) => stderr,
            None => {
                return TargetOutcome::Indeterminate(
                    "direct child stderr was not captured".to_owned(),
                );
            }
        };
        match self.children.insert(command.operation_id, child) {
            Ok(_) => {}
            Err(error) => return TargetOutcome::Indeterminate(error),
        };
        if command
            .launch_report
            .send(crate::contract::target::TargetLaunch::Launched)
            .is_err()
        {
            return self.stop_before_running(
                command.operation_id,
                "Control stopped waiting before durable Running acknowledgement".to_owned(),
            );
        }
        match command.running_permission.recv() {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return self.stop_before_running(command.operation_id, error),
            Err(_) => {
                return self.stop_before_running(
                    command.operation_id,
                    "Control did not grant durable Running acknowledgement".to_owned(),
                );
            }
        }
        let input = prompt_input::start(stdin, command.prompt.clone());
        let outcome = self.observe(command.operation_id, &command, input, stdout, stderr);
        match self.children.remove(command.operation_id) {
            Ok(()) => outcome,
            Err(error) => TargetOutcome::Indeterminate(error),
        }
    }

    fn cancel(&self, operation_id: TargetOperationId) -> Result<(), String> {
        self.children.terminate(operation_id)
    }
}

impl ClaudeTarget {
    fn stop_before_running(&self, operation: TargetOperationId, reason: String) -> TargetOutcome {
        match self.children.terminate(operation) {
            Ok(()) => match self.children.wait(operation) {
                Ok(_) => match self.children.remove(operation) {
                    Ok(()) => TargetOutcome::Indeterminate(reason),
                    Err(error) => TargetOutcome::Indeterminate(error),
                },
                Err(error) => TargetOutcome::Indeterminate(error),
            },
            Err(error) => TargetOutcome::Indeterminate(error),
        }
    }
    fn observe(
        &self,
        operation: TargetOperationId,
        command: &TargetCommand,
        input: thread::JoinHandle<Result<(), String>>,
        stdout: std::process::ChildStdout,
        stderr: std::process::ChildStderr,
    ) -> TargetOutcome {
        let mut pump = match OutputPump::new(stdout, stderr) {
            Ok(pump) => pump,
            Err(error) => {
                return match self.children.terminate(operation) {
                    Ok(()) => TargetOutcome::Indeterminate(error),
                    Err(termination_error) => TargetOutcome::Indeterminate(format!(
                        "{error}; direct child could not be terminated: {termination_error}"
                    )),
                };
            }
        };
        let mut terminal_error = None;
        let mut provider_failure = None;
        let mut evidence = Evidence::default();
        let status = loop {
            match pump.poll() {
                Ok(()) => {}
                Err(error) => {
                    terminal_error = Some(self.terminate_after_error(operation, error));
                }
            }
            let interpretation = interpret(&mut pump, &mut evidence, command);
            match interpretation {
                Ok(()) => {}
                Err(InterpretationError::ProviderFailure(error)) => {
                    provider_failure = Some(error);
                    if let Err(termination_error) = self.children.terminate(operation) {
                        terminal_error = Some(termination_error);
                    }
                }
                Err(InterpretationError::Ambiguous(error)) => {
                    terminal_error = Some(self.terminate_after_error(operation, error));
                }
            }
            match self.children.status(operation) {
                Ok(Some(status)) => {
                    if let Err(error) = pump.drain_ready() {
                        terminal_error = Some(error);
                    }
                    if let Err(error) = interpret(&mut pump, &mut evidence, command) {
                        match error {
                            InterpretationError::ProviderFailure(message) => {
                                provider_failure = Some(message)
                            }
                            InterpretationError::Ambiguous(message) => {
                                terminal_error = Some(message)
                            }
                        }
                    }
                    break (status, evidence);
                }
                Ok(None) => {}
                Err(error) => {
                    terminal_error = Some(self.terminate_after_error(operation, error));
                }
            }
            if command.cancel_requested.load(Ordering::SeqCst)
                && let Err(error) = self.children.terminate(operation)
            {
                terminal_error = Some(error);
            }
        };
        if command.cancel_requested.load(Ordering::SeqCst) {
            return TargetOutcome::Cancelled(
                "direct Claude child termination was observed".to_owned(),
            );
        }
        if let Some(error) = provider_failure {
            return TargetOutcome::Failed(with_stderr(error, &pump));
        }
        if let Some(error) = terminal_error {
            return TargetOutcome::Indeterminate(with_stderr(error, &pump));
        }
        if !status.0.success() {
            return TargetOutcome::Failed(with_stderr(
                "direct Claude child exited unsuccessfully".to_owned(),
                &pump,
            ));
        }
        match input.join() {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return TargetOutcome::Indeterminate(with_stderr(error, &pump)),
            Err(_) => {
                return TargetOutcome::Indeterminate(with_stderr(
                    "prompt writer panicked".to_owned(),
                    &pump,
                ));
            }
        }
        match status.1.success(command) {
            Ok(success) => TargetOutcome::Success(success),
            Err(InterpretationError::ProviderFailure(error)) => {
                TargetOutcome::Failed(with_stderr(error, &pump))
            }
            Err(InterpretationError::Ambiguous(error)) => {
                TargetOutcome::Indeterminate(with_stderr(error, &pump))
            }
        }
    }

    fn terminate_after_error(&self, operation: TargetOperationId, root: String) -> String {
        match self.children.terminate(operation) {
            Ok(()) => root,
            Err(termination) => {
                format!("{root}; direct-child termination failed: {termination}")
            }
        }
    }
}

fn interpret(
    pump: &mut OutputPump,
    evidence: &mut Evidence,
    command: &TargetCommand,
) -> Result<(), InterpretationError> {
    for line in pump
        .take_stdout_lines()
        .map_err(InterpretationError::Ambiguous)?
    {
        evidence.observe(&line, command)?;
    }
    Ok(())
}

fn with_stderr(message: String, pump: &OutputPump) -> String {
    let stderr = pump.stderr();
    if stderr.is_empty() {
        message
    } else {
        format!("{message}: {stderr}")
    }
}
