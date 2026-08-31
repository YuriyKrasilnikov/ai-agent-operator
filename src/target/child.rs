// Copyright 2026 Yuriy Krasilnikov
// SPDX-License-Identifier: Apache-2.0

//! Owns direct-child registration, explicit termination, and exit observation.

use std::{
    collections::HashMap,
    process::{Child, ExitStatus},
    sync::{Arc, Mutex},
};

use crate::contract::target::TargetOperationId;

#[derive(Clone, Default)]
pub struct Children {
    children: Arc<Mutex<HashMap<TargetOperationId, Arc<Mutex<Child>>>>>,
}

pub(crate) struct ChildRegistrationError {
    message: String,
    exit_evidence: ChildExitEvidence,
}

/// States whether direct-child exit was observed during cleanup.
pub(crate) enum ChildExitEvidence {
    Proven,
    Unproven,
}

impl ChildRegistrationError {
    pub(crate) fn message(&self) -> &str {
        &self.message
    }

    pub(crate) fn exit_evidence(&self) -> &ChildExitEvidence {
        &self.exit_evidence
    }
}

impl Children {
    pub fn insert(
        &self,
        operation: TargetOperationId,
        child: Child,
    ) -> Result<Arc<Mutex<Child>>, ChildRegistrationError> {
        let mut children = match self.children.lock() {
            Ok(children) => children,
            Err(_) => {
                return Err(stop_unregistered(
                    child,
                    "direct-child registry was poisoned",
                ));
            }
        };
        if children.contains_key(&operation) {
            return Err(stop_unregistered(
                child,
                "operation already owns a direct child",
            ));
        }
        let child = Arc::new(Mutex::new(child));
        children.insert(operation, Arc::clone(&child));
        Ok(child)
    }

    pub fn remove(&self, operation: TargetOperationId) -> Result<(), String> {
        let mut children = self
            .children
            .lock()
            .map_err(|_| "direct-child registry was poisoned".to_owned())?;
        if children.remove(&operation).is_none() {
            return Err("direct child disappeared before execution completed".to_owned());
        }
        Ok(())
    }

    pub fn terminate(&self, operation: TargetOperationId) -> Result<(), String> {
        let child = self.child(operation)?;
        let mut child = child
            .lock()
            .map_err(|_| "direct child state was poisoned".to_owned())?;
        child
            .kill()
            .map_err(|error| format!("direct child could not be terminated: {error}"))
    }

    pub fn status(&self, operation: TargetOperationId) -> Result<Option<ExitStatus>, String> {
        let child = self.child(operation)?;
        let mut child = child
            .lock()
            .map_err(|_| "direct child state was poisoned".to_owned())?;
        child
            .try_wait()
            .map_err(|error| format!("direct child status could not be observed: {error}"))
    }

    pub fn wait(&self, operation: TargetOperationId) -> Result<ExitStatus, String> {
        let child = self.child(operation)?;
        let mut child = child
            .lock()
            .map_err(|_| "direct child state was poisoned".to_owned())?;
        child
            .wait()
            .map_err(|error| format!("direct child exit could not be observed: {error}"))
    }

    fn child(&self, operation: TargetOperationId) -> Result<Arc<Mutex<Child>>, String> {
        let children = self
            .children
            .lock()
            .map_err(|_| "direct-child registry was poisoned".to_owned())?;
        children
            .get(&operation)
            .cloned()
            .ok_or_else(|| "direct child is not active".to_owned())
    }
}

fn stop_unregistered(child: Child, root: &str) -> ChildRegistrationError {
    let mut child = child;
    let termination = child.kill();
    let exit = child.wait();
    match (termination, exit) {
        (Ok(()), Ok(_)) => ChildRegistrationError {
            message: root.to_owned(),
            exit_evidence: ChildExitEvidence::Proven,
        },
        (Err(termination), Ok(_)) => ChildRegistrationError {
            message: format!(
                "{root}; unregistered direct child termination request failed after observed exit: {termination}"
            ),
            exit_evidence: ChildExitEvidence::Proven,
        },
        (Ok(()), Err(exit)) => ChildRegistrationError {
            message: format!(
                "{root}; unregistered direct child exit could not be observed: {exit}"
            ),
            exit_evidence: ChildExitEvidence::Unproven,
        },
        (Err(termination), Err(exit)) => ChildRegistrationError {
            message: format!(
                "{root}; unregistered direct child termination failed: {termination}; direct child exit could not be observed: {exit}"
            ),
            exit_evidence: ChildExitEvidence::Unproven,
        },
    }
}
