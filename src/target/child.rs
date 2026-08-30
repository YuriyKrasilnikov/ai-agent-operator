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

impl Children {
    pub fn insert(
        &self,
        operation: TargetOperationId,
        child: Child,
    ) -> Result<Arc<Mutex<Child>>, String> {
        let child = Arc::new(Mutex::new(child));
        let mut children = self
            .children
            .lock()
            .map_err(|_| "direct-child registry was poisoned".to_owned())?;
        if children.insert(operation, Arc::clone(&child)).is_some() {
            return Err("operation already owns a direct child".to_owned());
        }
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
