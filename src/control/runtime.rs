// Copyright 2026 Yuriy Krasilnikov
// SPDX-License-Identifier: Apache-2.0

//! Serializes launch and cancellation intent for one accepted operation.

use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use crate::contract::control::{OperationId, OperatorError};

pub enum CancellationOutcome {
    Signalled,
    GateAbsent,
}

#[derive(Clone, Default)]
pub struct RuntimeGate {
    slots: Arc<Mutex<HashMap<OperationId, Arc<AtomicBool>>>>,
}

impl RuntimeGate {
    pub fn admit(&self, operation: OperationId) -> Result<Arc<AtomicBool>, OperatorError> {
        let token = Arc::new(AtomicBool::new(false));
        let mut slots = self
            .slots
            .lock()
            .map_err(|_| OperatorError::State("operation runtime gate was poisoned".to_owned()))?;
        if slots.insert(operation, Arc::clone(&token)).is_some() {
            return Err(OperatorError::State(
                "operation runtime gate already exists".to_owned(),
            ));
        }
        Ok(token)
    }

    pub fn cancel(&self, operation: OperationId) -> Result<CancellationOutcome, OperatorError> {
        let slots = self
            .slots
            .lock()
            .map_err(|_| OperatorError::State("operation runtime gate was poisoned".to_owned()))?;
        match slots.get(&operation) {
            Some(token) => {
                token.store(true, Ordering::SeqCst);
                Ok(CancellationOutcome::Signalled)
            }
            None => Ok(CancellationOutcome::GateAbsent),
        }
    }

    pub fn cancelled(token: &AtomicBool) -> bool {
        token.load(Ordering::SeqCst)
    }

    pub fn release(&self, operation: OperationId) -> Result<(), OperatorError> {
        let mut slots = self
            .slots
            .lock()
            .map_err(|_| OperatorError::State("operation runtime gate was poisoned".to_owned()))?;
        if slots.remove(&operation).is_none() {
            return Err(OperatorError::State(
                "operation runtime gate disappeared".to_owned(),
            ));
        }
        Ok(())
    }
}
