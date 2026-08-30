// Copyright 2026 Yuriy Krasilnikov
// SPDX-License-Identifier: Apache-2.0

//! Operation Control composition and its C3-owned modules.

use crate::contract::control::{DaemonRequest, DaemonResponse, OperatorError, StatePort};
use crate::contract::target::TargetPort;
use std::sync::Arc;

pub mod ingress;

mod admission;
mod operation;
mod project;
mod runtime;
mod session_writer;

pub use operation::OperationControl;

impl OperationControl {
    pub fn new(state: Arc<dyn StatePort>, target: Arc<dyn TargetPort>) -> Self {
        Self {
            state,
            target,
            runtime: runtime::RuntimeGate::default(),
            refusal: Arc::new(std::sync::OnceLock::new()),
        }
    }

    /// Projects one daemon request to its single responsible Control capability.
    pub fn handle(&self, request: DaemonRequest) -> Result<DaemonResponse, OperatorError> {
        self.refusal()?;
        match request {
            DaemonRequest::ProjectRegister(project) => Ok(DaemonResponse::Project(
                project::register(self.state.as_ref(), project)?,
            )),
            DaemonRequest::ProjectGet { project_id } => Ok(DaemonResponse::Project(project::get(
                self.state.as_ref(),
                &project_id,
            )?)),
            DaemonRequest::ProjectList => Ok(DaemonResponse::Projects(project::list(
                self.state.as_ref(),
            )?)),
            DaemonRequest::OperationStart(start) => {
                Ok(DaemonResponse::Operation(self.start(start)?))
            }
            DaemonRequest::OperationGet { operation_id } => Ok(DaemonResponse::Operation(
                self.state.get_operation(operation_id)?,
            )),
            DaemonRequest::OperationWait {
                operation_id,
                wait_millis,
            } => Ok(DaemonResponse::Operation(
                self.wait(operation_id, wait_millis)?,
            )),
            DaemonRequest::OperationCancel { operation_id } => {
                Ok(DaemonResponse::Operation(self.cancel(operation_id)?))
            }
        }
    }
}
