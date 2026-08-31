// Copyright 2026 Yuriy Krasilnikov
// SPDX-License-Identifier: Apache-2.0

//! Returns atomic one-shot diagnostic snapshots after an independent cursor.

use std::{thread, time::Duration};

use crate::contract::control::{OperationDiagnostics, OperationDiagnosticsRequest, OperatorError};

use super::OperationControl;

const DIAGNOSTIC_POLL: Duration = Duration::from_millis(10);

impl OperationControl {
    pub(super) fn diagnostics(
        &self,
        request: OperationDiagnosticsRequest,
    ) -> Result<OperationDiagnostics, OperatorError> {
        request.validate()?;
        let deadline = std::time::Instant::now()
            .checked_add(Duration::from_millis(request.wait_millis))
            .ok_or_else(|| {
                OperatorError::InvalidRequest(
                    "diagnostic wait duration cannot be represented".to_owned(),
                )
            })?;
        loop {
            self.refusal()?;
            let snapshot = self.state.get_operation_diagnostics(
                request.operation_id,
                request.after_diagnostic_sequence,
            )?;
            if !snapshot.diagnostics.is_empty()
                || snapshot.operation.state.terminal()
                || std::time::Instant::now() >= deadline
            {
                return Ok(snapshot);
            }
            thread::sleep(DIAGNOSTIC_POLL);
        }
    }
}
