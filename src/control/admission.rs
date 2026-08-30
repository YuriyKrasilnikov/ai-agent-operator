// Copyright 2026 Yuriy Krasilnikov
// SPDX-License-Identifier: Apache-2.0

//! Validates complete commands before any durable lookup or target effect.

use crate::contract::control::{OperationStart, OperatorError};

pub fn validate_start(start: &OperationStart) -> Result<(), OperatorError> {
    start.validate()
}
