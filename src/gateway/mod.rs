// Copyright 2026 Yuriy Krasilnikov
// SPDX-License-Identifier: Apache-2.0

//! Initiator Gateway composition.

use serde::Serialize;

pub mod client;
pub mod mcp;
pub mod session;

pub use client::call;

#[derive(Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "data")]
pub(crate) enum GatewayError {
    InvalidArguments {
        message: String,
    },
    GatewayFailure {
        message: String,
    },
    Operator {
        error: crate::contract::control::OperatorError,
        message: String,
    },
}

impl GatewayError {
    pub(crate) fn invalid(message: String) -> Self {
        Self::InvalidArguments { message }
    }

    pub(crate) fn failure(message: String) -> Self {
        Self::GatewayFailure { message }
    }

    pub(crate) fn operator(error: crate::contract::control::OperatorError) -> Self {
        let message = error.to_string();
        Self::Operator { error, message }
    }
}
