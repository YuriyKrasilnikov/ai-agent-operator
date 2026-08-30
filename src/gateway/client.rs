// Copyright 2026 Yuriy Krasilnikov
// SPDX-License-Identifier: Apache-2.0

//! Typed local-daemon client without retries or target behavior.

use std::{
    io::{BufRead, BufReader, Write},
    os::unix::net::UnixStream,
    path::Path,
};

use crate::contract::control::{DaemonEnvelope, DaemonRequest, DaemonResponse, OperatorError};

pub fn call(endpoint: &Path, request: DaemonRequest) -> Result<DaemonResponse, OperatorError> {
    let mut stream = UnixStream::connect(endpoint)
        .map_err(|error| OperatorError::TransportUnavailable(error.to_string()))?;
    let encoded = serde_json::to_string(&request)
        .map_err(|error| OperatorError::Protocol(error.to_string()))?;
    stream
        .write_all(encoded.as_bytes())
        .map_err(|error| OperatorError::TransportUnavailable(error.to_string()))?;
    stream
        .write_all(b"\n")
        .map_err(|error| OperatorError::TransportUnavailable(error.to_string()))?;
    let mut response = String::new();
    let read = BufReader::new(stream)
        .read_line(&mut response)
        .map_err(|error| OperatorError::TransportUnavailable(error.to_string()))?;
    if read == 0 {
        return Err(OperatorError::TransportUnavailable(
            "daemon closed before sending a response".to_owned(),
        ));
    }
    let envelope: DaemonEnvelope = serde_json::from_str(&response).map_err(|error| {
        OperatorError::Protocol(format!("daemon response was invalid JSON: {error}"))
    })?;
    envelope.result
}
