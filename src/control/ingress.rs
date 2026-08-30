// Copyright 2026 Yuriy Krasilnikov
// SPDX-License-Identifier: Apache-2.0

//! Daemon-side Unix-domain inbound adapter owned by Operation Control.

use std::{
    io::{BufRead, BufReader, Write},
    os::unix::net::{UnixListener, UnixStream},
    path::Path,
    thread,
};

use crate::{
    contract::control::{DaemonEnvelope, DaemonRequest, OperatorError},
    control::OperationControl,
};

pub fn serve(path: &Path, control: OperationControl) -> Result<(), OperatorError> {
    let listener = UnixListener::bind(path).map_err(|error| {
        OperatorError::TransportUnavailable(format!(
            "daemon endpoint {} could not bind: {error}",
            path.display()
        ))
    })?;
    for connection in listener.incoming() {
        match connection {
            Ok(stream) => {
                let connection_control = control.clone();
                let spawn_result = thread::Builder::new()
                    .name("aiop-daemon-connection".to_owned())
                    .spawn(move || serve_connection(stream, connection_control));
                if let Err(error) = spawn_result {
                    return Err(OperatorError::TransportUnavailable(format!(
                        "daemon connection worker could not start: {error}"
                    )));
                }
            }
            Err(error) => {
                return Err(OperatorError::TransportUnavailable(format!(
                    "daemon endpoint could not accept: {error}"
                )));
            }
        }
    }
    Ok(())
}

fn serve_connection(mut stream: UnixStream, control: OperationControl) {
    let result = read_request(&stream).and_then(|request| control.handle(request));
    if let Err(error) = write_response(&mut stream, DaemonEnvelope { result }) {
        eprintln!("aiopd: daemon response could not be written: {error}");
    }
}

fn read_request(stream: &UnixStream) -> Result<DaemonRequest, OperatorError> {
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    let read = reader
        .read_line(&mut line)
        .map_err(|error| OperatorError::TransportUnavailable(error.to_string()))?;
    if read == 0 {
        return Err(OperatorError::Protocol(
            "daemon client closed before sending a request".to_owned(),
        ));
    }
    serde_json::from_str(&line).map_err(|error| {
        OperatorError::Protocol(format!("daemon request was invalid JSON: {error}"))
    })
}

fn write_response(stream: &mut UnixStream, response: DaemonEnvelope) -> Result<(), OperatorError> {
    let encoded = serde_json::to_string(&response)
        .map_err(|error| OperatorError::Protocol(error.to_string()))?;
    stream
        .write_all(encoded.as_bytes())
        .map_err(|error| OperatorError::TransportUnavailable(error.to_string()))?;
    stream
        .write_all(b"\n")
        .map_err(|error| OperatorError::TransportUnavailable(error.to_string()))
}
