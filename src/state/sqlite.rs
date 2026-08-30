// Copyright 2026 Yuriy Krasilnikov
// SPDX-License-Identifier: Apache-2.0

//! SS01: SQLite connection, schema creation, locking, and transactions.

use ::sqlite::{Connection, ConnectionThreadSafe};
use serde::{Serialize, de::DeserializeOwned};
use std::{
    path::Path,
    sync::{Arc, Mutex},
};

use super::ownership::StateFileLease;
use crate::contract::control::OperatorError;

#[derive(Clone)]
pub(crate) struct Adapter {
    _state_file: Arc<StateFileLease>,
    connection: Arc<Mutex<ConnectionThreadSafe>>,
}
impl Adapter {
    pub(crate) fn open(path: &Path) -> Result<Self, OperatorError> {
        let parent = path
            .parent()
            .ok_or_else(|| OperatorError::State("database path has no parent".to_owned()))?;
        std::fs::create_dir_all(parent).map_err(|e| OperatorError::State(e.to_string()))?;
        let state_file = Arc::new(StateFileLease::acquire(path)?);
        let connection = Connection::open_thread_safe(path).map_err(sql_error)?;
        for schema in [
            "CREATE TABLE IF NOT EXISTS projects (project_id TEXT PRIMARY KEY NOT NULL, record_json TEXT NOT NULL)",
            "CREATE TABLE IF NOT EXISTS operations (request_id TEXT PRIMARY KEY NOT NULL, operation_id TEXT UNIQUE NOT NULL, session_id TEXT NOT NULL, fingerprint TEXT NOT NULL, record_json TEXT NOT NULL)",
            "CREATE TABLE IF NOT EXISTS active_sessions (session_id TEXT PRIMARY KEY NOT NULL, operation_id TEXT UNIQUE NOT NULL)",
        ] {
            connection.execute(schema).map_err(sql_error)?;
        }
        Ok(Self {
            _state_file: state_file,
            connection: Arc::new(Mutex::new(connection)),
        })
    }
    pub(crate) fn transaction<T>(
        &self,
        action: impl FnOnce(&ConnectionThreadSafe) -> Result<T, OperatorError>,
    ) -> Result<T, OperatorError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| OperatorError::State("SQLite state mutex was poisoned".to_owned()))?;
        connection.execute("BEGIN IMMEDIATE").map_err(sql_error)?;
        match action(&connection) {
            Ok(v) => {
                connection.execute("COMMIT").map_err(sql_error)?;
                Ok(v)
            }
            Err(e) => match connection.execute("ROLLBACK") {
                Ok(()) => Err(e),
                Err(r) => Err(OperatorError::State(format!(
                    "{e}; SQLite rollback failed: {r}"
                ))),
            },
        }
    }
    pub(crate) fn read<T>(
        &self,
        action: impl FnOnce(&ConnectionThreadSafe) -> Result<T, OperatorError>,
    ) -> Result<T, OperatorError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| OperatorError::State("SQLite state mutex was poisoned".to_owned()))?;
        action(&connection)
    }
}

pub(crate) fn encode<T: Serialize>(value: &T) -> Result<String, OperatorError> {
    serde_json::to_string(value).map_err(|e| OperatorError::State(e.to_string()))
}
pub(crate) fn decode<T: DeserializeOwned>(value: String) -> Result<T, OperatorError> {
    serde_json::from_str(&value).map_err(|e| OperatorError::State(e.to_string()))
}
pub(crate) fn sql_error(error: ::sqlite::Error) -> OperatorError {
    OperatorError::State(error.to_string())
}
