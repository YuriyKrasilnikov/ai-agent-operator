// Copyright 2026 Yuriy Krasilnikov
// SPDX-License-Identifier: Apache-2.0

//! Owns the exclusive lifetime lease for one SQLite state file.

use std::{
    fs::{File, OpenOptions, TryLockError},
    path::Path,
};

use crate::contract::control::OperatorError;

pub(crate) struct StateFileLease {
    _file: File,
}

impl StateFileLease {
    pub(crate) fn acquire(path: &Path) -> Result<Self, OperatorError> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .truncate(false)
            .write(true)
            .open(path)
            .map_err(|error| OperatorError::State(error.to_string()))?;
        match file.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                return Err(OperatorError::State(format!(
                    "SQLite state file {} is already owned",
                    path.display()
                )));
            }
            Err(TryLockError::Error(error)) => {
                return Err(OperatorError::State(format!(
                    "SQLite state file {} could not be locked: {error}",
                    path.display()
                )));
            }
        }
        Ok(Self { _file: file })
    }
}
