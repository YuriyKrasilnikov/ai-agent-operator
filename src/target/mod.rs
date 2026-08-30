// Copyright 2026 Yuriy Krasilnikov
// SPDX-License-Identifier: Apache-2.0

//! Target Execution composition. Each child module owns one direct-child concern.

mod child;
mod command;
mod execution;
mod launch;
mod output;
mod prompt_input;
mod stream;

pub use execution::ClaudeTarget;
