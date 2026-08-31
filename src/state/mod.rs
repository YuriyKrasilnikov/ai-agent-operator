// Copyright 2026 Yuriy Krasilnikov
// SPDX-License-Identifier: Apache-2.0

//! SQLite State composition for durable project, operation, session, and conversation records.

mod conversation;
mod conversation_timeline;
mod conversation_turn;
mod diagnostic;
mod initiator_binding;
mod operation;
mod ownership;
mod project;
mod session_claim;
mod sqlite;

use std::path::Path;

use crate::contract::control::{
    BindingPersistence, Conversation, ConversationCloseAdmission, ConversationEvent,
    ConversationEventPayload, ConversationId, ConversationSend, ConversationSnapshot,
    ConversationStart, ConversationStartAdmission, ConversationState, ConversationStopMode,
    ConversationTurnAdmission, ConversationTurnObservation, InitiatorAgentIdentity,
    InitiatorBinding, InitiatorIdentity, InitiatorSessionIdentity, Operation, OperationAdmission,
    OperationDiagnostic, OperationDiagnosticPayload, OperationDiagnostics, OperationId,
    OperationStart, OperationState, OperatorError, ProjectId, ProjectRegistration,
    SessionClaimDisposition, SessionEvidence, SessionId, StatePort, TerminalOutcome, TurnId,
    TurnState,
};

#[derive(Clone)]
pub struct SqliteState {
    adapter: sqlite::Adapter,
}

impl SqliteState {
    pub fn open(path: &Path) -> Result<Self, OperatorError> {
        let state = Self {
            adapter: sqlite::Adapter::open(path)?,
        };
        conversation::recover(&state.adapter)?;
        Ok(state)
    }
}

impl StatePort for SqliteState {
    fn register_project(
        &self,
        project: ProjectRegistration,
    ) -> Result<ProjectRegistration, OperatorError> {
        project::register(&self.adapter, project)
    }
    fn get_project(&self, project_id: &ProjectId) -> Result<ProjectRegistration, OperatorError> {
        project::get(&self.adapter, project_id)
    }
    fn list_projects(&self) -> Result<Vec<ProjectRegistration>, OperatorError> {
        project::list(&self.adapter)
    }
    fn persist_operation_admission(
        &self,
        request: &OperationStart,
        session_id: SessionId,
        fingerprint: &str,
    ) -> Result<OperationAdmission, OperatorError> {
        operation::persist_admission(&self.adapter, request, session_id, fingerprint)
    }
    fn get_operation(&self, operation_id: OperationId) -> Result<Operation, OperatorError> {
        operation::get(&self.adapter, operation_id)
    }
    fn transition(
        &self,
        operation_id: OperationId,
        next: OperationState,
        terminal: Option<TerminalOutcome>,
        observed_session: Option<SessionId>,
        observed_model: Option<String>,
        observed_version: Option<String>,
    ) -> Result<Operation, OperatorError> {
        operation::transition(
            &self.adapter,
            operation_id,
            next,
            terminal,
            observed_session,
            observed_model,
            observed_version,
        )
    }
    fn record_operation_diagnostic(
        &self,
        operation_id: OperationId,
        payload: OperationDiagnosticPayload,
    ) -> Result<OperationDiagnostic, OperatorError> {
        diagnostic::record(&self.adapter, operation_id, payload)
    }
    fn get_operation_diagnostics(
        &self,
        operation_id: OperationId,
        after_diagnostic_sequence: u64,
    ) -> Result<OperationDiagnostics, OperatorError> {
        diagnostic::snapshot(&self.adapter, operation_id, after_diagnostic_sequence)
    }
    fn recover_current_daemon_incomplete(&self) -> Result<(), OperatorError> {
        conversation::recover(&self.adapter)
    }
    fn persist_conversation_start(
        &self,
        request: &ConversationStart,
        session_id: SessionId,
        fingerprint: &str,
    ) -> Result<ConversationStartAdmission, OperatorError> {
        conversation::persist_start(&self.adapter, request, session_id, fingerprint)
    }
    fn get_conversation(
        &self,
        conversation_id: ConversationId,
    ) -> Result<Conversation, OperatorError> {
        conversation::get(&self.adapter, conversation_id)
    }
    fn get_conversation_snapshot(
        &self,
        conversation_id: ConversationId,
        after_sequence: u64,
    ) -> Result<ConversationSnapshot, OperatorError> {
        conversation::snapshot(&self.adapter, conversation_id, after_sequence)
    }
    fn persist_conversation_turn(
        &self,
        request: &ConversationSend,
        fingerprint: &str,
    ) -> Result<ConversationTurnAdmission, OperatorError> {
        conversation::persist_turn(&self.adapter, request, fingerprint)
    }
    fn record_conversation_turn_observation(
        &self,
        conversation_id: ConversationId,
        turn_id: TurnId,
        state: Option<TurnState>,
        result: Option<String>,
        payload: ConversationEventPayload,
    ) -> Result<ConversationTurnObservation, OperatorError> {
        conversation::record_turn_observation(
            &self.adapter,
            conversation_id,
            turn_id,
            state,
            result,
            payload,
        )
    }
    fn record_conversation_initialization(
        &self,
        conversation_id: ConversationId,
        session_id: SessionId,
        model: String,
        claude_version: Option<String>,
    ) -> Result<ConversationEvent, OperatorError> {
        conversation::record_initialization(
            &self.adapter,
            conversation_id,
            session_id,
            model,
            claude_version,
        )
    }
    fn close_conversation(
        &self,
        conversation_id: ConversationId,
        mode: ConversationStopMode,
    ) -> Result<ConversationCloseAdmission, OperatorError> {
        conversation::close(&self.adapter, conversation_id, mode)
    }
    fn terminalize_conversation(
        &self,
        conversation_id: ConversationId,
        conversation_state: ConversationState,
        operation_state: OperationState,
        terminal: TerminalOutcome,
        claim_disposition: SessionClaimDisposition,
    ) -> Result<Conversation, OperatorError> {
        conversation::terminalize(
            &self.adapter,
            conversation_id,
            conversation_state,
            operation_state,
            terminal,
            claim_disposition,
        )
    }
    fn list_session_evidence(
        &self,
        project_id: &ProjectId,
    ) -> Result<Vec<SessionEvidence>, OperatorError> {
        operation::list_session_evidence(&self.adapter, project_id)
    }
    fn inspect_session_evidence(
        &self,
        project_id: &ProjectId,
        target_session_id: SessionId,
    ) -> Result<Vec<SessionEvidence>, OperatorError> {
        operation::inspect_session_evidence(&self.adapter, project_id, target_session_id)
    }
    fn persist_initiator_binding(
        &self,
        binding: &InitiatorBinding,
    ) -> Result<BindingPersistence, OperatorError> {
        initiator_binding::persist(&self.adapter, binding)
    }
    fn get_initiator_binding(
        &self,
        project_id: &ProjectId,
        identity: &InitiatorIdentity,
    ) -> Result<Option<InitiatorBinding>, OperatorError> {
        initiator_binding::get(&self.adapter, project_id, identity)
    }
    fn list_initiator_bindings_for_initiator(
        &self,
        project_id: &ProjectId,
        initiator_session_id: &InitiatorSessionIdentity,
        initiator_agent_id: &InitiatorAgentIdentity,
    ) -> Result<Vec<InitiatorBinding>, OperatorError> {
        initiator_binding::list_for_initiator(
            &self.adapter,
            project_id,
            initiator_session_id,
            initiator_agent_id,
        )
    }
}
