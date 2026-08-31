# AI Agent Operator

AI Agent Operator lets a local MCP client start one persistent Claude Opus
conversation through a separately running durable daemon, receive persisted
text deltas and turn results, submit additional turns to the same exact Claude
session, and reconnect its MCP projection without losing the durable timeline.
A second MCP client can also observe durable retry or authentication
diagnostics from the exact one-shot Claude child without launching a preflight
or inferring a cause. A client can inspect successful operator-owned session
evidence, bind an explicit initiator identity to one evidenced session, and
obtain a side-effect-free `new | resume_exact | refuse` decision.

The daemon, rather than the MCP client, owns the Claude child and the SQLite
state. A complete request is idempotent, and only one active operation may
write to a given Claude session.

## Prerequisites

- Linux.
- Rust 1.98.0, installed through `rustup`.
- A locally installed Claude Code executable.
- A daemon environment already configured with the Claude authentication and
  network access required by Claude Code.

The release supports one trusted operating-system account and local Unix-domain
socket communication. It is not a remote service and does not provide TLS,
authentication, authorization, or cross-account isolation.

## Build

```sh
cargo +1.98.0 build --release --bin aiopd --bin aiop-mcp
```

## Run the daemon

Start `aiopd` in the environment that owns Claude's authentication and network
configuration. Create one user-writable directory for the SQLite state and
Unix socket.

```sh
mkdir -p "$HOME/.local/state/ai-agent-operator"
./target/release/aiopd \
  --state "$HOME/.local/state/ai-agent-operator/operator.sqlite" \
  --socket "$HOME/.local/state/ai-agent-operator/operator.sock"
```

The socket pathname must not already exist. The daemon does not replace it
automatically.

## Connect an MCP client

Run the stateless stdio MCP server with the daemon socket path.

```sh
./target/release/aiop-mcp \
  --socket "$HOME/.local/state/ai-agent-operator/operator.sock"
```

The process speaks JSON-RPC over standard input and output. Initialize the MCP
session, send `notifications/initialized`, and use `tools/list` to obtain the
current schemas. The available tools are:

- `project_register`, `project_get`, `project_list`
- `operation_start`, `operation_get`, `operation_wait`, `operation_diagnostics`, `operation_cancel`
- `conversation_start`, `conversation_send`, `conversation_wait`, `conversation_stop`
- `session_inventory`, `session_inspect`
- `initiator_binding_register`, `session_decide`

Register a project before starting an operation. `claude_executable` is the
executable path passed directly to Claude; `working_directory` is its working
directory; `expected_opus_model` is the canonical model identity expected from
Claude's init event.

```json
{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"example","version":"1"}}}
{"jsonrpc":"2.0","method":"notifications/initialized"}
{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"project_register","arguments":{"project_id":"repository-review","working_directory":"/work/repository","claude_executable":"/usr/local/bin/claude","expected_opus_model":"claude-opus-5"}}}
```

Start a new review with a client-generated UUID request ID. The only accepted
review profile is `opus_read_only`; it fixes the Claude invocation to the
supported Opus, maximum-effort, read-only profile.

```json
{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"operation_start","arguments":{"request_id":"00000000-0000-4000-8000-000000000001","project_id":"repository-review","intent":{"kind":"new"},"prompt":"Review this frozen change.","review_profile":"opus_read_only"}}}
```

The tool returns an operation record in `result.content[0].text`. Wait for its
current or terminal state by passing its `operation_id`.

```json
{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"operation_wait","arguments":{"operation_id":"00000000-0000-4000-8000-000000000002","wait_millis":60000}}}
```

While a one-shot operation is running, another MCP client may read its durable
provider-neutral diagnostic timeline. Pass the highest returned
`diagnostic_sequence` after reconnecting. The read returns its atomic operation
snapshot plus events after that cursor, and stops when it sees a new event, a
terminal operation, or its caller-provided deadline.

```json
{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"operation_diagnostics","arguments":{"operation_id":"<operation_id returned by operation_start>","after_diagnostic_sequence":0,"wait_millis":60000}}}
```

The only diagnostic payloads are `provider_retrying` with validated
`attempt`, `max_retries`, and `retry_delay_ms`, `authentication_failed`, and
`diagnostic_unclassified`. They are direct-child observations, not inferred
network, sandbox, or account causes. The tool is unavailable for operations
created before diagnostic coverage and returns a typed conflict for live
conversations.

List successful session evidence created by this operator, then register one
complete initiator identity against an exact evidenced target session. The
identity key consists of initiator session, initiator agent, role, task, and
subject; an existing key cannot be silently replaced with another target.

```json
{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"session_inventory","arguments":{"project_id":"repository-review"}}}
{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"initiator_binding_register","arguments":{"project_id":"repository-review","initiator_session_id":"initiator-session","initiator_agent_id":"main","role_id":"review-coordinator","task_id":"release-review","subject_id":"candidate-identity","target_session_id":"00000000-0000-4000-8000-000000000004"}}}
```

Request a pure continuity decision. `continue_bound` returns `resume_exact`
only for the exact durable binding; otherwise it returns a typed refusal.
`independent` returns `new`. Decisions do not create operations, launch Claude,
or mutate bindings.

```json
{"jsonrpc":"2.0","id":8,"method":"tools/call","params":{"name":"session_decide","arguments":{"project_id":"repository-review","initiator_session_id":"initiator-session","initiator_agent_id":"main","role_id":"review-coordinator","task_id":"release-review","subject_id":"candidate-identity","continuity":"continue_bound"}}}
```

To continue the exact session, pass the decision's `target_session_id` as the
`session_id` of a `resume_exact` intent, together with a new request ID. The
daemon passes that UUID directly to Claude with its exact-resume invocation; it
does not select a nearby session.

```json
{"jsonrpc":"2.0","id":9,"method":"tools/call","params":{"name":"operation_start","arguments":{"request_id":"00000000-0000-4000-8000-000000000003","project_id":"repository-review","intent":{"kind":"resume_exact","session_id":"00000000-0000-4000-8000-000000000004"},"prompt":"Continue the review using the prior result.","review_profile":"opus_read_only"}}}
```

Start a live conversation with caller-generated request and turn UUIDs. Each
new turn is persisted before it is written to the already-running Claude child.
The `conversation_start` response contains its generated `conversation_id`; use
that exact value in later calls. Use `conversation_wait` with the last received
event sequence after an MCP reconnect; `conversation_stop` accepts `graceful`
or `cancel`.

After the first wait, reconnect if needed and use its highest event sequence as
the next cursor (the example uses `6` and then `12` as returned values).

```json
{"jsonrpc":"2.0","id":10,"method":"tools/call","params":{"name":"conversation_start","arguments":{"request_id":"00000000-0000-4000-8000-000000000010","project_id":"repository-review","intent":{"kind":"new"},"turn_id":"00000000-0000-4000-8000-000000000011","prompt":"Review this change live.","review_profile":"opus_read_only"}}}
{"jsonrpc":"2.0","id":11,"method":"tools/call","params":{"name":"conversation_wait","arguments":{"conversation_id":"<conversation_id returned by conversation_start>","after_sequence":0,"wait_millis":60000}}}
{"jsonrpc":"2.0","id":12,"method":"tools/call","params":{"name":"conversation_send","arguments":{"conversation_id":"<conversation_id returned by conversation_start>","turn_id":"00000000-0000-4000-8000-000000000012","prompt":"Check the error path too."}}}
{"jsonrpc":"2.0","id":13,"method":"tools/call","params":{"name":"conversation_wait","arguments":{"conversation_id":"<conversation_id returned by conversation_start>","after_sequence":6,"wait_millis":60000}}}
{"jsonrpc":"2.0","id":14,"method":"tools/call","params":{"name":"conversation_stop","arguments":{"conversation_id":"<conversation_id returned by conversation_start>","mode":"graceful"}}}
{"jsonrpc":"2.0","id":15,"method":"tools/call","params":{"name":"conversation_wait","arguments":{"conversation_id":"<conversation_id returned by conversation_start>","after_sequence":12,"wait_millis":60000}}}
```

## Current boundaries

- Claude Code is the only target provider.
- The supported operation intents are `new` and `resume_exact`. The operator
  does not discover external Claude sessions or select a target without an
  exact durable binding; it does not fork, rename, export, or adopt sessions.
- A daemon restart classifies incomplete work as indeterminate and refuses that
  session instead of reconnecting to a surviving child.
- One daemon owns a SQLite state file for its lifetime. A durable-state failure
  refuses further daemon requests until that daemon process is restarted.
- The operator has no automatic retry, fallback model, fallback executable,
  default timeout, or output truncation policy.
- A live conversation keeps one direct Claude child and its stdin until an
  explicit graceful close or cancellation. A restart does not adopt that child.
- It does not measure or compact Claude context and does not provide an
  interactive terminal attachment.

## License

Apache-2.0. See [LICENSE](LICENSE).
