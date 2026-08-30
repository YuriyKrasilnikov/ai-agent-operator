# Contributing

Use Rust 1.98.0 and Edition 2024. Keep changes within the current public
contract; do not add fallback behavior, retries, product limits, or unverified
hardening mechanisms.

Before proposing a change, run:

```sh
cargo +1.98.0 fmt --check
cargo +1.98.0 clippy --locked --all-targets --all-features -- -D warnings
cargo +1.98.0 test --locked --all-targets --all-features
cargo +1.98.0 build --locked --release --bin aiopd --bin aiop-mcp
```

Tests must observe public semantics or typed compilation boundaries, not private
source shape. Keep failures causal and explicit; do not mask them with defaults
or suppressions.
