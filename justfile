# mesh-proxy development tasks

# Format all code and stage changes
fmt:
    cargo fmt --all
    git add -u

# Run clippy on entire workspace
clippy:
    cargo clippy --workspace -- -D warnings

# Run all tests
test:
    cargo test --workspace

# Pre-commit: fmt → clippy → test
pre-commit: fmt clippy test
