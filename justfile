# Justfile for mesh-proxy Rust Workspace
# AI AGENT: Do NOT modify this file or use `git commit -n`/`--no-verify` to bypass pre-commit.

set shell := ["bash", "-c"]
set tempdir := "."
set dotenv-load := true

_repo_root := `git rev-parse --show-superproject-working-tree 2>/dev/null | grep . || git rev-parse --show-toplevel`
export MISE_TRUSTED_CONFIG_PATHS := _repo_root

# 50-minute timeout for heavy commands (prevents caller KV cache expiry)
_timeout := "3000"

_check_writable path attempted:
    #!/usr/bin/env bash
    set -euo pipefail
    path="{{path}}"
    attempted="{{attempted}}"
    resolved_path="$(readlink -f "$path" 2>/dev/null || printf '%s' "$path")"
    mkdir -p "$path" 2>/dev/null || true
    probe="$path/.write-probe.$$"
    if touch "$probe" >/dev/null 2>&1; then
        rm -f "$probe"
        exit 0
    fi
    echo >&2 "ERROR: attempted to write ${attempted} at ${path} (resolved: ${resolved_path}), but the path is not writable."
    exit 2

_require_cargo:
    #!/usr/bin/env bash
    if [ ! -f "{{_repo_root}}/Cargo.toml" ]; then
        echo "SKIP: no Cargo.toml yet — Rust quality gates deferred."
        exit 0
    fi

check-cargo-target-writable:
    #!/usr/bin/env bash
    set -euo pipefail
    target_dir="${CARGO_TARGET_DIR:-{{_repo_root}}/target}"
    just _check_writable "$target_dir" "cargo build artifacts"

check-nextest-state-writable:
    #!/usr/bin/env bash
    set -euo pipefail
    state_home="${XDG_STATE_HOME:-$HOME/.local/state}"
    just _check_writable "$state_home" "cargo nextest state files"

default: pre-commit

# ==============================================================================
# Core Workflow
# ==============================================================================

find-monolith-files:
    #!/usr/bin/env bash
    set -euo pipefail
    export MONOLITH_THRESHOLD_TOKENS="${MONOLITH_TOKEN_THRESHOLD:-20000}"
    export MONOLITH_THRESHOLD_LINES="${MONOLITH_LINE_THRESHOLD:-2500}"
    export MONOLITH_MODEL="${TOKUIN_MODEL:-gpt-4}"
    CHECKER=$(mktemp)
    trap 'rm -f "$CHECKER"' EXIT
    cat > "$CHECKER" << 'SCRIPT'
    #!/usr/bin/env bash
    file="$1"
    threshold_tokens="$MONOLITH_THRESHOLD_TOKENS"
    threshold_lines="$MONOLITH_THRESHOLD_LINES"
    model="$MONOLITH_MODEL"
    case "$file" in
        *.lock|*lock.json|*lock.yaml) exit 0 ;;
        */AGENTS.md|*/FACTORY.md) exit 0 ;;
        */PATTERN.md|*/SKILL.md) exit 0 ;;
        */workflow.toml) exit 0 ;;
        */fixtures/*) exit 0 ;;
    esac
    [ -f "$file" ] || exit 0
    grep -Iq '' "$file" 2>/dev/null || exit 0
    monolith_error() {
        echo ""
        echo "=========================================="
        echo "ERROR: Monolith file detected! ($1, limit: $2)"
        echo "  File: $file"
        echo "=========================================="
        exit 1
    }
    lines=$(wc -l < "$file" 2>/dev/null || echo 0)
    if [ "$lines" -gt "$threshold_lines" ]; then
        monolith_error "$lines lines" "$threshold_lines lines"
    fi
    tokens=$(tokuin estimate --model "$model" --format json "$file" 2>/dev/null \
        | jq -r '.tokens // 0' 2>/dev/null || echo 0)
    [ -z "$tokens" ] && tokens=0
    if [ "$tokens" -gt "$threshold_tokens" ]; then
        monolith_error "$tokens tokens" "$threshold_tokens tokens"
    fi
    SCRIPT
    chmod +x "$CHECKER"
    git ls-files --recurse-submodules \
        | parallel --halt now,fail=1 "$CHECKER" {}

check-generated-artifacts:
    #!/usr/bin/env bash
    set -euo pipefail
    blocked_paths="$(
        git diff --cached --name-only --diff-filter=ACMR \
            | rg '^([.]test-target/|[.]tmp/|target/|diff[.]txt$|test-write$)' || true
    )"
    if [ -n "${blocked_paths}" ]; then
        echo ""
        echo "=========================================="
        echo "ERROR: Generated or scratch artifacts are staged."
        echo "=========================================="
        printf '%s\n' "${blocked_paths}"
        exit 1
    fi

check-chinese:
    @echo "Checking for Chinese characters in source code..."
    @! rg "\p{Script=Han}" . --vimgrep --glob '!target/**' --glob '!.git/**' --glob '!*.md' --glob '!**/*.md' --glob '!**/i18n/**' --glob '!**/help_i18n/**'

pre-commit:
    #!/usr/bin/env bash
    set -euo pipefail
    timeout {{_timeout}} bash -c '
        set -euo pipefail
        just find-monolith-files
        just check-generated-artifacts
        just check-chinese
        just fmt
        just clippy
        just test
    '

fmt:
    #!/usr/bin/env bash
    set -euo pipefail
    [ ! -f "{{_repo_root}}/Cargo.toml" ] && echo "SKIP fmt: no Cargo.toml" && exit 0
    timeout {{_timeout}} cargo fmt --all
    git diff --name-only -- '*.rs' | xargs -r git add

clippy:
    #!/usr/bin/env bash
    set -euo pipefail
    [ ! -f "{{_repo_root}}/Cargo.toml" ] && echo "SKIP clippy: no Cargo.toml" && exit 0
    just check-cargo-target-writable
    timeout {{_timeout}} cargo clippy --workspace --all-features -- -D warnings

clippy-p package:
    just check-cargo-target-writable
    timeout {{_timeout}} cargo clippy -p {{package}} --all-features -- -D warnings

# ==============================================================================
# Testing
# ==============================================================================

test:
    #!/usr/bin/env bash
    set -euo pipefail
    [ ! -f "{{_repo_root}}/Cargo.toml" ] && echo "SKIP test: no Cargo.toml" && exit 0
    just check-cargo-target-writable
    just check-nextest-state-writable
    timeout {{_timeout}} cargo nextest run --workspace --all-features --no-tests=pass

test-p package:
    just check-cargo-target-writable
    just check-nextest-state-writable
    timeout {{_timeout}} cargo nextest run -p {{package}} --all-features --no-tests=pass

test-f pattern:
    just check-cargo-target-writable
    just check-nextest-state-writable
    timeout {{_timeout}} cargo nextest run --workspace --all-features -E 'test({{pattern}})'

# ==============================================================================
# Git Helpers
# ==============================================================================

review:
    @echo "=== Staged changes ==="
    git diff --cached --stat
    @echo ""
    @echo "=== Unstaged changes ==="
    git diff --stat
    @echo ""
    @echo "Review the above before committing."

install-hooks:
    @git config --unset core.hooksPath 2>/dev/null || true
    lefthook install
    @echo "Lefthook hooks installed."
