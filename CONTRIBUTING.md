# Contributing to polyglotmesh

Thanks for your interest in polyglotmesh! This document explains how to
file issues, submit pull requests, and run the project locally.

## Filing issues

Use the GitHub issue templates (`.github/ISSUE_TEMPLATE/`). For bug
reports, please include:

- `polyglotmesh --version` output
- The relevant slice of your `config.toml` (with secrets redacted)
- The output of `polyglotmesh serve` up to and including the failure
- A minimal `curl` reproduction if possible

## Pull requests

1. Fork the repo and create a feature branch: `git checkout -b feat/...`
2. Make your changes. Keep them focused — one PR per logical change.
3. Run the local checks:
   ```bash
   cargo build --release
   cargo test
   bash scripts/smoke-test.sh
   ```
4. Open a PR against `main`. Fill in the PR template.
5. Wait for CI to pass. A maintainer will review.

## Coding conventions

- **Rust 2021 edition**, stable toolchain (no nightly features).
- Prefer `parking_lot::Mutex/RwLock` over `std::sync` (we already do).
- Prefer atomic counters (`AtomicU64`) over locks on the hot path.
- Every public type/method that touches the proxy hot path must have a
  one-line `///` doc comment explaining what it does and why it
  exists.
- No `unwrap()` in non-test code paths that handle external input.
- New dependencies must be justified in the PR description (license,
  maintenance status, why no first-party alternative works).

## Commit messages

Use the imperative mood: "Add rate ring" not "Added rate ring". Prefix
with the area:

- `proxy: ...` for proxy/streaming changes
- `admin: ...` for admin API changes
- `storage: ...` for SQLite / persistence
- `metrics: ...` for observability
- `config: ...` for config parsing / hot-reload
- `docs: ...` for documentation only
- `ci: ...` for CI / release plumbing

## Security

Please **do not** file public issues for security vulnerabilities. Email
the maintainers (see `SECURITY.md` for the address) and we will coordinate
a fix and a disclosure timeline.
