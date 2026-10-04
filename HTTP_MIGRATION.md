# Shared HTTP deployment (2026-10-04)

Maintained local fork branch: codex/shared-http-migration-20261004. Upstream base cfc38e8, package 0.3.1, FastMCP 3.4.4 and installed kroger-api 0.3.1. The private launcher and credentials are deliberately not committed.

Run the CLI with `python -m kroger_mcp.cli --transport streamable-http --host 127.0.0.1 --port 18303` using the existing private credentials/token directory. User service `pa-kroger-http.service` uses a separate private `codex-http-launch.sh` derived from the original launcher; the original stdio launcher remains available for rollback. The committed CLI enables strict loopback Host/Origin guards. One shared service serializes entire calls, including awaits; a private advisory file lock coordinates updated processes. Cached clients are invalidated under that lock before loading possibly rotated tokens. Dependency network calls have connect/read timeouts (5/20 seconds) with no additional retries.

Private JSON writes use fsync and atomic replacement. Cart-to-history transfers use a replayable journal: restart completes the same snapshots before another tool runs, so an interrupted local transfer cannot append the same order twice. Failed writes propagate. OAuth attempts are scoped by MCP session; restart requires a new OAuth attempt. The service never automatically replays a remote cart mutation after restart or disconnect. A local tracker remains no proof of actual website cart contents.

Retained older stdio processes do not honor the new file lock; do not run mutating calls through those old sessions concurrently with the shared service. Refresh at an owner-approved safe checkpoint. No active sessions were killed for this migration.

Qualification: 31 tool schemas match stdio; installed Codex CLI read-only auth metadata/location calls pass. Fixtures test atomic private tokens, 30 concurrent cart/cache calls, two-process fake rotation, OAuth isolation, cancellation, finite network timeouts and crash journal replay. No test products or orders were sent to the real account. Strict hostile Host/Origin rejection and controlled systemd restart/fresh MCP reconnect pass.

Rollback: narrowly restore the original Kroger registration to codex-launch.sh, preserving other config registrations. Once fresh HTTP clients disconnect, disable `pa-kroger-http.service`. Keep state files and maintained patch; do not overwrite OAuth tokens with a historical backup. Tests: `.venv/bin/python -m pytest tests/test_shared_state.py -q`.
