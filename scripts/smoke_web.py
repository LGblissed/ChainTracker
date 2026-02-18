"""Smoke tests for the Chain Tracker Flask web app.

Run:
    python scripts/smoke_web.py
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Keep smoke tests self-contained if .env is not configured yet.
os.environ.setdefault("CHAIN_TRACKER_PASSWORD", "smoke-test-password")
os.environ.setdefault("CHAIN_TRACKER_COOKIE_SECRET", "smoke-test-cookie-secret")

from app import app  # noqa: E402


def _fail(message: str) -> int:
    """Print a failure message and return non-zero status."""
    print(f"[FAIL] {message}")
    return 1


def run() -> int:
    """Execute a minimal end-to-end web smoke test."""
    print("=== Web Smoke Test ===")
    client = app.test_client()

    # 1) Unauthenticated users should be redirected from overview to login.
    resp = client.get("/", follow_redirects=False)
    if resp.status_code not in {301, 302, 303, 307, 308}:
        return _fail(f"GET / expected redirect, got {resp.status_code}")
    location = resp.headers.get("Location", "")
    if "/login" not in location:
        return _fail(f"GET / redirect target expected /login, got {location!r}")
    print("[PASS] auth gate redirects to /login")

    # 2) Login page should render.
    resp = client.get("/login")
    if resp.status_code != 200:
        return _fail(f"GET /login expected 200, got {resp.status_code}")
    if b"Chain Tracker" not in resp.data:
        return _fail("GET /login missing expected page marker 'Chain Tracker'")
    print("[PASS] login page renders")

    # 3) Password auth should succeed.
    resp = client.post(
        "/api/auth",
        json={"action": "password", "password": os.environ["CHAIN_TRACKER_PASSWORD"]},
    )
    if resp.status_code != 200:
        return _fail(f"POST /api/auth password expected 200, got {resp.status_code}")
    payload = resp.get_json(silent=True) or {}
    if payload.get("ok") is not True:
        return _fail(f"POST /api/auth password expected ok=true, got {json.dumps(payload)}")
    print("[PASS] password auth works")

    # 4) Name step should succeed.
    resp = client.post("/api/auth", json={"action": "name", "name": "Smoke"})
    if resp.status_code != 200:
        return _fail(f"POST /api/auth name expected 200, got {resp.status_code}")
    payload = resp.get_json(silent=True) or {}
    if payload.get("ok") is not True:
        return _fail(f"POST /api/auth name expected ok=true, got {json.dumps(payload)}")
    print("[PASS] name step works")

    # 5) Authenticated overview should render.
    resp = client.get("/")
    if resp.status_code != 200:
        return _fail(f"GET / (authenticated) expected 200, got {resp.status_code}")
    if b"Panel" not in resp.data:
        return _fail("GET / (authenticated) missing expected marker 'Panel'")
    print("[PASS] overview renders after auth")

    # 6) Feed endpoints should be reachable.
    resp = client.get("/feed")
    if resp.status_code != 200:
        return _fail(f"GET /feed expected 200, got {resp.status_code}")
    resp = client.get("/api/feed")
    if resp.status_code != 200:
        return _fail(f"GET /api/feed expected 200, got {resp.status_code}")
    data = resp.get_json(silent=True)
    if not isinstance(data, list):
        return _fail(f"GET /api/feed expected list JSON, got {type(data).__name__}")
    print("[PASS] feed page and API render")

    print("=== 6/6 PASSED ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
