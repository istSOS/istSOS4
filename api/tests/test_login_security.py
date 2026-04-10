import os
import sys
import time
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

# Ensure api/ is on sys.path so imports from app.* resolve.
API_DIR = str(Path(__file__).resolve().parents[1])
if API_DIR not in sys.path:
    sys.path.insert(0, API_DIR)

# Keep imports deterministic in test environments.
os.environ.setdefault("SECRET_KEY", "test_secret_key")
os.environ.setdefault("ALGORITHM", "HS256")
os.environ.setdefault("ACCESS_TOKEN_EXPIRE_MINUTES", "5")
os.environ.setdefault("AUTHORIZATION", "1")

from app.login_security import LoginRateLimiter  # noqa: E402
from app.v1.endpoints.create import login as login_endpoint  # noqa: E402


class TestLoginRateLimiter:
    def setup_method(self):
        self.limiter = LoginRateLimiter(
            max_attempts=3,
            window_seconds=60,
            block_seconds=30,
            ip_max_attempts=4,
        )
        self.username = "admin"
        self.client_ip = "127.0.0.1"

    def test_allows_request_initially(self):
        allowed, retry_after = self.limiter.check(self.username, self.client_ip, now=100)
        assert allowed is True
        assert retry_after == 0

    def test_blocks_after_user_threshold(self):
        self.limiter.register_failure(self.username, self.client_ip, now=100)
        self.limiter.register_failure(self.username, self.client_ip, now=101)
        blocked_now, retry_after = self.limiter.register_failure(
            self.username,
            self.client_ip,
            now=102,
        )

        assert blocked_now is True
        assert retry_after == 30

        allowed, retry_after = self.limiter.check(self.username, self.client_ip, now=103)
        assert allowed is False
        assert retry_after > 0

    def test_blocks_after_ip_threshold(self):
        self.limiter.register_failure("user1", self.client_ip, now=100)
        self.limiter.register_failure("user2", self.client_ip, now=101)
        self.limiter.register_failure("user3", self.client_ip, now=102)
        blocked_now, retry_after = self.limiter.register_failure(
            "user4",
            self.client_ip,
            now=103,
        )

        assert blocked_now is True
        assert retry_after == 30

        # New username from same IP should still be blocked by IP-only bucket.
        allowed, retry_after = self.limiter.check("user5", self.client_ip, now=104)
        assert allowed is False
        assert retry_after > 0

    def test_block_expires_after_block_window(self):
        self.limiter.register_failure(self.username, self.client_ip, now=100)
        self.limiter.register_failure(self.username, self.client_ip, now=101)
        self.limiter.register_failure(self.username, self.client_ip, now=102)

        allowed_during_block, _ = self.limiter.check(self.username, self.client_ip, now=120)
        assert allowed_during_block is False

        allowed_after_expire, retry_after = self.limiter.check(
            self.username,
            self.client_ip,
            now=133,
        )
        assert allowed_after_expire is True
        assert retry_after == 0


class TestLoginApiBehavior:
    def setup_method(self):
        self.login_module = login_endpoint
        self.login_module.login_rate_limiter = LoginRateLimiter(
            max_attempts=3,
            window_seconds=60,
            block_seconds=30,
            ip_max_attempts=4,
        )

        app = FastAPI()
        app.include_router(self.login_module.v1)
        self.client = TestClient(app)

    def _post_login(self, username: str, password: str):
        return self.client.post(
            "/Login",
            data={"username": username, "password": password},
        )

    def test_pre_auth_block_returns_429_with_retry_after(self, monkeypatch):
        called = False

        async def fake_authenticate_user(username, password):
            nonlocal called
            called = True
            return None

        monkeypatch.setattr(
            self.login_module,
            "authenticate_user",
            fake_authenticate_user,
        )

        now = time.time()
        self.login_module.login_rate_limiter.register_failure(
            "alice",
            "testclient",
            now=now,
        )
        self.login_module.login_rate_limiter.register_failure(
            "alice",
            "testclient",
            now=now + 1,
        )
        self.login_module.login_rate_limiter.register_failure(
            "alice",
            "testclient",
            now=now + 2,
        )

        response = self._post_login("alice", "wrong")
        assert response.status_code == 429
        assert "Retry-After" in response.headers
        assert called is False

    def test_failed_credentials_are_401_then_blocked_on_next_request(self, monkeypatch):
        async def fake_authenticate_user(username, password):
            return None

        monkeypatch.setattr(
            self.login_module,
            "authenticate_user",
            fake_authenticate_user,
        )

        assert self._post_login("alice", "wrong").status_code == 401
        assert self._post_login("alice", "wrong").status_code == 401
        assert self._post_login("alice", "wrong").status_code == 401

        blocked_response = self._post_login("alice", "wrong")
        assert blocked_response.status_code == 429
        assert "Retry-After" in blocked_response.headers

    def test_success_clears_user_bucket(self, monkeypatch):
        async def fake_authenticate_user(username, password):
            if password == "good":
                return {"username": username, "role": "admin"}
            return None

        monkeypatch.setattr(
            self.login_module,
            "authenticate_user",
            fake_authenticate_user,
        )
        monkeypatch.setattr(
            self.login_module,
            "create_access_token",
            lambda data: ("token", 300),
        )

        assert self._post_login("alice", "wrong").status_code == 401
        assert self._post_login("alice", "wrong").status_code == 401
        assert self._post_login("alice", "good").status_code == 200

        # If success did not clear, this sequence would hit 429 too early.
        assert self._post_login("alice", "wrong").status_code == 401
        assert self._post_login("alice", "wrong").status_code == 401
        assert self._post_login("alice", "wrong").status_code == 401
        assert self._post_login("alice", "wrong").status_code == 429

    def test_ip_only_bucket_blocks_password_spraying(self, monkeypatch):
        self.login_module.login_rate_limiter = LoginRateLimiter(
            max_attempts=10,
            window_seconds=60,
            block_seconds=30,
            ip_max_attempts=3,
        )

        async def fake_authenticate_user(username, password):
            return None

        monkeypatch.setattr(
            self.login_module,
            "authenticate_user",
            fake_authenticate_user,
        )

        assert self._post_login("user1", "wrong").status_code == 401
        assert self._post_login("user2", "wrong").status_code == 401
        assert self._post_login("user3", "wrong").status_code == 401

        blocked_response = self._post_login("user4", "wrong")
        assert blocked_response.status_code == 429
        assert "Retry-After" in blocked_response.headers