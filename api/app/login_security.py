import logging
import time
from collections import defaultdict, deque


logger = logging.getLogger(__name__)


class LoginRateLimiter:
    """In-memory rate limiter keyed by client IP and username."""

    def __init__(self, max_attempts: int, window_seconds: int, block_seconds: int):
        self.max_attempts = max_attempts
        self.window_seconds = window_seconds
        self.block_seconds = block_seconds
        self._attempts: dict[str, deque[float]] = defaultdict(deque)
        self._blocked_until: dict[str, float] = {}

    @staticmethod
    def _build_key(username: str, client_ip: str) -> str:
        return f"{client_ip}:{username.strip().lower()}"

    def _prune(self, key: str, now: float) -> None:
        window_start = now - self.window_seconds
        attempts = self._attempts[key]
        while attempts and attempts[0] < window_start:
            attempts.popleft()

    def check(self, username: str, client_ip: str, now: float | None = None):
        now = now if now is not None else time.time()
        key = self._build_key(username, client_ip)
        blocked_until = self._blocked_until.get(key, 0)

        if blocked_until > now:
            return False, int(blocked_until - now)

        if blocked_until:
            del self._blocked_until[key]

        self._prune(key, now)
        return True, 0

    def register_failure(
        self,
        username: str,
        client_ip: str,
        now: float | None = None,
    ):
        now = now if now is not None else time.time()
        key = self._build_key(username, client_ip)
        self._prune(key, now)

        attempts = self._attempts[key]
        attempts.append(now)

        if len(attempts) >= self.max_attempts:
            blocked_until = now + self.block_seconds
            self._blocked_until[key] = blocked_until
            return True, self.block_seconds

        return False, 0

    def register_success(self, username: str, client_ip: str):
        key = self._build_key(username, client_ip)
        self._attempts.pop(key, None)
        self._blocked_until.pop(key, None)


def emit_login_audit(
    username: str,
    client_ip: str,
    status: str,
    detail: str = "",
) -> None:
    logger.info(
        "login_audit status=%s username=%s client_ip=%s detail=%s",
        status,
        username,
        client_ip,
        detail,
    )