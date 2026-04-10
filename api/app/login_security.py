import logging
import time
from collections import defaultdict, deque


logger = logging.getLogger(__name__)


class LoginRateLimiter:
    """In-memory rate limiter with per-user and per-IP buckets."""

    def __init__(
        self,
        max_attempts: int,
        window_seconds: int,
        block_seconds: int,
        ip_max_attempts: int | None = None,
    ):
        self.max_attempts = max_attempts
        self.window_seconds = window_seconds
        self.block_seconds = block_seconds
        self.ip_max_attempts = (
            ip_max_attempts if ip_max_attempts is not None else max_attempts * 3
        )
        self._attempts: dict[str, deque[float]] = defaultdict(deque)
        self._blocked_until: dict[str, float] = {}

    @staticmethod
    def _user_key(username: str, client_ip: str) -> str:
        return f"u:{client_ip}:{username.strip().lower()}"

    @staticmethod
    def _ip_key(client_ip: str) -> str:
        return f"ip:{client_ip}"

    def _prune(self, key: str, now: float) -> None:
        window_start = now - self.window_seconds
        attempts = self._attempts[key]
        while attempts and attempts[0] < window_start:
            attempts.popleft()

    def _is_blocked(self, key: str, now: float) -> tuple[bool, int]:
        blocked_until = self._blocked_until.get(key, 0)

        if blocked_until > now:
            return True, int(blocked_until - now)

        if blocked_until:
            del self._blocked_until[key]

        return False, 0

    def _record_attempt(self, key: str, now: float, threshold: int) -> tuple[bool, int]:
        self._prune(key, now)
        attempts = self._attempts[key]
        attempts.append(now)

        if len(attempts) >= threshold:
            blocked_until = now + self.block_seconds
            self._blocked_until[key] = blocked_until
            return True, self.block_seconds

        return False, 0

    def check(self, username: str, client_ip: str, now: float | None = None):
        now = now if now is not None else time.time()
        user_key = self._user_key(username, client_ip)
        blocked, retry_after = self._is_blocked(user_key, now)
        if blocked:
            return False, retry_after

        ip_key = self._ip_key(client_ip)
        blocked, retry_after = self._is_blocked(ip_key, now)
        if blocked:
            return False, retry_after

        return True, 0

    def register_failure(
        self,
        username: str,
        client_ip: str,
        now: float | None = None,
    ):
        now = now if now is not None else time.time()
        user_key = self._user_key(username, client_ip)
        ip_key = self._ip_key(client_ip)

        user_blocked, user_retry_after = self._record_attempt(
            user_key,
            now,
            self.max_attempts,
        )
        ip_blocked, ip_retry_after = self._record_attempt(
            ip_key,
            now,
            self.ip_max_attempts,
        )

        if user_blocked or ip_blocked:
            return True, max(user_retry_after, ip_retry_after)

        return False, 0

    def register_success(self, username: str, client_ip: str):
        # Clear only the targeted account bucket after successful auth.
        user_key = self._user_key(username, client_ip)
        self._attempts.pop(user_key, None)
        self._blocked_until.pop(user_key, None)


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