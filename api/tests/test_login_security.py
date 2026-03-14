import unittest

from app.login_security import LoginRateLimiter


class LoginRateLimiterTestCase(unittest.TestCase):
    def setUp(self):
        self.limiter = LoginRateLimiter(
            max_attempts=3,
            window_seconds=60,
            block_seconds=30,
        )
        self.username = "admin"
        self.client_ip = "127.0.0.1"

    def test_allows_request_initially(self):
        allowed, retry_after = self.limiter.check(self.username, self.client_ip, now=100)
        self.assertTrue(allowed)
        self.assertEqual(retry_after, 0)

    def test_blocks_after_threshold(self):
        self.limiter.register_failure(self.username, self.client_ip, now=100)
        self.limiter.register_failure(self.username, self.client_ip, now=101)
        blocked_now, retry_after = self.limiter.register_failure(
            self.username,
            self.client_ip,
            now=102,
        )

        self.assertTrue(blocked_now)
        self.assertEqual(retry_after, 30)

        allowed, retry_after = self.limiter.check(self.username, self.client_ip, now=103)
        self.assertFalse(allowed)
        self.assertGreater(retry_after, 0)

    def test_success_clears_failures(self):
        self.limiter.register_failure(self.username, self.client_ip, now=100)
        self.limiter.register_failure(self.username, self.client_ip, now=101)
        self.limiter.register_success(self.username, self.client_ip)

        allowed, retry_after = self.limiter.check(self.username, self.client_ip, now=102)
        self.assertTrue(allowed)
        self.assertEqual(retry_after, 0)

    def test_block_expires_after_block_window(self):
        self.limiter.register_failure(self.username, self.client_ip, now=100)
        self.limiter.register_failure(self.username, self.client_ip, now=101)
        self.limiter.register_failure(self.username, self.client_ip, now=102)

        allowed_during_block, _ = self.limiter.check(self.username, self.client_ip, now=120)
        self.assertFalse(allowed_during_block)

        allowed_after_expire, retry_after = self.limiter.check(
            self.username,
            self.client_ip,
            now=133,
        )
        self.assertTrue(allowed_after_expire)
        self.assertEqual(retry_after, 0)


if __name__ == "__main__":
    unittest.main()