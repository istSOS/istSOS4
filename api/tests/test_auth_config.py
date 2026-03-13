import unittest

from app.auth_config import validate_auth_config


class AuthConfigValidationTestCase(unittest.TestCase):
    def test_auth_enabled_requires_secret_key(self):
        with self.assertRaisesRegex(ValueError, "requires SECRET_KEY"):
            validate_auth_config(authorization=1, secret_key=None, debug=0)

    def test_auth_enabled_rejects_short_secret_key(self):
        with self.assertRaisesRegex(ValueError, "too short"):
            validate_auth_config(authorization=1, secret_key="short", debug=0)

    def test_auth_enabled_rejects_placeholder_secret_key(self):
        with self.assertRaisesRegex(ValueError, "placeholder"):
            validate_auth_config(authorization=1, secret_key="secret", debug=0)

    def test_auth_enabled_accepts_strong_secret_key(self):
        warnings = validate_auth_config(
            authorization=1,
            secret_key="a" * 64,
            debug=0,
        )
        self.assertEqual(warnings, [])

    def test_auth_disabled_returns_warning_messages(self):
        warnings = validate_auth_config(authorization=0, secret_key=None, debug=0)

        self.assertGreaterEqual(len(warnings), 1)
        self.assertIn("AUTHORIZATION=0", warnings[0])


if __name__ == "__main__":
    unittest.main()