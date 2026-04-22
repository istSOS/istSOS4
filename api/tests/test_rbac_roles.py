import unittest

from app.rbac_roles import get_db_role_for_rbac, validate_rbac_role


class RbacRolesTestCase(unittest.TestCase):
    def test_validate_role_accepts_supported_values(self):
        self.assertEqual(validate_rbac_role("viewer"), "viewer")
        self.assertEqual(validate_rbac_role("Editor"), "editor")
        self.assertEqual(validate_rbac_role(" custom "), "custom")

    def test_validate_role_rejects_unknown_value(self):
        with self.assertRaisesRegex(ValueError, "Invalid role"):
            validate_rbac_role("administrator")

    def test_get_db_role_mapping(self):
        self.assertEqual(get_db_role_for_rbac("viewer"), "user")
        self.assertEqual(get_db_role_for_rbac("editor"), "user")
        self.assertEqual(get_db_role_for_rbac("obs_manager"), "sensor")
        self.assertEqual(get_db_role_for_rbac("sensor"), "sensor")
        self.assertEqual(get_db_role_for_rbac("custom"), "user")


if __name__ == "__main__":
    unittest.main()
