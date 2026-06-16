import unittest

from app.rbac_roles import (
    DENIED_CREATOR_ROLES,
    check_create_permission,
    get_db_role_for_rbac,
    validate_rbac_role,
)


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


class CheckCreatePermissionTestCase(unittest.TestCase):
    def test_denied_creator_roles_includes_viewer(self):
        self.assertIn("viewer", DENIED_CREATOR_ROLES)

    def test_viewer_denied_create(self):
        self.assertFalse(check_create_permission("viewer"))
        self.assertFalse(check_create_permission("VIEWER"))

    def test_editor_allowed_create(self):
        self.assertTrue(check_create_permission("editor"))
        self.assertTrue(check_create_permission("EDITOR"))

    def test_obs_manager_allowed_create(self):
        self.assertTrue(check_create_permission("obs_manager"))

    def test_sensor_allowed_create(self):
        self.assertTrue(check_create_permission("sensor"))

    def test_custom_allowed_create(self):
        self.assertTrue(check_create_permission("custom"))

    def test_none_role_allowed(self):
        self.assertTrue(check_create_permission(None))


if __name__ == "__main__":
    unittest.main()
