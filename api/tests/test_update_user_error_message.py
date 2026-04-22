import unittest
from pathlib import Path


class UpdateUserErrorMessageTestCase(unittest.TestCase):
    def test_update_user_undefined_object_message_is_user_not_found(self):
        endpoint_file = (
            Path(__file__).resolve().parents[1]
            / "app"
            / "v1"
            / "endpoints"
            / "update"
            / "user.py"
        )
        content = endpoint_file.read_text(encoding="utf-8")

        self.assertIn("except UndefinedObjectError as e:", content)
        self.assertIn('content={"message": "User not found"}', content)
        self.assertNotIn('content={"message": "Policy not found"}', content)


if __name__ == "__main__":
    unittest.main()
