"""Unit tests for safe RLS policy expression parsing and rendering."""

import unittest

from app.utils.policy_expression import render_policy_expression


class PolicyExpressionTest(unittest.TestCase):
    def test_boolean_literals(self):
        self.assertEqual(render_policy_expression("true"), "TRUE")
        self.assertEqual(render_policy_expression(" FALSE "), "FALSE")

    def test_simple_comparisons(self):
        self.assertEqual(
            render_policy_expression("network = 'IDROLOGIA'"),
            "\"network\" = 'IDROLOGIA'",
        )
        self.assertEqual(
            render_policy_expression("temperature >= -5.5"),
            '"temperature" >= -5.5',
        )
        self.assertEqual(
            render_policy_expression("status != 'draft'"),
            "\"status\" != 'draft'",
        )

    def test_quoted_identifiers_and_escaped_strings(self):
        self.assertEqual(
            render_policy_expression("\"name\" = 'O''Reilly'"),
            "\"name\" = 'O''Reilly'",
        )
        self.assertEqual(
            render_policy_expression('"my""col" <> 0'),
            '"my""col" <> 0',
        )

    def test_in_and_is_null_expressions(self):
        self.assertEqual(
            render_policy_expression('"id" IN (1, 2, 3)'),
            '"id" IN (1, 2, 3)',
        )
        self.assertEqual(
            render_policy_expression("code not in ('a', 'b')"),
            "\"code\" NOT IN ('a', 'b')",
        )
        self.assertEqual(
            render_policy_expression('"name" IS NOT NULL'),
            '"name" IS NOT NULL',
        )
        self.assertEqual(
            render_policy_expression("deleted_at is null"),
            '"deleted_at" IS NULL',
        )

    def test_boolean_combinators_and_parentheses(self):
        self.assertEqual(
            render_policy_expression("status = 'active' AND public = true"),
            '"status" = \'active\' AND "public" = TRUE',
        )
        self.assertEqual(
            render_policy_expression(
                "(network = 'A' OR network = 'B') AND NOT archived = true"
            ),
            '("network" = \'A\' OR "network" = \'B\') AND NOT "archived" = TRUE',
        )

    def test_rejects_sql_injection_and_malformed_expressions(self):
        invalid_cases = [
            "",
            "   ",
            None,
            123,
            "true) WITH CHECK (false",
            "network = 'x'; DROP TABLE users",
            "exists (select 1)",
            "true -- comment",
            "true /* block comment */",
            "my_func(1)",
            "(network = 'A'",
            "network = 'A')",
            "network = 'unterminated",
            '"" = 1',
            "network =",
            "network = other_column",
            "id IN ()",
        ]
        for expr in invalid_cases:
            with self.subTest(expr=expr):
                with self.assertRaises(ValueError):
                    render_policy_expression(expr)


if __name__ == "__main__":
    unittest.main()
