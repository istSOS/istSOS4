"""Validation and rendering for custom row-level-security policy predicates."""

from __future__ import annotations

import re
from dataclasses import dataclass


_IDENTIFIER_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_NUMBER_RE = re.compile(r"-?(?:\d+\.\d+|\d+)")

_KEYWORDS = {
    "AND",
    "FALSE",
    "ILIKE",
    "IN",
    "IS",
    "LIKE",
    "NOT",
    "NULL",
    "OR",
    "TRUE",
}


@dataclass(frozen=True)
class _Token:
    kind: str
    value: object


class _PolicyExpressionParser:
    def __init__(self, tokens: list[_Token]):
        self._tokens = tokens
        self._pos = 0

    def parse(self) -> str:
        expression = self._parse_or()
        if self._peek().kind != "EOF":
            raise ValueError("Unexpected token in policy expression")
        return expression

    def _parse_or(self) -> str:
        expression = self._parse_and()
        while self._match_keyword("OR"):
            rhs = self._parse_and()
            expression = f"({expression} OR {rhs})"
        return expression

    def _parse_and(self) -> str:
        expression = self._parse_not()
        while self._match_keyword("AND"):
            rhs = self._parse_not()
            expression = f"({expression} AND {rhs})"
        return expression

    def _parse_not(self) -> str:
        if self._match_keyword("NOT"):
            return f"(NOT {self._parse_not()})"
        return self._parse_predicate()

    def _parse_predicate(self) -> str:
        left = self._parse_primary()

        if self._match_keyword("IS"):
            not_sql = " NOT" if self._match_keyword("NOT") else ""
            value = self._consume_constant({"NULL", "TRUE", "FALSE"})
            return f"{left} IS{not_sql} {value}"

        if self._match_keyword("NOT"):
            if self._match_keyword("IN"):
                values = self._parse_value_list()
                return f"{left} NOT IN ({', '.join(values)})"
            if self._match_keyword("LIKE"):
                return f"{left} NOT LIKE {self._parse_primary()}"
            if self._match_keyword("ILIKE"):
                return f"{left} NOT ILIKE {self._parse_primary()}"
            raise ValueError("Expected IN, LIKE, or ILIKE after NOT")

        if self._match_keyword("IN"):
            values = self._parse_value_list()
            return f"{left} IN ({', '.join(values)})"

        if self._match_keyword("LIKE"):
            return f"{left} LIKE {self._parse_primary()}"

        if self._match_keyword("ILIKE"):
            return f"{left} ILIKE {self._parse_primary()}"

        if self._peek().kind == "OP":
            operator = self._advance().value
            right = self._parse_primary()
            return f"{left} {operator} {right}"

        return left

    def _parse_primary(self) -> str:
        token = self._peek()

        if self._match("("):
            expression = self._parse_or()
            self._expect(")")
            return f"({expression})"

        if token.kind == "IDENT":
            return self._quote_identifier(str(self._advance().value))

        if token.kind == "KEYWORD":
            if token.value in {"TRUE", "FALSE", "NULL"}:
                return str(self._advance().value)
            raise ValueError("Unexpected keyword in policy expression")

        if token.kind == "STRING":
            value = str(self._advance().value)
            return "'" + value.replace("'", "''") + "'"

        if token.kind == "NUMBER":
            return str(self._advance().value)

        raise ValueError("Unexpected token in policy expression")

    def _parse_value_list(self) -> list[str]:
        self._expect("(")
        values = [self._parse_primary()]
        while self._match(","):
            values.append(self._parse_primary())
        self._expect(")")
        return values

    def _consume_constant(self, allowed: set[str]) -> str:
        token = self._peek()
        if token.kind == "KEYWORD" and token.value in allowed:
            return str(self._advance().value)
        raise ValueError("Expected constant in policy expression")

    def _peek(self) -> _Token:
        return self._tokens[self._pos]

    def _advance(self) -> _Token:
        token = self._peek()
        self._pos += 1
        return token

    def _match(self, value: str) -> bool:
        if self._peek().value == value:
            self._advance()
            return True
        return False

    def _match_keyword(self, value: str) -> bool:
        token = self._peek()
        if token.kind == "KEYWORD" and token.value == value:
            self._advance()
            return True
        return False

    def _expect(self, value: str) -> None:
        if not self._match(value):
            raise ValueError("Malformed policy expression")

    @staticmethod
    def _quote_identifier(value: str) -> str:
        return '"' + value.replace('"', '""') + '"'


def render_policy_expression(value: str) -> str:
    """Return a normalized SQL predicate from a constrained expression grammar."""
    if not isinstance(value, str):
        raise ValueError("Policy expression must be a string")

    expression = value.strip()
    if expression == "":
        raise ValueError("Policy expression must not be empty")

    tokens = _tokenize(expression)
    return _PolicyExpressionParser(tokens).parse()


def _tokenize(expression: str) -> list[_Token]:
    tokens: list[_Token] = []
    pos = 0

    while pos < len(expression):
        char = expression[pos]

        if char.isspace():
            pos += 1
            continue

        if char in "(),":
            tokens.append(_Token("PUNCT", char))
            pos += 1
            continue

        if expression.startswith(("<=", ">=", "<>", "!="), pos):
            tokens.append(_Token("OP", expression[pos : pos + 2]))
            pos += 2
            continue

        if char in "=<>":
            tokens.append(_Token("OP", char))
            pos += 1
            continue

        if char == "'":
            value, pos = _read_string(expression, pos)
            tokens.append(_Token("STRING", value))
            continue

        if char == '"':
            value, pos = _read_quoted_identifier(expression, pos)
            tokens.append(_Token("IDENT", value))
            continue

        identifier = _IDENTIFIER_RE.match(expression, pos)
        if identifier is not None:
            value = identifier.group(0)
            upper_value = value.upper()
            if upper_value in _KEYWORDS:
                tokens.append(_Token("KEYWORD", upper_value))
            else:
                tokens.append(_Token("IDENT", value))
            pos = identifier.end()
            continue

        number = _NUMBER_RE.match(expression, pos)
        if number is not None:
            tokens.append(_Token("NUMBER", number.group(0)))
            pos = number.end()
            continue

        raise ValueError("Unsafe policy expression")

    tokens.append(_Token("EOF", "EOF"))
    return tokens


def _read_string(expression: str, pos: int) -> tuple[str, int]:
    chars: list[str] = []
    pos += 1

    while pos < len(expression):
        char = expression[pos]
        if char == "'":
            if pos + 1 < len(expression) and expression[pos + 1] == "'":
                chars.append("'")
                pos += 2
                continue
            return "".join(chars), pos + 1
        chars.append(char)
        pos += 1

    raise ValueError("Unterminated string in policy expression")


def _read_quoted_identifier(expression: str, pos: int) -> tuple[str, int]:
    chars: list[str] = []
    pos += 1

    while pos < len(expression):
        char = expression[pos]
        if char == '"':
            if pos + 1 < len(expression) and expression[pos + 1] == '"':
                chars.append('"')
                pos += 2
                continue
            if not chars:
                raise ValueError("Invalid policy expression identifier")
            return "".join(chars), pos + 1
        chars.append(char)
        pos += 1

    raise ValueError("Unterminated identifier in policy expression")
