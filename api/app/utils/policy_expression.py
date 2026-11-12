"""Safe tokenizer, parser, and SQL renderer for row-level security policy expressions."""

from dataclasses import dataclass
from typing import List, Tuple

from app.utils.utils import pg_quote_ident, pg_quote_literal

_KEYWORDS = {"AND", "OR", "NOT", "IN", "IS", "NULL", "TRUE", "FALSE"}
_COMPARISON_OPS = {"=", "!=", "<>", "<", "<=", ">", ">="}


@dataclass(frozen=True)
class _Token:
    kind: str
    value: str


def _read_quoted(text: str, start: int, quote: str) -> Tuple[str, int]:
    """Read a single- or double-quoted SQL token with doubled-quote escaping."""
    chars = []
    i = start + 1
    length = len(text)
    while i < length:
        ch = text[i]
        if ch == quote:
            if i + 1 < length and text[i + 1] == quote:
                chars.append(quote)
                i += 2
                continue
            return "".join(chars), i + 1
        chars.append(ch)
        i += 1
    raise ValueError("Unterminated quoted token in policy expression")


def _read_number(text: str, start: int) -> Tuple[str, int]:
    """Read an integer or decimal literal starting at start."""
    i = start + 1 if text[start] == "-" else start
    length = len(text)
    while i < length and text[i].isdigit():
        i += 1
    if i < length and text[i] == ".":
        i += 1
        if i >= length or not text[i].isdigit():
            raise ValueError("Invalid numeric literal in policy expression")
        while i < length and text[i].isdigit():
            i += 1
    return text[start:i], i


def _read_word(text: str, start: int) -> Tuple[_Token, int]:
    """Read a keyword or unquoted identifier starting at start."""
    i = start
    length = len(text)
    while i < length and (text[i].isalnum() or text[i] == "_"):
        i += 1
    word = text[start:i]
    upper = word.upper()
    kind = upper if upper in _KEYWORDS else "IDENT"
    value = upper if upper in _KEYWORDS else word
    return _Token(kind, value), i


def _tokenize(text: str) -> List[_Token]:
    tokens: List[_Token] = []
    i = 0
    length = len(text)

    while i < length:
        ch = text[i]

        if ch.isspace():
            i += 1
        elif ch in ("\x00", ";"):
            raise ValueError("Unsupported character in policy expression")
        elif text.startswith("--", i) or text.startswith("/*", i):
            raise ValueError("Comments are not allowed in policy expressions")
        elif ch in ("(", ")", ","):
            tokens.append(_Token(ch, ch))
            i += 1
        elif text[i : i + 2] in ("!=", "<>", "<=", ">="):
            tokens.append(_Token("OP", text[i : i + 2]))
            i += 2
        elif ch in ("=", "<", ">"):
            tokens.append(_Token("OP", ch))
            i += 1
        elif ch == "'":
            val, i = _read_quoted(text, i, "'")
            tokens.append(_Token("STRING", val))
        elif ch == '"':
            val, i = _read_quoted(text, i, '"')
            if val.strip() == "":
                raise ValueError("Empty identifier in policy expression")
            tokens.append(_Token("QIDENT", val))
        elif ch.isdigit() or (ch == "-" and i + 1 < length and text[i + 1].isdigit()):
            num, i = _read_number(text, i)
            tokens.append(_Token("NUMBER", num))
        elif ch.isalpha() or ch == "_":
            tok, i = _read_word(text, i)
            tokens.append(tok)
        else:
            raise ValueError(f"Unexpected character {ch!r} in policy expression")

    return tokens


class _PolicyParser:  # pylint: disable=too-few-public-methods
    """Recursive-descent parser that validates and renders safe RLS expressions."""

    def __init__(self, tokens: List[_Token]):
        self.tokens = tokens
        self.pos = 0

    def _peek(self) -> _Token | None:
        if self.pos < len(self.tokens):
            return self.tokens[self.pos]
        return None

    def _consume(self, expected_kind: str | None = None) -> _Token:
        tok = self._peek()
        if tok is None:
            raise ValueError("Unexpected end of policy expression")
        if expected_kind is not None and tok.kind != expected_kind:
            raise ValueError(f"Expected {expected_kind}, got {tok.kind} ({tok.value})")
        self.pos += 1
        return tok

    def parse(self) -> str:
        """Parse a full expression and ensure no trailing tokens remain."""
        if not self.tokens:
            raise ValueError("Policy expression must not be empty")
        result = self._parse_or()
        if self._peek() is not None:
            extra = self._peek()
            raise ValueError(
                f"Unexpected trailing token in policy expression: {extra.value}"
            )
        return result

    def _parse_or(self) -> str:
        parts = [self._parse_and()]
        while self._peek() is not None and self._peek().kind == "OR":
            self._consume("OR")
            parts.append(self._parse_and())
        return " OR ".join(parts)

    def _parse_and(self) -> str:
        parts = [self._parse_not()]
        while self._peek() is not None and self._peek().kind == "AND":
            self._consume("AND")
            parts.append(self._parse_not())
        return " AND ".join(parts)

    def _parse_not(self) -> str:
        tok = self._peek()
        if tok is not None and tok.kind == "NOT":
            self._consume("NOT")
            return f"NOT {self._parse_not()}"
        return self._parse_primary()

    def _parse_primary(self) -> str:
        tok = self._peek()
        if tok is None:
            raise ValueError("Unexpected end of policy expression")

        if tok.kind == "(":
            self._consume("(")
            inner = self._parse_or()
            self._consume(")")
            return f"({inner})"

        if tok.kind in ("TRUE", "FALSE"):
            self._consume()
            return tok.kind

        if tok.kind in ("IDENT", "QIDENT"):
            return self._parse_comparison()

        raise ValueError(f"Invalid start of expression: {tok.value}")

    def _parse_comparison(self) -> str:
        ident_tok = self._consume()
        ident_sql = pg_quote_ident(ident_tok.value)

        tok = self._peek()
        if tok is None:
            raise ValueError(f"Expected operator after identifier {ident_tok.value!r}")

        if tok.kind == "IS":
            self._consume("IS")
            if self._peek() is not None and self._peek().kind == "NOT":
                self._consume("NOT")
                self._consume("NULL")
                return f"{ident_sql} IS NOT NULL"
            self._consume("NULL")
            return f"{ident_sql} IS NULL"

        if tok.kind == "NOT":
            self._consume("NOT")
            self._consume("IN")
            values = self._parse_in_list()
            return f"{ident_sql} NOT IN ({values})"

        if tok.kind == "IN":
            self._consume("IN")
            values = self._parse_in_list()
            return f"{ident_sql} IN ({values})"

        if tok.kind == "OP" and tok.value in _COMPARISON_OPS:
            op = self._consume("OP").value
            lit = self._parse_literal()
            return f"{ident_sql} {op} {lit}"

        raise ValueError(
            f"Unsupported operator {tok.value!r} after {ident_tok.value!r}"
        )

    def _parse_in_list(self) -> str:
        self._consume("(")
        items = [self._parse_literal()]
        while self._peek() is not None and self._peek().kind == ",":
            self._consume(",")
            items.append(self._parse_literal())
        self._consume(")")
        return ", ".join(items)

    def _parse_literal(self) -> str:
        tok = self._peek()
        if tok is None:
            raise ValueError("Expected literal value in policy expression")

        if tok.kind == "STRING":
            self._consume("STRING")
            return pg_quote_literal(tok.value)

        if tok.kind == "NUMBER":
            self._consume("NUMBER")
            return tok.value

        if tok.kind in ("TRUE", "FALSE"):
            self._consume()
            return tok.kind

        raise ValueError(f"Expected literal value, got {tok.value!r}")


def render_policy_expression(value: str) -> str:
    """Validate and safely render a row-level security policy expression."""
    if not isinstance(value, str):
        raise ValueError("Policy expression must be a string")

    expression = value.strip()
    if expression == "":
        raise ValueError("Policy expression must not be empty")

    tokens = _tokenize(expression)
    return _PolicyParser(tokens).parse()
