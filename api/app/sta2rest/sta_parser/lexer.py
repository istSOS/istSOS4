"""
Lexer for the SensorThings API.

Author: Filippo Finke
"""

import re
import urllib.parse

# Define the token types
TOKEN_TYPES = {
    "COUNT": r"\$count=",
    "TOP": r"\$top=",
    "SKIP": r"\$skip=",
    "SELECT": r"\$select=",
    "FILTER": r"\$filter=",
    "EXPAND": r"\$expand=",
    "ORDERBY": r"\$orderby=",
    "ASOF": r"\$as_of=",
    "FROMTO": r"\$from_to=",
    "RESULT_FORMAT": r"\$resultFormat=",
    "RESULT_FORMAT_VALUE": r"\bdataArray\b",
    "SUBQUERY_SEPARATOR": r";",
    "VALUE_SEPARATOR": r",",
    "OPTIONS_SEPARATOR": r"&",
    "ORDER": r"\basc\b|\bdesc\b",
    "BOOL": r"\btrue\b|\bfalse\b",
    "TIMESTAMP": r"[1-9]\d{3}-(?:0\d|1[0-2])-(?:[0-2]\d|3[01])"
    + r"T"
    + r"(?:[01]\d|2[0-3]):[0-5]\d(:?:[0-5]\d(?:\.\d{1,12})?)"
    + r"?(Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)?",
    "EXPAND_IDENTIFIER": r"[a-zA-Z_][.a-zA-Z0-9_]*",
    "IDENTIFIER": r"[a-zA-Z_][.a-zA-Z0-9_/]*",
    "INTEGER": r"[0-9]+",
    "STRING": r"'[^']*'",
    "LEFT_PAREN": r"\(",
    "RIGHT_PAREN": r"\)",
    "WHITESPACE": r"\s+",
    "EXPAND_SEPARATOR": r"\/",
}


class Token:
    """A class representing a token."""

    def __init__(self, type, value):
        """
        Initialize a new Token object.

        Args:
            type (str): The type of the token.
            value (str): The value of the token.
        """
        self.type = type
        self.value = value

    def __str__(self):
        """
        Return a string representation of the token.

        Returns:
            str: The string representation of the token.
        """
        return f"Token({self.type}, {self.value})"


class Lexer:
    """A class for tokenizing SensorThings API queries."""

    def __init__(self, text):
        """
        Initialize a new Lexer object.

        Args:
            text (str): The input text to be tokenized.
        """

        if "'" in urllib.parse.unquote(text):
            if "+" in text:
                self.text = urllib.parse.unquote_plus(text)
            else:
                self.text = urllib.parse.unquote(text)
        else:
            self.text = urllib.parse.unquote(text)

        self.tokens = self.tokenize()

    def tokenize(self):
        """
        Tokenize the input text.

        Returns:
            list: A list of Token objects representing the tokens.
        """
        tokens = []
        position = 0

        while position < len(self.text):
            match = None

            for token_type, pattern in TOKEN_TYPES.items():
                regex = re.compile(pattern)
                match = regex.match(self.text, position)

                if match:
                    value = match.group(0)
                    token = Token(token_type, value)
                    tokens.append(token)
                    position = match.end(0)
                    break

            if not match:
                raise Exception(
                    f"Invalid character at position {position}: {self.text[position]}"
                )

        return tokens

    def __str__(self):
        """
        Return a string representation of the lexer.

        Returns:
            str: The string representation of the lexer.
        """
        return "\n".join(str(token) for token in self.tokens)


# Example usage
if __name__ == "__main__":
    text = """$expand=Locations,Datastreams($select=id,name,unitOfMeasurement;$expand=ObservedProperty($select=name),Observations($select=result,phenomenonTime;$orderby=phenomenonTime desc;$top=1))"""
    lexer = Lexer(text)
    print(lexer)
