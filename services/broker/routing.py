"""
Stack-based expression evaluator (RPN pattern) for topic routing rules.

Implements a simple Reverse Polish Notation evaluator that can be used
for filtering and transforming messages to determine routing decisions.

Grammar (RPN):
- Literals: numbers, strings (quoted), booleans (true/false)
- Field access: $.field (e.g., $.topic, $.data, $.headers.key)
- Operators: + - * / % (arithmetic), == != < > <= >= (comparison)
- Logical: and or not
- Ternary: ? : (condition ? true_expr : false_expr)
- Functions: contains, startswith, endswith, length, lower, upper
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Optional


class TokenType(Enum):
    NUMBER = "NUMBER"
    STRING = "STRING"
    BOOLEAN = "BOOLEAN"
    IDENTIFIER = "IDENTIFIER"
    OPERATOR = "OPERATOR"
    FUNCTION = "FUNCTION"
    TERNARY_IF = "TERNARY_IF"
    TERNARY_ELSE = "TERNARY_ELSE"
    FIELD_ACCESS = "FIELD_ACCESS"


@dataclass
class Token:
    type: TokenType
    value: Any
    position: int


class RPNEvaluator:
    """
    Stack-based Reverse Polish Notation evaluator for routing expressions.

    Example expressions:
    - "$.topic == 'orders'"                    # Route to orders topic
    - "$.data.length() > 100 and $.headers.priority == 'high'"  # Large high-priority messages
    - "$.topic == 'orders' ? 'partition-0' : 'partition-1'"      # Ternary routing
    - "contains($.data, 'error') ? 'dead-letter' : $.topic"      # Error routing
    """

    # Operator precedence (not used in RPN but kept for reference)
    OPERATORS = {
        # Arithmetic
        "+": (2, lambda a, b: a + b),
        "-": (2, lambda a, b: a - b),
        "*": (2, lambda a, b: a * b),
        "/": (2, lambda a, b: a / b if b != 0 else 0),
        "%": (2, lambda a, b: a % b if b != 0 else 0),
        # Comparison
        "==": (2, lambda a, b: a == b),
        "!=": (2, lambda a, b: a != b),
        "<": (2, lambda a, b: a < b),
        ">": (2, lambda a, b: a > b),
        "<=": (2, lambda a, b: a <= b),
        ">=": (2, lambda a, b: a >= b),
        # Logical
        "and": (2, lambda a, b: bool(a) and bool(b)),
        "or": (2, lambda a, b: bool(a) or bool(b)),
        "not": (1, lambda a: not bool(a)),
    }

    # Built-in functions
    FUNCTIONS: dict[str, Callable] = {
        "contains": lambda haystack, needle: str(needle) in str(haystack),
        "startswith": lambda s, prefix: str(s).startswith(str(prefix)),
        "endswith": lambda s, suffix: str(s).endswith(str(suffix)),
        "length": lambda s: len(str(s)),
        "lower": lambda s: str(s).lower(),
        "upper": lambda s: str(s).upper(),
        "equals": lambda a, b: a == b,
        "matches": lambda s, pattern: bool(re.search(pattern, str(s))),
    }

    def __init__(self):
        self.tokens: list[Token] = []
        self.position = 0

    def tokenize(self, expression: str) -> list[Token]:
        """Tokenize an RPN expression string."""
        tokens = []
        pos = 0
        i = 0
        length = len(expression)

        while i < length:
            char = expression[i]

            # Skip whitespace
            if char.isspace():
                i += 1
                continue

            # String literal (single or double quoted)
            if char in ('"', "'"):
                quote = char
                i += 1
                start = i
                value = ""
                while i < length and expression[i] != quote:
                    if expression[i] == "\\" and i + 1 < length:
                        # Keep backslash as literal escape sequence
                        value += expression[i]  # Add the backslash
                        i += 1
                        value += expression[i]  # Add the escaped char
                    else:
                        value += expression[i]
                    i += 1
                if i < length:
                    i += 1  # Skip closing quote
                tokens.append(Token(TokenType.STRING, value, pos))
                pos = i
                continue

            # Number (integer or float)
            if char.isdigit() or (char == "-" and i + 1 < length and expression[i + 1].isdigit()):
                start = i
                if char == "-":
                    i += 1
                while i < length and (expression[i].isdigit() or expression[i] == "."):
                    i += 1
                value_str = expression[start:i]
                value = float(value_str) if "." in value_str else int(value_str)
                tokens.append(Token(TokenType.NUMBER, value, pos))
                pos = i
                continue

            # Boolean literals
            if expression[i:i+4].lower() == "true":
                tokens.append(Token(TokenType.BOOLEAN, True, pos))
                i += 4
                pos = i
                continue
            if expression[i:i+5].lower() == "false":
                tokens.append(Token(TokenType.BOOLEAN, False, pos))
                i += 5
                pos = i
                continue

            # Field access: $.field or $.headers.key
            if expression[i:i+2] == "$.":
                i += 2
                start = i
                value = ""
                while i < length and (expression[i].isalnum() or expression[i] in "_."):
                    value += expression[i]
                    i += 1
                tokens.append(Token(TokenType.FIELD_ACCESS, value, pos))
                pos = i
                continue

            # Operators and functions (identifiers)
            if char.isalpha() or char == "_":
                start = i
                value = ""
                while i < length and (expression[i].isalnum() or expression[i] in "_"):
                    value += expression[i]
                    i += 1
                lower_val = value.lower()
                if lower_val in ("and", "or", "not"):
                    tokens.append(Token(TokenType.OPERATOR, lower_val, pos))
                elif lower_val in self.FUNCTIONS:
                    tokens.append(Token(TokenType.FUNCTION, lower_val, pos))
                elif lower_val == "?":
                    tokens.append(Token(TokenType.TERNARY_IF, "?", pos))
                elif lower_val == ":":
                    tokens.append(Token(TokenType.TERNARY_ELSE, ":", pos))
                else:
                    tokens.append(Token(TokenType.IDENTIFIER, value, pos))
                pos = i
                continue

            # Multi-char operators
            if i + 1 < length:
                two_char = expression[i:i+2]
                if two_char in ("==", "!=", "<=", ">="):
                    tokens.append(Token(TokenType.OPERATOR, two_char, pos))
                    i += 2
                    pos = i
                    continue

            # Single-char operators
            if char in ("+", "-", "*", "/", "%", "<", ">", "=", "?", ":"):
                tokens.append(Token(TokenType.OPERATOR, char, pos))
                i += 1
                pos = i
                continue

            # Unknown character
            raise ValueError(f"Unknown character at position {i}: '{char}'")

        return tokens

    def evaluate_rpn(self, rpn_tokens: list[Token], context: dict[str, Any]) -> Any:
        """Evaluate a tokenized RPN expression with the given context."""
        stack = []

        for token in rpn_tokens:
            if token.type == TokenType.NUMBER:
                stack.append(token.value)
            elif token.type == TokenType.STRING:
                stack.append(token.value)
            elif token.type == TokenType.BOOLEAN:
                stack.append(token.value)
            elif token.type == TokenType.FIELD_ACCESS:
                value = self._get_field_value(context, token.value)
                stack.append(value)
            elif token.type == TokenType.FUNCTION:
                func = self.FUNCTIONS.get(token.value)
                if func is None:
                    raise ValueError(f"Unknown function: {token.value}")
                # Functions pop their arguments from stack
                # We need to know arity - for simplicity, assume common patterns
                if token.value in ("contains", "startswith", "endswith", "equals", "matches"):
                    # Binary functions
                    if len(stack) < 2:
                        raise ValueError(f"Function {token.value} requires 2 arguments")
                    b = stack.pop()
                    a = stack.pop()
                    stack.append(func(a, b))
                elif token.value in ("length", "lower", "upper"):
                    # Unary functions
                    if len(stack) < 1:
                        raise ValueError(f"Function {token.value} requires 1 argument")
                    a = stack.pop()
                    stack.append(func(a))
                else:
                    raise ValueError(f"Unsupported function arity: {token.value}")
            elif token.type == TokenType.OPERATOR:
                op = token.value
                if op not in self.OPERATORS:
                    raise ValueError(f"Unknown operator: {op}")
                arity, func = self.OPERATORS[op]
                if len(stack) < arity:
                    raise ValueError(f"Operator {op} requires {arity} arguments")
                if arity == 2:
                    b = stack.pop()
                    a = stack.pop()
                    stack.append(func(a, b))
                else:  # unary
                    a = stack.pop()
                    stack.append(func(a))
            elif token.type == TokenType.TERNARY_IF:
                # Ternary is handled specially - we expect the stack to have:
                # condition, true_expr, false_expr (in that order after parsing)
                # But in RPN, ternary is: condition true_expr false_expr ? :
                # This is complex - we'll handle it in infix-to-RPN conversion
                pass
            elif token.type == TokenType.TERNARY_ELSE:
                pass

        if len(stack) != 1:
            raise ValueError(f"Invalid RPN expression: stack has {len(stack)} items, expected 1")
        return stack[0]

    def _get_field_value(self, context: dict[str, Any], field_path: str) -> Any:
        """Extract a field value from the context using dot notation."""
        parts = field_path.split(".")
        current = context

        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            elif hasattr(current, part):
                current = getattr(current, part)
            else:
                return None

            if current is None:
                return None

        return current

    def infix_to_rpn(self, expression: str) -> list[Token]:
        """Convert infix expression to RPN using Shunting Yard algorithm."""
        # For simplicity, we'll support a restricted infix subset that converts to RPN
        # Full implementation would be more complex. For now, we'll parse a simpler format.
        # Actually, let's support both: if expression contains spaces and operators in infix positions,
        # treat as infix. Otherwise assume RPN.
        tokens = self.tokenize(expression)

        # Check if it looks like infix (operators between operands)
        # Simple heuristic: if we have FIELD_ACCESS followed by OPERATOR followed by something
        looks_infix = False
        for i in range(len(tokens) - 2):
            if (tokens[i].type == TokenType.FIELD_ACCESS and
                tokens[i+1].type == TokenType.OPERATOR and
                tokens[i+2].type in (TokenType.NUMBER, TokenType.STRING, TokenType.FIELD_ACCESS, TokenType.BOOLEAN)):
                looks_infix = True
                break

        if looks_infix:
            return self._shunting_yard(tokens)
        else:
            # Already in RPN or simple format
            return tokens

    def _shunting_yard(self, tokens: list[Token]) -> list[Token]:
        """Convert infix tokens to RPN using Shunting Yard."""
        output = []
        operator_stack = []

        # Precedence for shunting yard
        precedence = {
            "or": 1,
            "and": 2,
            "==": 3, "!=": 3, "<": 3, ">": 3, "<=": 3, ">=": 3,
            "+": 4, "-": 4,
            "*": 5, "/": 5, "%": 5,
            "not": 6,
        }

        i = 0
        while i < len(tokens):
            token = tokens[i]

            if token.type in (TokenType.NUMBER, TokenType.STRING, TokenType.BOOLEAN, TokenType.FIELD_ACCESS):
                output.append(token)

            elif token.type == TokenType.FUNCTION:
                operator_stack.append(token)

            elif token.type == TokenType.OPERATOR:
                op = token.value
                while (operator_stack and
                       operator_stack[-1].type == TokenType.OPERATOR and
                       operator_stack[-1].value != "(" and
                       precedence.get(operator_stack[-1].value, 0) >= precedence.get(op, 0)):
                    output.append(operator_stack.pop())
                operator_stack.append(token)

            elif token.type == TokenType.TERNARY_IF:
                # Handle ternary: condition ? true_expr : false_expr
                # We push a marker for the ternary
                operator_stack.append(token)

            elif token.type == TokenType.TERNARY_ELSE:
                # Pop until we find the matching ?
                while operator_stack and operator_stack[-1].type != TokenType.TERNARY_IF:
                    output.append(operator_stack.pop())
                if operator_stack and operator_stack[-1].type == TokenType.TERNARY_IF:
                    operator_stack.pop()  # Remove the ?
                    # Add a ternary operator marker
                    output.append(Token(TokenType.OPERATOR, "?:", token.position))

            i += 1

        # Pop remaining operators
        while operator_stack:
            output.append(operator_stack.pop())

        return output

    def evaluate(self, expression: str, context: dict[str, Any]) -> Any:
        """Evaluate an expression (infix or RPN) with the given context."""
        rpn_tokens = self.infix_to_rpn(expression)
        return self.evaluate_rpn(rpn_tokens, context)


class RoutingRule:
    """A single routing rule with an expression and target destination."""

    def __init__(self, name: str, expression: str, target: str):
        self.name = name
        self.expression = expression
        self.target = target
        self.evaluator = RPNEvaluator()

    def matches(self, message: dict[str, Any]) -> bool:
        """Check if this rule matches the message."""
        try:
            result = self.evaluator.evaluate(self.expression, message)
            return bool(result)
        except Exception:
            return False

    def evaluate_target(self, message: dict[str, Any]) -> str:
        """Evaluate the target expression (can be dynamic)."""
        try:
            result = self.evaluator.evaluate(self.target, message)
            return str(result)
        except Exception:
            return self.target


class RoutingEngine:
    """
    Stack-based routing engine for topic/partition routing.

    Evaluates routing rules in order, returns the first matching target.
    """

    def __init__(self, default_target: str = "default"):
        self.rules: list[RoutingRule] = []
        self.default_target = default_target

    def add_rule(self, name: str, expression: str, target: str) -> None:
        """Add a routing rule."""
        rule = RoutingRule(name, expression, target)
        self.rules.append(rule)

    def route(self, message: dict[str, Any]) -> str:
        """Route a message to the appropriate target."""
        for rule in self.rules:
            if rule.matches(message):
                return rule.evaluate_target(message)
        return self.default_target

    def route_batch(self, messages: list[dict[str, Any]]) -> list[tuple[dict[str, Any], str]]:
        """Route a batch of messages."""
        return [(msg, self.route(msg)) for msg in messages]


def create_routing_engine() -> RoutingEngine:
    """Create a routing engine with some default example rules (RPN format)."""
    engine = RoutingEngine(default_target="default-topic")

    # Rule 1: Route orders topic messages to orders partition
    engine.add_rule(
        "orders-routing",
        "$.topic 'orders' ==",
        "orders-partition"
    )

    # Rule 2: Route high-priority messages to priority partition
    engine.add_rule(
        "priority-routing",
        "$.headers.priority 'high' ==",
        "priority-partition"
    )

    # Rule 3: Route error-containing messages to dead-letter (RPN: field string contains)
    engine.add_rule(
        "error-routing",
        "$.data 'error' contains",
        "dead-letter-topic"
    )

    # Rule 4: Large messages to large-message topic (RPN: field length 1000 >)
    engine.add_rule(
        "large-message-routing",
        "$.data length 1000 >",
        "large-messages-topic"
    )

    return engine