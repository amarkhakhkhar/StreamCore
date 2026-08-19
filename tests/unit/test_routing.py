"""Tests for RPN-based routing expression evaluator."""

import pytest
from services.broker.routing import (
    RPNEvaluator,
    RoutingEngine,
    RoutingRule,
    create_routing_engine,
    TokenType,
)


class TestRPNEvaluator:
    """Tests for the RPN expression evaluator."""

    def test_simple_number(self):
        """Test evaluating a simple number."""
        evaluator = RPNEvaluator()
        result = evaluator.evaluate("42", {})
        assert result == 42

    def test_simple_string(self):
        """Test evaluating a simple string."""
        evaluator = RPNEvaluator()
        result = evaluator.evaluate("'hello'", {})
        assert result == "hello"

    def test_boolean_literals(self):
        """Test boolean literals."""
        evaluator = RPNEvaluator()
        assert evaluator.evaluate("true", {}) is True
        assert evaluator.evaluate("false", {}) is False

    def test_field_access(self):
        """Test field access from context."""
        evaluator = RPNEvaluator()
        context = {"topic": "orders", "data": "test message"}
        result = evaluator.evaluate("$.topic", context)
        assert result == "orders"

        result = evaluator.evaluate("$.data", context)
        assert result == "test message"

    def test_nested_field_access(self):
        """Test nested field access."""
        evaluator = RPNEvaluator()
        context = {"headers": {"priority": "high", "trace_id": "abc123"}}
        result = evaluator.evaluate("$.headers.priority", context)
        assert result == "high"

        result = evaluator.evaluate("$.headers.trace_id", context)
        assert result == "abc123"

    def test_arithmetic_operators(self):
        """Test arithmetic operators."""
        evaluator = RPNEvaluator()
        assert evaluator.evaluate("2 3 +", {}) == 5
        assert evaluator.evaluate("10 3 -", {}) == 7
        assert evaluator.evaluate("4 5 *", {}) == 20
        assert evaluator.evaluate("20 4 /", {}) == 5
        assert evaluator.evaluate("17 5 %", {}) == 2

    def test_comparison_operators(self):
        """Test comparison operators."""
        evaluator = RPNEvaluator()
        context = {"value": 10}

        assert evaluator.evaluate("$.value 10 ==", context) is True
        assert evaluator.evaluate("$.value 5 ==", context) is False
        assert evaluator.evaluate("$.value 10 !=", context) is False
        assert evaluator.evaluate("$.value 5 !=", context) is True
        assert evaluator.evaluate("$.value 5 >", context) is True
        assert evaluator.evaluate("$.value 15 >", context) is False
        assert evaluator.evaluate("$.value 15 <", context) is True
        assert evaluator.evaluate("$.value 10 >=", context) is True
        assert evaluator.evaluate("$.value 10 <=", context) is True

    def test_logical_operators(self):
        """Test logical operators."""
        evaluator = RPNEvaluator()

        assert evaluator.evaluate("true true and", {}) is True
        assert evaluator.evaluate("true false and", {}) is False
        assert evaluator.evaluate("false false or", {}) is False
        assert evaluator.evaluate("true false or", {}) is True
        assert evaluator.evaluate("true not", {}) is False
        assert evaluator.evaluate("false not", {}) is True

    def test_complex_expression(self):
        """Test complex expression with field access and operators."""
        evaluator = RPNEvaluator()
        context = {"topic": "orders", "data": "test", "headers": {"priority": "high"}}

        # topic == 'orders' and priority == 'high'
        expr = "$.topic 'orders' == $.headers.priority 'high' == and"
        result = evaluator.evaluate(expr, context)
        assert result is True

        # Change priority to low
        context["headers"]["priority"] = "low"
        result = evaluator.evaluate(expr, context)
        assert result is False

    def test_contains_function(self):
        """Test contains function."""
        evaluator = RPNEvaluator()
        context = {"data": "error: connection failed"}

        assert evaluator.evaluate("$.data 'error' contains", context) is True
        assert evaluator.evaluate("$.data 'warning' contains", context) is False

    def test_startswith_endswith_functions(self):
        """Test startswith and endswith functions."""
        evaluator = RPNEvaluator()
        context = {"topic": "orders.created"}

        assert evaluator.evaluate("$.topic 'orders.' startswith", context) is True
        assert evaluator.evaluate("$.topic '.created' endswith", context) is True
        assert evaluator.evaluate("$.topic 'events.' startswith", context) is False

    def test_length_function(self):
        """Test length function."""
        evaluator = RPNEvaluator()
        context = {"data": "hello world"}

        assert evaluator.evaluate("$.data length", context) == 11

    def test_lower_upper_functions(self):
        """Test lower and upper functions."""
        evaluator = RPNEvaluator()
        context = {"data": "Hello World"}

        assert evaluator.evaluate("$.data lower", context) == "hello world"
        assert evaluator.evaluate("$.data upper", context) == "HELLO WORLD"

    def test_equals_function(self):
        """Test equals function."""
        evaluator = RPNEvaluator()
        context = {"topic": "orders"}

        assert evaluator.evaluate("$.topic 'orders' equals", context) is True
        assert evaluator.evaluate("$.topic 'events' equals", context) is False

    def test_matches_function(self):
        """Test regex matches function."""
        evaluator = RPNEvaluator()
        context = {"data": "order-12345"}

        # RPN: field pattern matches
        assert evaluator.evaluate("$.data 'order-\\d+' matches", context) is True
        assert evaluator.evaluate("$.data 'event-\\d+' matches", context) is False

    def test_infix_expression_simple(self):
        """Test simple infix expression (converted to RPN)."""
        evaluator = RPNEvaluator()
        context = {"topic": "orders", "priority": "high"}

        # Simple infix: $.topic == 'orders'
        result = evaluator.evaluate("$.topic == 'orders'", context)
        assert result is True

        result = evaluator.evaluate("$.topic == 'events'", context)
        assert result is False

    def test_infix_with_and(self):
        """Test infix with and operator."""
        evaluator = RPNEvaluator()
        context = {"topic": "orders", "headers": {"priority": "high"}}

        result = evaluator.evaluate("$.topic == 'orders' and $.headers.priority == 'high'", context)
        assert result is True

        context["headers"]["priority"] = "low"
        result = evaluator.evaluate("$.topic == 'orders' and $.headers.priority == 'high'", context)
        assert result is False

    def test_infix_with_or(self):
        """Test infix with or operator."""
        evaluator = RPNEvaluator()
        context = {"topic": "orders"}

        result = evaluator.evaluate("$.topic == 'orders' or $.topic == 'events'", context)
        assert result is True

        context["topic"] = "unknown"
        result = evaluator.evaluate("$.topic == 'orders' or $.topic == 'events'", context)
        assert result is False


class TestRoutingRule:
    """Tests for RoutingRule class."""

    def test_simple_match(self):
        """Test simple routing rule match."""
        rule = RoutingRule("test-rule", "$.topic == 'orders'", "orders-partition")

        message = {"topic": "orders", "data": "test"}
        assert rule.matches(message) is True

        message = {"topic": "events", "data": "test"}
        assert rule.matches(message) is False

    def test_dynamic_target(self):
        """Test dynamic target evaluation."""
        rule = RoutingRule("test-rule", "$.topic == 'orders'", "$.topic + '-partition'")

        message = {"topic": "orders", "data": "test"}
        target = rule.evaluate_target(message)
        assert target == "orders-partition"


class TestRoutingEngine:
    """Tests for RoutingEngine class."""

    def test_default_routing(self):
        """Test default routing when no rules match."""
        engine = RoutingEngine(default_target="default-topic")

        message = {"topic": "unknown", "data": "test"}
        target = engine.route(message)
        assert target == "default-topic"

    def test_first_matching_rule(self):
        """Test that first matching rule wins."""
        engine = RoutingEngine(default_target="default-topic")
        engine.add_rule("rule1", "$.topic == 'orders'", "orders-partition")
        engine.add_rule("rule2", "$.topic == 'events'", "events-partition")

        message = {"topic": "orders", "data": "test"}
        target = engine.route(message)
        assert target == "orders-partition"

    def test_priority_routing(self):
        """Test priority-based routing."""
        engine = RoutingEngine(default_target="default-topic")
        engine.add_rule("high-priority", "$.headers.priority == 'high'", "priority-partition")
        engine.add_rule("normal", "true", "normal-partition")

        message = {"topic": "test", "data": "msg", "headers": {"priority": "high"}}
        target = engine.route(message)
        assert target == "priority-partition"

        message = {"topic": "test", "data": "msg", "headers": {"priority": "normal"}}
        target = engine.route(message)
        assert target == "normal-partition"

    def test_error_routing(self):
        """Test error message routing to dead-letter."""
        engine = RoutingEngine(default_target="default-topic")
        engine.add_rule("error-route", "$.data 'error' contains", "dead-letter")

        message = {"topic": "orders", "data": "error: connection timeout", "headers": {}}
        target = engine.route(message)
        assert target == "dead-letter"

        message = {"topic": "orders", "data": "order created successfully", "headers": {}}
        target = engine.route(message)
        assert target == "default-topic"

    def test_large_message_routing(self):
        """Test large message routing."""
        engine = RoutingEngine(default_target="default-topic")
        engine.add_rule("large-msg", "$.data length 100 >", "large-messages")

        message = {"topic": "test", "data": "x" * 150, "headers": {}}
        target = engine.route(message)
        assert target == "large-messages"

        message = {"topic": "test", "data": "small", "headers": {}}
        target = engine.route(message)
        assert target == "default-topic"

    def test_batch_routing(self):
        """Test routing a batch of messages."""
        engine = RoutingEngine(default_target="default-topic")
        engine.add_rule("orders", "$.topic == 'orders'", "orders-partition")
        engine.add_rule("events", "$.topic == 'events'", "events-partition")

        messages = [
            {"topic": "orders", "data": "order-1", "headers": {}},
            {"topic": "events", "data": "event-1", "headers": {}},
            {"topic": "unknown", "data": "test", "headers": {}},
        ]

        results = engine.route_batch(messages)

        assert results[0][1] == "orders-partition"
        assert results[1][1] == "events-partition"
        assert results[2][1] == "default-topic"

    def test_created_routing_engine(self):
        """Test the factory function creates engine with default rules."""
        engine = create_routing_engine()

        # Test default rules (RPN format)
        assert engine.route({"topic": "orders", "data": "test", "headers": {}}) == "orders-partition"
        assert engine.route({"topic": "test", "data": "test", "headers": {"priority": "high"}}) == "priority-partition"
        assert engine.route({"topic": "test", "data": "error: something failed", "headers": {}}) == "dead-letter-topic"
        assert engine.route({"topic": "test", "data": "x" * 1500, "headers": {}}) == "large-messages-topic"
        assert engine.route({"topic": "unknown", "data": "test", "headers": {}}) == "default-topic"


class TestRoutingEdgeCases:
    """Tests for edge cases and error handling."""

    def test_missing_field_returns_false(self):
        """Test that missing fields return None/False gracefully."""
        evaluator = RPNEvaluator()
        context = {"topic": "orders"}

        # Missing field returns None, comparison with None is False
        result = evaluator.evaluate("$.missing.field == 'value'", context)
        assert result is False

    def test_empty_context(self):
        """Test with empty context."""
        evaluator = RPNEvaluator()
        result = evaluator.evaluate("$.topic == 'orders'", {})
        assert result is False

    def test_division_by_zero(self):
        """Test division by zero handling."""
        evaluator = RPNEvaluator()
        result = evaluator.evaluate("10 0 /", {})
        assert result == 0  # Protected division

    def test_modulo_by_zero(self):
        """Test modulo by zero handling."""
        evaluator = RPNEvaluator()
        result = evaluator.evaluate("10 0 %", {})
        assert result == 0  # Protected modulo

    def test_complex_routing_scenario(self):
        """Test a realistic routing scenario with multiple rules."""
        engine = RoutingEngine(default_target="default")

        # High priority messages go to priority queue
        engine.add_rule("priority", "$.headers.priority 'high' ==", "priority-queue")

        # Error messages go to dead letter
        engine.add_rule("errors", "$.data 'error' contains", "dead-letter")

        # Large messages go to large message topic
        engine.add_rule("large", "$.data length 1000 >", "large-messages")

        # Orders topic messages
        engine.add_rule("orders", "$.topic 'orders' ==", "orders-partition")

        # Events topic messages
        engine.add_rule("events", "$.topic 'events' ==", "events-partition")

        # Test cases
        test_cases = [
            ({"topic": "orders", "data": "small order", "headers": {}}, "orders-partition"),
            ({"topic": "events", "data": "user clicked", "headers": {}}, "events-partition"),
            ({"topic": "orders", "data": "order", "headers": {"priority": "high"}}, "priority-queue"),
            ({"topic": "orders", "data": "error: failed to process", "headers": {}}, "dead-letter"),
            ({"topic": "orders", "data": "x" * 2000, "headers": {}}, "large-messages"),
            ({"topic": "unknown", "data": "test", "headers": {}}, "default"),
        ]

        for message, expected in test_cases:
            target = engine.route(message)
            assert target == expected, f"Failed for {message}: got {target}, expected {expected}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])