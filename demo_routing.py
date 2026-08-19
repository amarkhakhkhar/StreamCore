#!/usr/bin/env python
"""
Demo: 3 different routing rules correctly filtering and routing a stream
of test messages to the right destinations.

This demonstrates the RPN-based routing engine in action.
"""

from services.broker.routing import RoutingEngine, create_routing_engine


def demo_basic_routing():
    """Demo 1: Basic topic-based routing."""
    print("=" * 60)
    print("DEMO 1: Basic Topic-Based Routing")
    print("=" * 60)

    engine = RoutingEngine(default_target="default-topic")
    engine.add_rule("orders", "$.topic 'orders' ==", "orders-partition")
    engine.add_rule("events", "$.topic 'events' ==", "events-partition")
    engine.add_rule("users", "$.topic 'users' ==", "users-partition")

    messages = [
        {"topic": "orders", "data": "order-001", "headers": {}},
        {"topic": "events", "data": "user-clicked", "headers": {}},
        {"topic": "users", "data": "user-profile-update", "headers": {}},
        {"topic": "unknown", "data": "test", "headers": {}},
    ]

    for msg in messages:
        target = engine.route(msg)
        print(f"  topic={msg['topic']:12} -> {target}")

    print()


def demo_priority_routing():
    """Demo 2: Priority-based routing with headers."""
    print("=" * 60)
    print("DEMO 2: Priority-Based Routing")
    print("=" * 60)

    engine = RoutingEngine(default_target="normal-queue")
    engine.add_rule("high-priority", "$.headers.priority 'high' ==", "priority-queue")
    engine.add_rule("low-priority", "$.headers.priority 'low' ==", "low-priority-queue")

    messages = [
        {"topic": "orders", "data": "urgent-order", "headers": {"priority": "high"}},
        {"topic": "orders", "data": "regular-order", "headers": {"priority": "normal"}},
        {"topic": "events", "data": "batch-job", "headers": {"priority": "low"}},
        {"topic": "events", "data": "realtime-event", "headers": {"priority": "high"}},
        {"topic": "test", "data": "no-headers", "headers": {}},
    ]

    for msg in messages:
        target = engine.route(msg)
        priority = msg['headers'].get('priority', 'none')
        print(f"  priority={priority:10} topic={msg['topic']:12} -> {target}")

    print()


def demo_content_based_routing():
    """Demo 3: Content-based routing (error detection, large messages)."""
    print("=" * 60)
    print("DEMO 3: Content-Based Routing (Error + Size)")
    print("=" * 60)

    engine = RoutingEngine(default_target="default-topic")
    engine.add_rule("errors", "$.data 'error' contains", "dead-letter-topic")
    engine.add_rule("large-msgs", "$.data length 1000 >", "large-messages-topic")
    engine.add_rule("orders", "$.topic 'orders' ==", "orders-partition")

    messages = [
        {"topic": "orders", "data": "normal order processing", "headers": {}},
        {"topic": "orders", "data": "error: database connection failed", "headers": {}},
        {"topic": "events", "data": "x" * 1500, "headers": {}},  # Large message
        {"topic": "events", "data": "small event", "headers": {}},
        {"topic": "orders", "data": "error: timeout", "headers": {}},
    ]

    for msg in messages:
        target = engine.route(msg)
        data_preview = msg['data'][:40] + ("..." if len(msg['data']) > 40 else "")
        print(f"  data={data_preview:45} -> {target}")

    print()


def demo_complex_rules():
    """Demo 4: Complex combined routing rules."""
    print("=" * 60)
    print("DEMO 4: Complex Combined Rules")
    print("=" * 60)

    engine = create_routing_engine()  # Uses the factory with default rules

    # The factory creates these rules (in RPN format):
    # - orders-routing: $.topic 'orders' ==
    # - priority-routing: $.headers.priority 'high' ==
    # - error-routing: $.data 'error' contains
    # - large-message-routing: $.data length 1000 >

    test_messages = [
        # Normal order
        {"topic": "orders", "data": "create order", "headers": {}},
        # High priority order
        {"topic": "orders", "data": "urgent order", "headers": {"priority": "high"}},
        # Order with error
        {"topic": "orders", "data": "error: payment failed", "headers": {}},
        # Large message (non-order)
        {"topic": "analytics", "data": "x" * 2000, "headers": {}},
        # Unknown topic, normal priority
        {"topic": "unknown", "data": "test message", "headers": {"priority": "normal"}},
    ]

    print("Routing engine rules:")
    for rule in engine.rules:
        print(f"  {rule.name}: {rule.expression} -> {rule.target}")
    print()

    for msg in test_messages:
        target = engine.route(msg)
        matched = None
        for rule in engine.rules:
            if rule.matches(msg):
                matched = rule.name
                break
        priority = msg['headers'].get('priority', 'none')
        data_preview = msg['data'][:30] + ("..." if len(msg['data']) > 30 else "")
        print(f"  topic={msg['topic']:12} priority={priority:6} data={data_preview:35} -> {target} (matched: {matched})")


if __name__ == "__main__":
    demo_basic_routing()
    demo_priority_routing()
    demo_content_based_routing()
    demo_complex_rules()

    print("=" * 60)
    print("All demos completed successfully!")
    print("=" * 60)