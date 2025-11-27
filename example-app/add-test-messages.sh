#!/bin/bash

# This script adds test messages to the orders topic to generate DLQ messages

echo "========================================="
echo "📨 Add Test Messages"
echo "========================================="
echo ""

API_URL="http://localhost:8080"
DLQ_TOPIC="test-dlq"

echo "Adding test messages that will go to DLQ..."
echo ""

# Message 1: Invalid amount (non-retryable - IllegalArgumentException)
echo "Sending: Order with negative amount (non-retryable)"
curl -s -X POST "$API_URL/api/test/order/non-retryable/illegal-argument" > /dev/null
sleep 0.5

# Message 2: Missing customer ID (non-retryable - ValidationException)
echo "Sending: Order with null customer ID (non-retryable)"
curl -s -X POST "$API_URL/api/test/order/non-retryable/validation" > /dev/null
sleep 0.5

# Message 3: Missing payment method (retryable - will retry 3 times then DLQ)
echo "Sending: Order with null payment method (retryable, will exhaust retries)"
curl -s -X POST "$API_URL/api/test/order/retryable/payment" > /dev/null
sleep 0.5

# Message 4: Status = FAIL (retryable - will retry 3 times then DLQ)
echo "Sending: Order with FAIL status (retryable, will exhaust retries)"
curl -s -X POST "$API_URL/api/test/order/retryable/runtime" > /dev/null
sleep 0.5

echo ""
echo "Waiting for retries to complete and messages to reach DLQ..."
echo "(This takes about 3-4 seconds for retryable messages)"
sleep 4

echo ""
echo "Checking DLQ status..."
MESSAGE_COUNT=$(curl -s "$API_URL/dlq?topic=$DLQ_TOPIC" | jq -r '.summary.totalEvents')

echo ""
echo "========================================="
echo "Results:"
echo "========================================="
echo "Messages in DLQ: $MESSAGE_COUNT"
echo ""

if [ "$MESSAGE_COUNT" -gt 0 ]; then
    echo "✅ Success! $MESSAGE_COUNT messages added to DLQ"
    echo ""
    echo "Message breakdown:"
    echo "  - 2 non-retryable (went directly to DLQ)"
    echo "  - 2 retryable (exhausted 3 retries, then DLQ)"
    echo ""
else
    echo "❌ No messages in DLQ"
    echo "   Check application logs for issues."
fi

echo ""

