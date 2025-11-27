#!/bin/bash

echo "========================================="
echo "EMERGENCY: Stop Infinite Replay Loop"
echo "========================================="
echo ""
echo "This script will:"
echo "1. Stop any running replay operations (via curl)"
echo "2. Clean up the DLQ topic"
echo "3. Clean up the orders topic"
echo "4. Reset consumer groups"
echo ""
read -p "Press ENTER to continue or Ctrl+C to cancel..."

KAFKA_BROKER="localhost:9092"

echo ""
echo "Step 1: Attempting to stop the application (if accessible)..."
echo "You may need to manually restart your Spring Boot application."
echo ""

echo "Step 2: Cleaning up topics..."

# Delete and recreate test-dlq
echo "  - Deleting test-dlq topic..."
kafka-topics --bootstrap-server $KAFKA_BROKER --delete --topic test-dlq 2>/dev/null
sleep 1
echo "  - Recreating test-dlq topic..."
kafka-topics --bootstrap-server $KAFKA_BROKER --create --topic test-dlq --partitions 1 --replication-factor 1

# Delete and recreate orders topic
echo "  - Deleting orders topic..."
kafka-topics --bootstrap-server $KAFKA_BROKER --delete --topic orders 2>/dev/null
sleep 1
echo "  - Recreating orders topic..."
kafka-topics --bootstrap-server $KAFKA_BROKER --create --topic orders --partitions 1 --replication-factor 1

echo ""
echo "Step 3: Resetting consumer groups..."
kafka-consumer-groups --bootstrap-server $KAFKA_BROKER --delete --group dlq-replay-test-dlq 2>/dev/null
kafka-consumer-groups --bootstrap-server $KAFKA_BROKER --delete --group orders-consumer-group 2>/dev/null

echo ""
echo "========================================="
echo "Cleanup Complete!"
echo "========================================="
echo ""
echo "⚠️  IMPORTANT: You MUST restart your Spring Boot application now!"
echo ""
echo "After restarting, DO NOT call the replay endpoint until you:"
echo "1. Fix the test data (use valid OrderEvents)"
echo "2. OR manually fix the messages in the DLQ"
echo ""

