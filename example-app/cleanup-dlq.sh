#!/bin/bash

# This script cleans up the DLQ topic that has accumulated duplicate messages

echo "========================================="
echo "DLQ Cleanup Script"
echo "========================================="
echo ""
echo "This will delete the test-dlq topic and recreate it empty."
echo "All existing DLQ messages will be lost!"
echo ""
read -p "Are you sure you want to continue? (yes/no): " confirm

if [ "$confirm" != "yes" ]; then
    echo "Cleanup cancelled."
    exit 0
fi

KAFKA_BROKER="localhost:9092"
DLQ_TOPIC="test-dlq"

echo ""
echo "Step 1: Deleting topic $DLQ_TOPIC..."
kafka-topics --bootstrap-server $KAFKA_BROKER --delete --topic $DLQ_TOPIC 2>/dev/null

# Wait a moment for deletion
sleep 2

echo "Step 2: Recreating topic $DLQ_TOPIC..."
kafka-topics --bootstrap-server $KAFKA_BROKER \
  --create \
  --topic $DLQ_TOPIC \
  --partitions 1 \
  --replication-factor 1

echo ""
echo "Step 3: Deleting replay consumer group (to reset offsets)..."
kafka-consumer-groups --bootstrap-server $KAFKA_BROKER \
  --delete \
  --group dlq-replay-$DLQ_TOPIC 2>/dev/null

echo ""
echo "========================================="
echo "Cleanup Complete!"
echo "========================================="
echo ""
echo "The $DLQ_TOPIC topic is now empty."
echo "You can now test the replay functionality with a clean slate."
echo ""

