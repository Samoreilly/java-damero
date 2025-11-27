#!/bin/bash

# Check the current state of the DLQ topic

KAFKA_BROKER="localhost:9092"
DLQ_TOPIC="test-dlq"

echo "========================================="
echo "DLQ Topic Status"
echo "========================================="
echo ""

echo "Topic: $DLQ_TOPIC"
echo ""

# Get topic details
echo "Topic Information:"
kafka-topics --bootstrap-server $KAFKA_BROKER --describe --topic $DLQ_TOPIC 2>/dev/null

echo ""
echo "----------------------------------------"
echo "Consumer Group Offsets:"
echo "----------------------------------------"

# Get consumer group offsets
kafka-consumer-groups --bootstrap-server $KAFKA_BROKER \
  --describe \
  --group dlq-replay-$DLQ_TOPIC 2>/dev/null

echo ""
echo "----------------------------------------"
echo "Message Count Estimate:"
echo "----------------------------------------"

# Get the earliest and latest offsets to estimate message count
kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list $KAFKA_BROKER \
  --topic $DLQ_TOPIC \
  --time -2 2>/dev/null | \
  while read line; do
    partition=$(echo $line | cut -d: -f2)
    earliest=$(echo $line | cut -d: -f3)

    latest=$(kafka-run-class kafka.tools.GetOffsetShell \
      --broker-list $KAFKA_BROKER \
      --topic $DLQ_TOPIC \
      --time -1 2>/dev/null | \
      grep ":$partition:" | \
      cut -d: -f3)

    count=$((latest - earliest))
    echo "Partition $partition: $count messages (offsets $earliest to $latest)"
  done

echo ""
echo "========================================="

