#!/bin/bash

# Kafka Damero Throughput Test
# Sends messages directly to Kafka topic (bypasses REST API)
# Tests batch processing performance by writing JSON directly to Kafka

set -e

# Add Kafka to PATH
export PATH="/home/sam-o-reilly/kafka/kafka_2.13-3.9.1/bin:$PATH"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
TOPIC="orders"
NUM_MESSAGES=${1:-100000}
BOOTSTRAP_SERVER="localhost:9092"

echo -e "${BLUE}==================================================================="
echo "           Kafka Damero Throughput Test"
echo "===================================================================${NC}"
echo ""
echo "Configuration:"
echo "  Topic:          $TOPIC"
echo "  Messages:       $NUM_MESSAGES"
echo "  Bootstrap:      $BOOTSTRAP_SERVER"
echo ""

# Pre-flight checks
echo -e "${YELLOW}Running pre-flight checks...${NC}"

# Check Kafka tools
if ! command -v kafka-topics.sh &> /dev/null; then
    echo "ERROR: Kafka tools not found in PATH"
    echo "Please set: export PATH=\"/path/to/kafka/bin:\$PATH\""
    exit 1
fi
echo "✓ Kafka tools found"

# Check Kafka connection
if ! kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVER --list > /dev/null 2>&1; then
    echo "ERROR: Cannot connect to Kafka at $BOOTSTRAP_SERVER"
    echo "Please ensure Kafka is running"
    exit 1
fi
echo "✓ Kafka is running"

# Check if topic exists
if kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVER --list 2>/dev/null | grep -q "^${TOPIC}$"; then
    echo "✓ Topic '$TOPIC' exists"
else
    echo "⚠ Topic '$TOPIC' does not exist - creating it..."
    kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVER \
        --create \
        --topic $TOPIC \
        --partitions 1 \
        --replication-factor 1 2>/dev/null
    echo "✓ Topic created"
fi

echo ""

# Generate messages
echo -e "${YELLOW}Generating $NUM_MESSAGES JSON messages...${NC}"
GEN_START=$(date +%s)

TEMP_FILE=$(mktemp)
trap "rm -f $TEMP_FILE" EXIT

{
    for i in $(seq 1 $NUM_MESSAGES); do
        echo "{\"orderId\":\"perf-$i\",\"customerId\":\"customer-$((i % 1000))\",\"amount\":$((RANDOM % 1000 + 100)).99,\"paymentMethod\":\"CREDIT_CARD\",\"status\":\"PENDING\"}"
    done
} > $TEMP_FILE

GEN_END=$(date +%s)
GEN_TIME=$((GEN_END - GEN_START))
FILE_SIZE=$(du -h $TEMP_FILE | cut -f1)
echo "✓ Generated in ${GEN_TIME}s (file size: $FILE_SIZE)"
echo ""

# Send messages to Kafka
echo -e "${GREEN}==================================================================="
echo "                  SENDING TO KAFKA"
echo "===================================================================${NC}"
echo "Start time: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""

SEND_START=$(date +%s)

cat $TEMP_FILE | kafka-console-producer.sh \
    --bootstrap-server $BOOTSTRAP_SERVER \
    --topic $TOPIC \
    --property "parse.key=false" \
    --compression-codec snappy \
    --request-required-acks 1 2>/dev/null

SEND_END=$(date +%s)
SEND_DURATION=$((SEND_END - SEND_START))

echo ""
echo -e "${GREEN}✓ All messages sent to Kafka${NC}"
echo ""

# Calculate and display results
TOTAL_TIME=$((SEND_END - GEN_START))

echo -e "${GREEN}==================================================================="
echo "                    RESULTS"
echo "===================================================================${NC}"
echo ""
echo "MESSAGE GENERATION:"
echo "  Time:              ${GEN_TIME}s"
if [ $GEN_TIME -gt 0 ]; then
    echo "  Rate:              $((NUM_MESSAGES / GEN_TIME)) messages/sec"
fi
echo ""
echo "KAFKA PRODUCER:"
echo "  Time:              ${SEND_DURATION}s"
if [ $SEND_DURATION -gt 0 ]; then
    echo "  Throughput:        $((NUM_MESSAGES / SEND_DURATION)) messages/sec"
fi
echo ""
echo "TOTAL:"
echo "  Total time:        ${TOTAL_TIME}s"
if [ $TOTAL_TIME -gt 0 ]; then
    echo "  Overall rate:      $((NUM_MESSAGES / TOTAL_TIME)) messages/sec"
fi
echo ""
echo -e "${YELLOW}Consumer Processing:${NC}"
echo "  Messages are now in Kafka topic '$TOPIC'"
echo "  If your consumer app is running, it should be processing them"
echo "  in batches now."
echo ""
echo "To monitor batch processing in real-time:"
echo "  tail -f app.log | grep -E 'Batch complete|Processing batch'"
echo ""
echo -e "${GREEN}Test complete!${NC}"
echo "==================================================================="

