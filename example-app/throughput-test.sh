#!/bin/bash

# Kafka Damero Throughput Test
# Sends messages directly to Kafka topic (bypasses REST API)
# Tests batch processing performance

# Add Kafka to PATH
export PATH="/home/sam-o-reilly/kafka/kafka_2.13-3.9.1/bin:$PATH"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Configuration
TOPIC="orders"
NUM_MESSAGES=${1:-50000}
BOOTSTRAP_SERVER="localhost:9094"
CONSUMER_GROUP="order-processor"

echo -e "${BLUE}==================================================================="
echo "           Kafka Damero Throughput Test"
echo "===================================================================${NC}"
echo ""
echo "Configuration:"
echo "  Topic:          $TOPIC"
echo "  Messages:       $NUM_MESSAGES"
echo "  Bootstrap:      $BOOTSTRAP_SERVER"
echo "  Consumer Group: $CONSUMER_GROUP"
echo ""

# Pre-flight checks
echo -e "${YELLOW}Running pre-flight checks...${NC}"

# Check Kafka tools
if ! command -v kafka-topics.sh &> /dev/null; then
    echo -e "${RED}ERROR: Kafka tools not found in PATH${NC}"
    echo "Please set: export PATH=\"/path/to/kafka/bin:\$PATH\""
    exit 1
fi
echo -e "  ${GREEN}✓${NC} Kafka tools found"

# Check Kafka connection
if ! kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVER --list > /dev/null 2>&1; then
    echo -e "${RED}ERROR: Cannot connect to Kafka at $BOOTSTRAP_SERVER${NC}"
    echo "Please ensure Kafka is running"
    exit 1
fi
echo -e "  ${GREEN}✓${NC} Kafka is running"

# Reset topic for clean test
echo ""
echo -e "${YELLOW}Resetting topic for clean test...${NC}"

# Delete topic (ignore errors if it doesn't exist)
kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVER --delete --topic $TOPIC 2>/dev/null || true
sleep 3

# Create topic
kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVER \
    --create \
    --topic $TOPIC \
    --partitions 1 \
    --replication-factor 1 2>/dev/null || true

# Verify topic exists
if kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVER --list 2>/dev/null | grep -q "^${TOPIC}$"; then
    echo -e "  ${GREEN}✓${NC} Topic '$TOPIC' ready"
else
    echo -e "${RED}ERROR: Failed to create topic${NC}"
    exit 1
fi

# Reset consumer group offsets
echo -e "${YELLOW}Resetting consumer group offsets...${NC}"
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP_SERVER \
    --group $CONSUMER_GROUP \
    --topic $TOPIC \
    --reset-offsets \
    --to-earliest \
    --execute 2>/dev/null || true
echo -e "  ${GREEN}✓${NC} Consumer group offsets reset"

echo ""

# Generate messages
echo -e "${YELLOW}Generating $NUM_MESSAGES JSON messages...${NC}"
GEN_START=$(date +%s.%N)

TEMP_FILE=$(mktemp)
trap "rm -f $TEMP_FILE" EXIT

{
    for i in $(seq 1 $NUM_MESSAGES); do
        echo "{\"orderId\":\"perf-$i\",\"customerId\":\"customer-$((i % 1000))\",\"amount\":$((RANDOM % 1000 + 100)).99,\"paymentMethod\":\"CREDIT_CARD\",\"status\":\"PENDING\"}"
    done
} > $TEMP_FILE

GEN_END=$(date +%s.%N)
GEN_TIME=$(echo "$GEN_END - $GEN_START" | bc)
FILE_SIZE=$(du -h $TEMP_FILE | cut -f1)
echo -e "  ${GREEN}✓${NC} Generated in ${GEN_TIME}s (file size: $FILE_SIZE)"
echo ""

# Send messages to Kafka
echo -e "${GREEN}==================================================================="
echo "                  SENDING TO KAFKA"
echo "===================================================================${NC}"
echo "Start time: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""

SEND_START=$(date +%s.%N)

cat $TEMP_FILE | kafka-console-producer.sh \
    --bootstrap-server $BOOTSTRAP_SERVER \
    --topic $TOPIC \
    --property "parse.key=false" \
    --compression-codec snappy \
    --request-required-acks 1 2>/dev/null

SEND_END=$(date +%s.%N)
SEND_DURATION=$(echo "$SEND_END - $SEND_START" | bc)

echo ""
echo -e "${GREEN}✓ All $NUM_MESSAGES messages sent to Kafka${NC}"
echo ""

# Calculate and display producer results
echo -e "${GREEN}==================================================================="
echo "                    PRODUCER RESULTS"
echo "===================================================================${NC}"
echo ""
echo "MESSAGE GENERATION:"
printf "  Time:              %.2fs\n" $GEN_TIME
if [ $(echo "$GEN_TIME > 0" | bc) -eq 1 ]; then
    RATE=$(echo "$NUM_MESSAGES / $GEN_TIME" | bc)
    echo "  Rate:              $RATE messages/sec"
fi
echo ""
echo "KAFKA PRODUCER:"
printf "  Time:              %.2fs\n" $SEND_DURATION
if [ $(echo "$SEND_DURATION > 0" | bc) -eq 1 ]; then
    THROUGHPUT=$(echo "$NUM_MESSAGES / $SEND_DURATION" | bc)
    echo "  Throughput:        $THROUGHPUT messages/sec"
fi
echo ""

# Check current consumer lag
echo -e "${CYAN}==================================================================="
echo "                    CONSUMER STATUS"
echo "===================================================================${NC}"
echo ""
echo -e "${YELLOW}Checking consumer group lag...${NC}"
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP_SERVER \
    --group $CONSUMER_GROUP \
    --describe 2>/dev/null | head -20

echo ""
echo -e "${GREEN}==================================================================="
echo "                    NEXT STEPS"
echo "===================================================================${NC}"
echo ""
echo "Messages are now in Kafka topic '$TOPIC'."
echo ""
echo -e "${CYAN}To process messages, start the consumer app:${NC}"
echo "  cd example-app && mvn spring-boot:run"
echo ""
echo -e "${CYAN}To monitor batch processing:${NC}"
echo "  tail -f app.log | grep -E 'Batch complete|Processing batch'"
echo ""
echo -e "${CYAN}To check consumer lag in real-time:${NC}"
echo "  watch -n 1 'kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group order-processor --describe 2>/dev/null'"
echo ""
echo -e "${GREEN}Test setup complete!${NC}"
echo "==================================================================="
