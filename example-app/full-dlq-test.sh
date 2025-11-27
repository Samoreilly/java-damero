#!/bin/bash

# Comprehensive DLQ test: Clean -> Add Messages -> Replay -> Verify Deletion

echo "========================================="
echo "🧪 COMPREHENSIVE DLQ TEST"
echo "========================================="
echo ""
echo "This script will:"
echo "  1. Clean the DLQ (remove all messages)"
echo "  2. Add fresh test messages"
echo "  3. Replay messages with auto-delete"
echo "  4. Verify messages were deleted"
echo ""

API_URL="http://localhost:8080"
DLQ_TOPIC="test-dlq"

# Check if application is running
echo "Checking if application is running..."
if ! curl -s "$API_URL/dlq?topic=$DLQ_TOPIC" > /dev/null 2>&1; then
    echo "❌ Application is not running on port 8080"
    echo "   Please start the application first:"
    echo "   cd /home/sam-o-reilly/Downloads/java-damero/example-app"
    echo "   ./run-and-test.sh"
    exit 1
fi
echo "✅ Application is running"
echo ""

# Step 1: Clean DLQ
echo "========================================="
echo "Step 1: Cleaning DLQ"
echo "========================================="
INITIAL_COUNT=$(curl -s "$API_URL/dlq?topic=$DLQ_TOPIC" | jq -r '.summary.totalEvents')
echo "Current DLQ message count: $INITIAL_COUNT"

if [ "$INITIAL_COUNT" != "0" ] && [ "$INITIAL_COUNT" != "null" ] && [ "$INITIAL_COUNT" != "" ]; then
    read -p "Clean DLQ? This will delete $INITIAL_COUNT messages (yes/no): " confirm

    if [ "$confirm" == "yes" ]; then
        echo "Cleaning DLQ topic..."
        kafka-topics --bootstrap-server localhost:9092 --delete --topic $DLQ_TOPIC 2>/dev/null
        sleep 2
        kafka-topics --bootstrap-server localhost:9092 \
          --create \
          --topic $DLQ_TOPIC \
          --partitions 1 \
          --replication-factor 1 2>/dev/null
        sleep 1
        echo "✅ DLQ cleaned"
    else
        echo "Keeping existing messages"
    fi
else
    echo "✅ DLQ is already empty"
fi
echo ""

# Step 2: Add test messages
echo "========================================="
echo "Step 2: Adding Test Messages"
echo "========================================="
echo ""
./add-test-messages.sh

AFTER_ADD=$(curl -s "$API_URL/dlq?topic=$DLQ_TOPIC" | jq -r '.summary.totalEvents')
echo ""

if [ "$AFTER_ADD" == "0" ] || [ "$AFTER_ADD" == "null" ] || [ "$AFTER_ADD" == "" ]; then
    echo "❌ Failed to add messages to DLQ"
    exit 1
fi

echo "✅ $AFTER_ADD messages now in DLQ"
echo ""
read -p "Press ENTER to continue to replay test..."

# Step 3: Replay with auto-delete
echo ""
echo "========================================="
echo "Step 3: Replaying Messages (with auto-delete)"
echo "========================================="
echo ""
echo "Calling replay endpoint with skipValidation=true..."
curl -s -X POST "$API_URL/dlq/replay/$DLQ_TOPIC?skipValidation=true" | jq .
echo ""

echo "Waiting 5 seconds for replay and deletion..."
sleep 5

# Step 4: Verify deletion
echo ""
echo "========================================="
echo "Step 4: Verification"
echo "========================================="
AFTER_REPLAY=$(curl -s "$API_URL/dlq?topic=$DLQ_TOPIC" | jq -r '.summary.totalEvents')

echo ""
echo "📊 Results:"
echo "   Messages before replay: $AFTER_ADD"
echo "   Messages after replay:  $AFTER_REPLAY"
echo "   Messages deleted:       $((AFTER_ADD - AFTER_REPLAY))"
echo ""

if [ "$AFTER_REPLAY" == "0" ]; then
    echo "✅ ✅ ✅ SUCCESS! ✅ ✅ ✅"
    echo ""
    echo "All messages were successfully:"
    echo "  ✓ Replayed to the orders topic"
    echo "  ✓ Processed with validation skipped"
    echo "  ✓ Deleted from the DLQ automatically"
    echo ""
    echo "The DLQ is now clean! 🎉"
    echo ""
    echo "This proves the auto-delete feature is working correctly!"
elif [ "$AFTER_REPLAY" -lt "$AFTER_ADD" ]; then
    echo "⚠️  PARTIAL SUCCESS"
    echo ""
    echo "$((AFTER_ADD - AFTER_REPLAY)) messages were deleted, but $AFTER_REPLAY remain."
    echo "This may indicate some messages failed during replay."
    echo ""
    echo "Check application logs for details:"
    echo "  tail -f app.log"
else
    echo "❌ FAILURE"
    echo ""
    echo "No messages were deleted from the DLQ!"
    echo ""
    echo "Possible causes:"
    echo "  - Auto-delete feature not enabled"
    echo "  - Application using old library version"
    echo "  - Replay failed"
    echo ""
    echo "Check application logs:"
    echo "  tail -f app.log"
fi

echo ""
echo "========================================="
echo "Test Complete"
echo "========================================="
echo ""

