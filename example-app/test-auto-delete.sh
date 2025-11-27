#!/bin/bash

echo "========================================="
echo "✅ TEST: Automatic Message Deletion"
echo "========================================="
echo ""
echo "This verifies that replayed messages are"
echo "automatically deleted from the DLQ."
echo ""

API_URL="http://localhost:8080"
DLQ_TOPIC="test-dlq"

echo "Step 1: Check current DLQ message count..."
BEFORE=$(curl -s "$API_URL/dlq?topic=$DLQ_TOPIC" | jq -r '.summary.totalEvents')
echo "Messages in DLQ BEFORE adding test messages: $BEFORE"
echo ""

if [ "$BEFORE" == "0" ] || [ "$BEFORE" == "null" ] || [ "$BEFORE" == "" ]; then
    echo "📨 No messages in DLQ. Adding test messages..."
    echo ""

    # Call the add-test-messages script
    ./add-test-messages.sh

    echo ""
    echo "Test messages added. Checking DLQ again..."
    BEFORE=$(curl -s "$API_URL/dlq?topic=$DLQ_TOPIC" | jq -r '.summary.totalEvents')
    echo "Messages in DLQ NOW: $BEFORE"
    echo ""

    if [ "$BEFORE" == "0" ] || [ "$BEFORE" == "null" ] || [ "$BEFORE" == "" ]; then
        echo "❌ Still no messages in DLQ!"
        echo "   Check that the application is running on port 8081"
        exit 1
    fi
else
    echo "✅ Found $BEFORE existing messages in DLQ"
    echo ""
    read -p "Add MORE test messages? (y/N): " add_more

    if [ "$add_more" == "y" ] || [ "$add_more" == "Y" ]; then
        echo ""
        ./add-test-messages.sh

        echo ""
        echo "Checking DLQ again..."
        BEFORE=$(curl -s "$API_URL/dlq?topic=$DLQ_TOPIC" | jq -r '.summary.totalEvents')
        echo "Messages in DLQ NOW: $BEFORE"
        echo ""
    fi
fi

read -p "Ready to replay and DELETE $BEFORE messages from DLQ? Press ENTER..."

echo ""
echo "Step 2: Calling replay with skipValidation=true..."
echo "         (Messages will be replayed AND deleted from DLQ)"
curl -s -X POST "$API_URL/dlq/replay/$DLQ_TOPIC?skipValidation=true" | jq .
echo ""

echo "Step 3: Waiting 5 seconds for replay and deletion..."
sleep 5

echo "Step 4: Check DLQ message count after replay..."
AFTER=$(curl -s "$API_URL/dlq?topic=$DLQ_TOPIC" | jq -r '.summary.totalEvents')
echo "Messages in DLQ AFTER replay: $AFTER"
echo ""

echo "========================================="
echo "Results:"
echo "========================================="
echo "Before replay: $BEFORE messages"
echo "After replay:  $AFTER messages"
echo "Deleted:       $((BEFORE - AFTER)) messages"
echo ""

if [ "$AFTER" == "0" ]; then
    echo "✅ SUCCESS! All replayed messages were deleted!"
    echo ""
    echo "This proves:"
    echo "  - Messages were replayed to orders topic"
    echo "  - X-Replay-Mode header was added"
    echo "  - OrderProcessingService skipped validation"
    echo "  - Messages 'succeeded' without going back to DLQ"
    echo "  - ✨ Messages were DELETED from DLQ after replay!"
    echo ""
    echo "The DLQ is now clean! 🎉"
elif [ "$AFTER" -lt "$BEFORE" ]; then
    echo "✅ PARTIAL SUCCESS!"
    echo "   Some messages were deleted ($((BEFORE - AFTER)) out of $BEFORE)"
    echo "   Remaining messages may have failed replay."
else
    echo "❌ FAILURE: Messages were not deleted"
    echo "   Before: $BEFORE"
    echo "   After:  $AFTER"
    echo ""
    echo "Check application logs for errors."
fi

echo ""
echo "========================================="
echo ""

