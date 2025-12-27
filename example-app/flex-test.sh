#!/bin/bash

# flex-test.sh - Verifies the Flexible Deserialization capabilities of the library

BASE_URL="http://localhost:8080/test/flex"

echo "=========================================="
echo "    FLEXIBLE DESERIALIZATION TEST SUITE    "
echo "=========================================="
echo "This script triggers events with different serialization formats"
echo "to verify the 'Zero-Config' and 'Flexible Type' capabilities."
echo ""

# Function to run a test case
run_test() {
    ENDPOINT=$1
    DESCRIPTION=$2
    
    echo "-----------------------------------------------------------------"
    echo "TEST: $DESCRIPTION"
    echo "Endpoint: $ENDPOINT"
    
    # Send request and capture output
    RESPONSE=$(curl -s "$BASE_URL/$ENDPOINT")
    
    echo "Response: $RESPONSE"
    echo "-----------------------------------------------------------------"
    echo ""
    sleep 0.5
}

# 1. Standard Happy Path (Explicit Headers)
# Use Case: Standard Spring Kafka Producer interacting with our Consumer
run_test "order-with-header" "POJO with Explicit '__TypeId__' Header"

# 2. Zero-Config Inference (No Headers)
# Use Case: external producer (Python/Node) or simple Java producer without header config
# The library should infer 'OrderEvent' from the @KafkaListener signature
run_test "order-no-header" "POJO WITHOUT Headers (Zero-Config Inference)"

# 3. Primitives - Boolean
# Use Case: Simple flags or toggle events
run_test "bool-no-header" "Boolean Primitive (Zero-Config)"

# 4. Primitives - Double
# Use Case: Sensor readings or financial amounts sent as raw numbers
run_test "double-no-header" "Double Primitive (Zero-Config)"

# 5. Raw String
# Use Case: Simple text messages or legacy text integration
run_test "string-no-header" "Raw String (Zero-Config)"

# 6. Advanced: Transparent Event Wrapper
# Use Case: Validating that internal 'EventWrapper' is unwrapped automatically
# and the user only sees the inner event payload.
run_test "wrapped-event" "Internal EventWrapper (Transparent Unwrapping)"

echo "=========================================="
echo "    TEST SUITE COMPLETE    "
echo "=========================================="
echo "Check the application logs (mvn spring-boot:run tab) to verify:"
echo "1. Consumers received the correct types (OrderEvent, Boolean, etc.)"
echo "2. No deserialization errors occurred"
echo "3. 'EventWrapper' logic was hidden from the application code"
