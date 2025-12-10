#!/bin/bash

# Test script to verify rate limiting functionality
# Make sure Kafka is running on localhost:9092 and the app is running on localhost:8080

echo "=========================================="
echo "Testing Rate Limiting Functionality"
echo "=========================================="
echo ""
echo "Rate limit configured: 1 message per 1ms"
echo "This means messages should be throttled heavily"
echo ""

BASE_URL="http://localhost:8080/test"

#count=1
#while [ $count -le 5000 ]; do
  echo "1. Testing successful order (should be throttled)..."
  time curl -X POST $BASE_URL/order/success
  echo ""
  echo ""
#  ((count++))
#done


sleep 2

echo "2. Testing non-retryable exception (IllegalArgumentException)..."
time curl -X POST $BASE_URL/order/non-retryable/illegal-argument
echo ""
echo ""

sleep 2

echo "3. Testing non-retryable exception (ValidationException)..."
time curl -X POST $BASE_URL/order/non-retryable/validation
echo ""
echo ""

sleep 2

echo "4. Testing retryable exception (PaymentException)..."
time curl -X POST $BASE_URL/order/retryable/payment
echo ""
echo ""

sleep 2

echo "5. Testing retryable exception (RuntimeException)..."
time curl -X POST $BASE_URL/order/retryable/runtime
echo ""
echo ""

echo "=========================================="
echo "Test Complete"
echo "=========================================="
echo ""
echo "Check application logs for rate limiting messages:"
echo "  - Look for 'Rate limit reached' debug messages"
echo "  - Look for 'sleeping for X ms' messages"
echo "  - With 1 message per 1ms limit, throttling should be very aggressive"
echo ""

#curl -X POST http://localhost:8080/api/test/order/non-retryable/illegal-argument

#curl -X POST http://localhost:8080/api/test/order/non-retryable/illegal-argument