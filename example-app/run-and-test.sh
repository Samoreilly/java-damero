#!/bin/bash

echo "=========================================="
echo "Starting Example Application and Testing"
echo "=========================================="
echo ""

# Kill any existing Java processes
echo "Cleaning up existing processes..."
pkill -9 -f kafka-example-app 2>/dev/null
sleep 2

# Start the application on port 8080
echo "Starting application on port 8080..."
cd "$(dirname "$0")"
java -jar target/kafka-example-app-1.0.0.jar > app.log 2>&1 &
APP_PID=$!

echo "Application started with PID: $APP_PID"
echo "Waiting for application to start..."

# Wait for application to start (max 30 seconds)
for i in {1..30}; do
    if curl -s http://localhost:8080/actuator/health > /dev/null 2>&1 || curl -s http://localhost:8080/ > /dev/null 2>&1; then
        echo "Application is ready!"
        break
    fi
    if ! ps -p $APP_PID > /dev/null; then
        echo "ERROR: Application failed to start"
        echo "Last 50 lines of log:"
        tail -50 app.log
        exit 1
    fi
    sleep 1
    echo -n "."
done
echo ""

# Test the endpoints
echo ""
echo "=========================================="
echo "Testing DLQ API Endpoints"
echo "=========================================="
echo ""

echo "1. Testing GET /dlq endpoint..."
curl -s http://localhost:8080/dlq?topic=test-dlq | jq . || curl -s http://localhost:8080/dlq?topic=test-dlq
echo ""
echo ""

echo "2. Testing POST /dlq/replay/{topic} endpoint..."
curl -v -X POST http://localhost:8080/dlq/replay/test-dlq 2>&1 | grep -E "(HTTP/|< |message|topic|timestamp)"
echo ""
echo ""

echo "3. Testing GET /dlq/raw endpoint..."
curl -s http://localhost:8080/dlq/raw?topic=test-dlq | jq . || curl -s http://localhost:8080/dlq/raw?topic=test-dlq
echo ""
echo ""

echo "=========================================="
echo "Test Complete!"
echo "=========================================="
echo ""
echo "The application is still running on port 8080"
echo "View logs with: tail -f app.log"
echo "Stop the application with: kill $APP_PID"
echo ""

