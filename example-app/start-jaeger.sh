#!/bin/bash

# Script to start Jaeger for viewing Damero Kafka library traces
# Jaeger is an open-source distributed tracing platform

echo "============================================"
echo "Starting Jaeger for Trace Visualization"
echo "============================================"
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker is not running. Please start Docker first."
    exit 1
fi

# Check if Jaeger is already running
if sudo docker ps | grep -q jaeger; then
    echo "Jaeger is already running"
    echo ""
else
    echo "Starting Jaeger container..."
    sudo docker run -d --name jaeger \
        -e COLLECTOR_OTLP_ENABLED=true \
        -p 16686:16686 \
        -p 4317:4317 \
        -p 4318:4318 \
        jaegertracing/all-in-one:latest

    if [ $? -eq 0 ]; then
        echo "Jaeger started successfully!"
        echo ""
    else
        # If container already exists but is stopped, start it
        if sudo docker ps -a | grep -q jaeger; then
            echo "ℹJaeger container exists but is stopped. Starting..."
            sudo docker start jaeger
            echo "Jaeger restarted!"
            echo ""
        else
            echo "Failed to start Jaeger"
            exit 1
        fi
    fi
fi

echo "============================================"
echo "Jaeger URLs:"
echo "============================================"
echo "📊 UI (view traces):     http://localhost:16686"
echo "📥 OTLP gRPC endpoint:   localhost:4317"
echo "📥 OTLP HTTP endpoint:   localhost:4318"
echo ""
echo "============================================"
echo "How to use:"
echo "============================================"
echo "1. Make sure your example-app has OpenTelemetryConfig.java"
echo "2. Run your Kafka example app: mvn spring-boot:run"
echo "3. Send some messages to Kafka"
echo "4. Open http://localhost:16686 in your browser"
echo "5. Select service 'kafka-example-app' and click 'Find Traces'"
echo ""
echo "To stop Jaeger: docker stop jaeger"
echo "To remove Jaeger: docker rm jaeger"
echo "============================================"

