#!/bin/bash

# run multi-partition tests separately to avoid resource contention
# this script runs each partition-related test in isolation

echo "running multi-partition tests..."
echo "================================"

# run multi-partition integration test
echo ""
echo "running MultiPartitionIntegrationTest..."
mvn test -Dtest=MultiPartitionIntegrationTest -q

if [ $? -eq 0 ]; then
    echo "MultiPartitionIntegrationTest: PASSED"
else
    echo "MultiPartitionIntegrationTest: FAILED"
    exit 1
fi

echo ""
echo "================================"
echo "all multi-partition tests completed"
