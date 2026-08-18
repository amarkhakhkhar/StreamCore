#!/bin/bash
# Container durability test script
# Proves records survive container restarts

set -e

echo "=========================================="
echo "StreamCore Container Durability Test"
echo "=========================================="

CONTAINER_NAME="streamcore-broker"
IMAGE_NAME="streamcore-broker"
VOLUME_NAME="streamcore-data"

# Build the image
echo -e "\n[1/6] Building Docker image..."
docker build -t $IMAGE_NAME -f docker/Dockerfile .

# Clean up any existing container
echo -e "\n[2/6] Cleaning up existing containers..."
docker compose down -v 2>/dev/null || true

# Start the container
echo -e "\n[3/6] Starting container..."
docker compose up -d

# Wait for container to be healthy
echo -e "\n[4/6] Waiting for container to be healthy..."
for i in {1..30}; do
    if docker compose exec -T broker python -c "import httpx; httpx.get('http://localhost:8000/health').raise_for_status()" 2>/dev/null; then
        echo "Container is healthy!"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "Container failed to become healthy"
        docker compose logs
        exit 1
    fi
    sleep 1
done

# Write test records
echo -e "\n[5/6] Writing test records..."
RECORDS_TO_WRITE=50

for i in $(seq 1 $RECORDS_TO_WRITE); do
    RESPONSE=$(docker compose exec -T broker curl -s -X POST \
        -H "Content-Type: application/json" \
        -d "{\"data\": \"durability-test-record-$i\"}" \
        http://localhost:8000/append)
    echo "  Appended record $i: $RESPONSE"
done

# Get log info before restart
echo -e "\n[6/6] Getting log info before restart..."
BEFORE_INFO=$(docker compose exec -T broker curl -s http://localhost:8000/info)
echo "Before restart: $BEFORE_INFO"

# Record the offsets we wrote
FIRST_RECORD="durability-test-record-1"
LAST_RECORD="durability-test-record-$RECORDS_TO_WRITE"

# Stop the container (simulate crash/shutdown)
echo -e "\n[7/6] Stopping container (simulating restart)..."
docker compose stop
sleep 2

# Start the container again
echo -e "\n[8/6] Restarting container..."
docker compose up -d

# Wait for container to be healthy again
echo -e "\n[9/6] Waiting for container to be healthy after restart..."
for i in {1..30}; do
    if docker compose exec -T broker curl -s http://localhost:8000/health 2>/dev/null | grep -q "healthy"; then
        echo "Container is healthy after restart!"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "Container failed to become healthy after restart"
        docker compose logs
        exit 1
    fi
    sleep 1
done

# Get log info after restart
echo -e "\n[10/6] Getting log info after restart..."
AFTER_INFO=$(docker compose exec -T broker curl -s http://localhost:8000/info)
echo "After restart: $AFTER_INFO"

# Verify records are still readable
echo -e "\n[11/6] Verifying records survived restart..."

# Check first record
FIRST_RESPONSE=$(docker compose exec -T broker curl -s http://localhost:8000/read/0)
echo "First record: $FIRST_RESPONSE"

# Check our test records are in order
echo -e "\n[12/6] Verifying exact ordering of test records..."

ERRORS=0
for i in $(seq 1 $RECORDS_TO_WRITE); do
    EXPECTED="durability-test-record-$i"
    RESPONSE=$(docker compose exec -T broker curl -s "http://localhost:8000/read?start_offset=$((i-1))&max_records=1")

    if echo "$RESPONSE" | grep -q "$EXPECTED"; then
        echo "  ✓ Record $i verified: $EXPECTED"
    else
        echo "  ✗ Record $i MISMATCH: expected '$EXPECTED', got '$RESPONSE'"
        ERRORS=$((ERRORS + 1))
    fi
done

# Final report
echo -e "\n=========================================="
echo "Durability Test Results"
echo "=========================================="
echo "Records written before restart: $RECORDS_TO_WRITE"
echo "Records verified after restart: $((RECORDS_TO_WRITE - ERRORS))"
echo "Errors: $ERRORS"

if [ $ERRORS -eq 0 ]; then
    echo -e "\n✓ SUCCESS: All records survived container restart in exact order!"
else
    echo -e "\n✗ FAILURE: $ERRORS records were lost or out of order"
    exit 1
fi

# Cleanup
echo -e "\nCleaning up..."
docker compose down -v

echo "Done!"
