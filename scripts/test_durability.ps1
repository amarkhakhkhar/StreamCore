# Container durability test script for Windows PowerShell
# Proves records survive container restarts

param()

$ErrorActionPreference = "Stop"

Write-Host "=========================================="
Write-Host "StreamCore Container Durability Test"
Write-Host "=========================================="

$CONTAINER_NAME = "streamcore-broker"
$IMAGE_NAME = "streamcore-broker"

# Build the image
Write-Host "`n[1/6] Building Docker image..."
docker build -t $IMAGE_NAME -f docker/Dockerfile .

# Clean up any existing container
Write-Host "`n[2/6] Cleaning up existing containers..."
docker compose down -v 2>$null

# Start the container
Write-Host "`n[3/6] Starting container..."
docker compose up -d

# Wait for container to be healthy
Write-Host "`n[4/6] Waiting for container to be healthy..."
$maxAttempts = 30
$attempt = 0
while ($attempt -lt $maxAttempts) {
    try {
        $health = docker compose exec -T broker curl -s http://localhost:8000/health 2>$null
        if ($health -match "healthy") {
            Write-Host "Container is healthy!"
            break
        }
    } catch {}

    $attempt++
    if ($attempt -eq $maxAttempts) {
        Write-Host "Container failed to become healthy"
        docker compose logs
        exit 1
    }
    Start-Sleep -Seconds 1
}

# Write test records
Write-Host "`n[5/6] Writing test records..."
$RECORDS_TO_WRITE = 50

for ($i = 1; $i -le $RECORDS_TO_WRITE; $i++) {
    $body = '{"data": "durability-test-record-' + $i + '"}'
    $response = docker compose exec -T broker curl -s -X POST `
        -H "Content-Type: application/json" `
        -d $body `
        http://localhost:8000/append
    Write-Host "  Appended record $i: $response"
}

# Get log info before restart
Write-Host "`n[6/6] Getting log info before restart..."
$BEFORE_INFO = docker compose exec -T broker curl -s http://localhost:8000/info
Write-Host "Before restart: $BEFORE_INFO"

# Stop the container (simulate crash/shutdown)
Write-Host "`n[7/6] Stopping container (simulating restart)..."
docker compose stop
Start-Sleep -Seconds 2

# Start the container again
Write-Host "`n[8/6] Restarting container..."
docker compose up -d

# Wait for container to be healthy again
Write-Host "`n[9/6] Waiting for container to be healthy after restart..."
$attempt = 0
while ($attempt -lt $maxAttempts) {
    try {
        $health = docker compose exec -T broker curl -s http://localhost:8000/health 2>$null
        if ($health -match "healthy") {
            Write-Host "Container is healthy after restart!"
            break
        }
    } catch {}

    $attempt++
    if ($attempt -eq $maxAttempts) {
        Write-Host "Container failed to become healthy after restart"
        docker compose logs
        exit 1
    }
    Start-Sleep -Seconds 1
}

# Get log info after restart
Write-Host "`n[10/6] Getting log info after restart..."
$AFTER_INFO = docker compose exec -T broker curl -s http://localhost:8000/info
Write-Host "After restart: $AFTER_INFO"

# Verify records are still readable
Write-Host "`n[11/6] Verifying records survived restart..."

# Check first record
$FIRST_RESPONSE = docker compose exec -T broker curl -s http://localhost:8000/read/0
Write-Host "First record: $FIRST_RESPONSE"

# Check our test records are in order
Write-Host "`n[12/6] Verifying exact ordering of test records..."

$ERRORS = 0
for ($i = 1; $i -le $RECORDS_TO_WRITE; $i++) {
    $EXPECTED = "durability-test-record-$i"
    $offset = $i - 1
    $RESPONSE = docker compose exec -T broker curl -s "http://localhost:8000/read?start_offset=$offset&max_records=1"

    if ($RESPONSE -match [regex]::Escape($EXPECTED)) {
        Write-Host "  [OK] Record $i verified: $EXPECTED"
    } else {
        Write-Host "  [FAIL] Record $i MISMATCH: expected '$EXPECTED', got '$RESPONSE'"
        $ERRORS++
    }
}

# Final report
Write-Host "`n=========================================="
Write-Host "Durability Test Results"
Write-Host "=========================================="
Write-Host "Records written before restart: $RECORDS_TO_WRITE"
Write-Host "Records verified after restart: $($RECORDS_TO_WRITE - $ERRORS)"
Write-Host "Errors: $ERRORS"

if ($ERRORS -eq 0) {
    Write-Host "`n[SUCCESS] All records survived container restart in exact order!" -ForegroundColor Green
} else {
    Write-Host "`n[FAILURE] $ERRORS records were lost or out of order" -ForegroundColor Red
    exit 1
}

# Cleanup
Write-Host "`nCleaning up..."
docker compose down -v

Write-Host "Done!"
