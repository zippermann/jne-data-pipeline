# JNE Audit Trail Installation Script
# Run this in PowerShell to set up the audit trail system

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "JNE AUDIT TRAIL INSTALLATION" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Check if in correct directory
$currentPath = Get-Location
Write-Host "Current directory: $currentPath" -ForegroundColor Yellow
Write-Host ""

# Step 1: Create audit folder structure
Write-Host "[Step 1/5] Creating audit folder structure..." -ForegroundColor Green
New-Item -ItemType Directory -Force -Path "scripts\audit" | Out-Null
Write-Host "  > Created scripts\audit\" -ForegroundColor Green
Write-Host ""

# Step 2: Check PostgreSQL container
Write-Host "[Step 2/5] Checking PostgreSQL container..." -ForegroundColor Green
$postgresRunning = docker ps --filter "name=jne-postgres" --filter "status=running" --format "{{.Names}}"
if ($postgresRunning -eq "jne-postgres") {
    Write-Host "  > PostgreSQL container is running" -ForegroundColor Green
} else {
    Write-Host "  X PostgreSQL container is not running!" -ForegroundColor Red
    Write-Host "  Please run: docker-compose up -d" -ForegroundColor Yellow
    exit 1
}
Write-Host ""

# Step 3: Create audit schema in database
Write-Host "[Step 3/5] Creating audit schema in PostgreSQL..." -ForegroundColor Green
$sqlFile = "scripts\init-db\02-create-audit-schema.sql"

if (Test-Path $sqlFile) {
    # Use Get-Content and pipe to docker exec instead of < operator
    Get-Content $sqlFile | docker exec -i jne-postgres psql -U jne_user -d jne_dashboard
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  > Audit schema created successfully" -ForegroundColor Green
    } else {
        Write-Host "  X Failed to create audit schema" -ForegroundColor Red
        Write-Host "  Error code: $LASTEXITCODE" -ForegroundColor Yellow
        exit 1
    }
} else {
    Write-Host "  X SQL file not found: $sqlFile" -ForegroundColor Red
    Write-Host "  Please ensure 02-create-audit-schema.sql is in scripts\init-db\" -ForegroundColor Yellow
    exit 1
}
Write-Host ""

# Step 4: Verify audit tables
Write-Host "[Step 4/5] Verifying audit tables..." -ForegroundColor Green
$verifyQuery = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'audit';"
$tableCount = docker exec jne-postgres psql -U jne_user -d jne_dashboard -t -c $verifyQuery
$tableCount = $tableCount.Trim()

if ([int]$tableCount -ge 5) {
    Write-Host "  > Found $tableCount audit tables" -ForegroundColor Green
} else {
    Write-Host "  ! Only found $tableCount audit tables (expected 5+)" -ForegroundColor Yellow
}
Write-Host ""

# Step 5: Install Python dependencies
Write-Host "[Step 5/5] Checking Python dependencies..." -ForegroundColor Green
$dependencies = @("sqlalchemy", "psycopg2-binary", "pandas")
foreach ($dep in $dependencies) {
    $installed = pip show $dep 2>$null
    if ($installed) {
        Write-Host "  > $dep is installed" -ForegroundColor Green
    } else {
        Write-Host "  ! $dep is NOT installed" -ForegroundColor Yellow
        Write-Host "    Installing $dep..." -ForegroundColor Yellow
        pip install $dep --quiet
    }
}
Write-Host ""

# Summary
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "INSTALLATION COMPLETE!" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "1. Copy audit_logger.py to scripts\audit\" -ForegroundColor White
Write-Host "2. Replace scripts\etl\load_data.py with load_data_with_audit.py" -ForegroundColor White
Write-Host "3. Replace scripts\kafka\kafka_producer.py with kafka_producer_with_audit.py" -ForegroundColor White
Write-Host "4. Copy jne_audit_monitoring_dag.py to airflow\dags\" -ForegroundColor White
Write-Host "5. Test with: python scripts\etl\load_data.py" -ForegroundColor White
Write-Host ""
Write-Host "Read AUDIT_TRAIL_README.md for detailed usage instructions." -ForegroundColor Yellow
Write-Host ""

# Test database connection
Write-Host "Testing database connection..." -ForegroundColor Green
$testQuery = "SELECT 'Audit system ready!' as status;"
$result = docker exec jne-postgres psql -U jne_user -d jne_dashboard -t -c $testQuery
Write-Host "  $result" -ForegroundColor Green
Write-Host ""
