#!/bin/bash

echo "=========================================="
echo "HMS-Mirror 404 Diagnostic"  
echo "=========================================="
echo ""

echo "1. Checking if application is running on port 8090..."
if lsof -i:8090 > /dev/null 2>&1; then
    echo "✓ Port 8090 is in use"
    lsof -i:8090 | grep LISTEN
else
    echo "✗ Port 8090 is NOT in use - Application not running!"
    echo "  Start with: mvn spring-boot:run -Dspring-boot.run.arguments='--server.port=8090 --hms-mirror.rocksdb.enabled=true'"
    exit 1
fi
echo ""

echo "2. Testing if API root is accessible..."
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8090/hms-mirror/api/v1/ 2>/dev/null || echo "000")
if [ "$HTTP_CODE" == "404" ] || [ "$HTTP_CODE" == "200" ]; then
    echo "✓ API root responds (HTTP $HTTP_CODE)"
else
    echo "✗ API root not accessible (HTTP $HTTP_CODE)"
fi
echo ""

echo "3. Testing /api/v1/jobs endpoint..."
JOBS_CODE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8090/hms-mirror/api/v1/jobs 2>/dev/null || echo "000")
if [ "$JOBS_CODE" == "404" ]; then
    echo "✗ Jobs API returns 404 - JobsController NOT LOADED"
    echo ""
    echo "  CAUSE: JobsController requires hms-mirror.rocksdb.enabled=true"
    echo "  Your application was likely started WITHOUT this property!"
    echo ""
    echo "  HOW TO FIX:"
    echo "  1. Stop current application (Ctrl+C or pkill)"
    echo "  2. Restart with RocksDB enabled:"
    echo "     mvn spring-boot:run -Dspring-boot.run.arguments='--server.port=8090 --hms-mirror.rocksdb.enabled=true'"
    echo ""
elif [ "$JOBS_CODE" == "200" ]; then
    echo "✓ Jobs API accessible (HTTP $JOBS_CODE)"
else
    echo "? Jobs API returned HTTP $JOBS_CODE"
fi
echo ""

echo "4. Testing validate endpoint..."
VAL_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
    -H "Content-Type: application/json" \
    -d '{}' \
    http://localhost:8090/hms-mirror/api/v1/jobs/test-job/validate 2>/dev/null || echo "000")

if [ "$VAL_CODE" == "404" ]; then
    echo "✗ Validate endpoint returns 404"
elif [ "$VAL_CODE" == "500" ] || [ "$VAL_CODE" == "400" ]; then
    echo "✓ Validate endpoint exists (HTTP $VAL_CODE - expected for invalid job)"  
else
    echo "? Validate endpoint returned HTTP $VAL_CODE"
fi
echo ""

echo "5. Checking application logs for RocksDB..."
echo "  Looking for RocksDB initialization in logs..."
if [ -f "logs/hms-mirror.log" ]; then
    if grep -q "rocksdb" logs/hms-mirror.log; then
        echo "✓ RocksDB mentions found in logs"
    else
        echo "✗ No RocksDB mentions in logs"
    fi
else
    echo "  (log file not found at default location)"
fi
echo ""

echo "=========================================="
echo "SUMMARY"
echo "=========================================="
if [ "$JOBS_CODE" == "404" ]; then
    echo "PROBLEM: JobsController is NOT loaded"
    echo "REASON:  Application started without --hms-mirror.rocksdb.enabled=true"
    echo "FIX:     Restart application with correct parameters"
    echo ""
    echo "Run this command:"
    echo "  mvn spring-boot:run -Dspring-boot.run.arguments='--server.port=8090 --hms-mirror.rocksdb.enabled=true'"
else
    echo "API endpoints appear to be accessible."
    echo "If still getting 404 in browser:"
    echo "  1. Clear browser cache (Ctrl+Shift+R)"
    echo "  2. Check browser DevTools Network tab for actual URL being called"
    echo "  3. Verify jobKey value is not empty/undefined"
fi
echo ""
