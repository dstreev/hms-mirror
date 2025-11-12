#!/bin/bash

echo "==================================="
echo "HMS-Mirror SPA Configuration Check"
echo "==================================="
echo ""

echo "1. Checking if React build exists..."
if [ -d "src/main/resources/static/react" ]; then
    echo "✓ React build directory exists"
    if [ -f "src/main/resources/static/react/index.html" ]; then
        echo "✓ index.html found"
    else
        echo "✗ index.html NOT found - rebuild needed"
    fi
else
    echo "✗ React build directory NOT found - rebuild needed"
fi
echo ""

echo "2. Checking Java controller files..."
if [ -f "src/main/java/com/cloudera/utils/hms/mirror/web/controller/SpaFallbackController.java" ]; then
    echo "✓ SpaFallbackController.java exists"
else
    echo "✗ SpaFallbackController.java NOT found"
fi
echo ""

echo "3. Checking application.yaml context-path..."
grep -A1 "servlet:" src/main/resources/application.yaml | grep "context-path"
echo ""

echo "4. Checking vite.config.ts base path..."
grep "base:" src/main/frontend/vite.config.ts
echo ""

echo "5. Checking if Spring Boot is running..."
if lsof -i:8090 > /dev/null 2>&1; then
    echo "✓ Port 8090 is in use (Spring Boot likely running)"
    echo "  You need to RESTART Spring Boot for changes to take effect"
else
    echo "✗ Port 8090 is not in use (Spring Boot not running)"
fi
echo ""

echo "==================================="
echo "Action Required:"
echo "==================================="
echo "1. Rebuild: mvn clean package -DskipTests"
echo "2. Restart: mvn spring-boot:run (or restart your running instance)"
echo "3. Test: http://localhost:8090/hms-mirror/jobs/build"
echo ""
