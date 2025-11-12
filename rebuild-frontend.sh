#!/bin/bash

echo "🔧 Rebuilding HMS-Mirror Frontend..."

# Navigate to frontend directory
cd src/main/frontend

# Install dependencies (if needed)
echo "📦 Installing dependencies..."
npm install

# Build the React app
echo "🏗️  Building React application..."
npm run build

# Go back to project root
cd ../../..

# Copy built files to Spring Boot resources
echo "📋 Copying build artifacts to Spring Boot static resources..."
rm -rf src/main/resources/static/react/*
cp -r src/main/frontend/build/* src/main/resources/static/react/

echo "✅ Frontend rebuild complete!"
echo ""
echo "Now restart your Spring Boot application:"
echo "  mvn spring-boot:run -Dspring-boot.run.arguments=\"--server.port=8080 --hms-mirror.rocksdb.enabled=true\""

