# Quick Start: Build Commands

## Standard Build
```bash
# Build JAR and distribution
mvn clean package

# Run locally
java -jar target/hms-mirror-4.0.0.0.jar
```

## GraalVM Native Image
```bash
# Prerequisites: Install GraalVM and native-image
sdk install java 17.0.9-graalce
gu install native-image

# Build native executable
mvn clean package -Pnative -DskipTests

# Run native executable
./target/hms-mirror-native

# Build native container
mvn spring-boot:build-image -Pnative
docker run -p 8080:8080 hms-mirror-native:4.0.0.0
```

## Jib Container Build
```bash
# Build OCI image (requires registry or local daemon)
mvn clean package -Pjib

# Build to local Docker daemon
mvn clean package -Pjib-docker

# Run container
docker run -p 8080:8080 hms-mirror:4.0.0.0

# Push to registry
mvn clean package -Pjib -Djib.to.image=myregistry.io/hms-mirror:4.0.0.0

# Multi-architecture build
mvn clean package -Pjib \
  -Djib.to.image=myregistry.io/hms-mirror:4.0.0.0 \
  -Djib.from.platforms=linux/amd64,linux/arm64
```

## Development Build
```bash
# Build without frontend
mvn clean package -DskipTests -Dfrontend-maven-plugin.skip=true

# Run with custom port
mvn spring-boot:run -Dspring-boot.run.arguments="--server.port=8090"

# Run with RocksDB enabled
mvn spring-boot:run -Dspring-boot.run.arguments="--hms-mirror.rocksdb.enabled=true"
```

## Container Run Examples
```bash
# With volume mounts
docker run -p 8080:8080 \
  -v $(pwd)/configs:/opt/hms-mirror/configs \
  -v $(pwd)/data:/opt/hms-mirror/data \
  hms-mirror:4.0.0.0

# With environment variables
docker run -p 8080:8080 \
  -e JAVA_TOOL_OPTIONS="-Xmx16g" \
  -e HMS_MIRROR_ROCKSDB_ENABLED=true \
  hms-mirror:4.0.0.0

# With Docker Compose
docker-compose up
```

## Performance Comparison

| Build Type | Command | Startup | Memory | Size |
|------------|---------|---------|--------|------|
| Standard JAR | `mvn package` | ~15s | ~512MB | ~150MB |
| Native Image | `mvn package -Pnative` | ~0.1s | ~100MB | ~80MB |
| Jib Container | `mvn package -Pjib` | ~15s | ~512MB | ~400MB |

## Troubleshooting

### Native Image Issues
```bash
# Generate reflection config from tests
mvn test -Pnative -Dagent

# Check generated configs
ls target/native/agent-output/main/
```

### Jib Issues
```bash
# Build with debug logging
mvn package -Pjib -X

# Skip authentication
mvn package -Pjib -Djib.to.auth.username= -Djib.to.auth.password=

# Use insecure registry
mvn package -Pjib -Djib.allowInsecureRegistries=true
```

## CI/CD Integration

### GitHub Actions
```yaml
- name: Build with Jib
  run: mvn package -Pjib -Djib.to.image=${{ env.REGISTRY }}/${{ env.IMAGE_NAME }}:${{ github.sha }}
```

### Jenkins
```groovy
stage('Build Container') {
    steps {
        sh 'mvn clean package -Pjib -Djib.to.image=${DOCKER_REGISTRY}/hms-mirror:${BUILD_NUMBER}'
    }
}
```

For detailed documentation, see [BUILD_OPTIONS.md](BUILD_OPTIONS.md)
