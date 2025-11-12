# HMS-Mirror Build Options

This document describes the available build options for HMS-Mirror, including GraalVM Native Image compilation and Jib containerization.

## Standard Build

The standard build creates a Spring Boot executable JAR:

```bash
mvn clean package
```

This produces:
- `target/hms-mirror-4.0.0.0.jar` - Main executable JAR
- `target/hms-mirror-4.0.0.0-bin.tar.gz` - Distribution archive with scripts

## GraalVM Native Image Build

GraalVM Native Image compiles Java applications ahead-of-time into native executables, providing:
- **Faster startup time**: ~100x faster than JVM
- **Lower memory footprint**: ~5x less memory usage
- **No JVM required**: Standalone native executable

### Prerequisites

1. **Install GraalVM 17 or later**:
   ```bash
   # macOS with SDKMAN
   sdk install java 17.0.9-graalce
   sdk use java 17.0.9-graalce

   # or download from https://www.graalvm.org/downloads/
   ```

2. **Install Native Image tool**:
   ```bash
   gu install native-image
   ```

### Build Native Executable

```bash
mvn clean package -Pnative -DskipTests
```

This creates:
- `target/hms-mirror-native` - Native executable (no JVM needed)

### Run Native Executable

```bash
./target/hms-mirror-native
```

Or with arguments:
```bash
./target/hms-mirror-native --server.port=8090 --hms-mirror.rocksdb.enabled=true
```

### Build Native Container Image

Build a container with GraalVM native image using Cloud Native Buildpacks:

```bash
mvn spring-boot:build-image -Pnative
```

This creates a Docker image: `hms-mirror-native:4.0.0.0`

### Run Native Container

```bash
docker run -p 8080:8080 hms-mirror-native:4.0.0.0
```

### Native Image Limitations

⚠️ **Important Considerations:**

1. **Reflection**: All reflection usage must be configured in `reflect-config.json`
2. **Resources**: All resources must be listed in `resource-config.json`
3. **Dynamic Class Loading**: Not supported (e.g., some JDBC drivers)
4. **JNI**: Native libraries (like RocksDB) require special configuration
5. **Build Time**: Native image compilation takes 5-10 minutes

### Troubleshooting Native Build

If you encounter issues:

1. **Generate config from test run**:
   ```bash
   mvn test -Pnative -Dagent
   ```

2. **Check generated configs**:
   ```
   target/native/agent-output/main/
   ```

3. **Update reflection config** in `src/main/resources/META-INF/native-image/`

## Jib Container Build

Jib builds optimized Docker containers without requiring Docker installed or a Dockerfile.

### Build Container Image (to registry)

```bash
mvn clean package -Pjib
```

This creates:
- `hms-mirror:4.0.0.0` - OCI container image
- `hms-mirror:latest` - Latest tag

By default, builds to local Docker daemon. To push to registry:

```bash
mvn clean package -Pjib -Djib.to.image=myregistry.io/hms-mirror:4.0.0.0
```

### Build Container Image (to local Docker)

For local testing without pushing to registry:

```bash
mvn clean package -Pjib-docker
```

This builds directly to your local Docker daemon.

### Run Jib Container

```bash
docker run -p 8080:8080 \
  -v /path/to/configs:/opt/hms-mirror/configs \
  -v /path/to/data:/opt/hms-mirror/data \
  hms-mirror:4.0.0.0
```

### Jib Multi-Architecture Support

The Jib profile builds for both AMD64 and ARM64 architectures:

```bash
mvn clean package -Pjib \
  -Djib.to.image=myregistry.io/hms-mirror:4.0.0.0 \
  -Djib.from.platforms=linux/amd64,linux/arm64
```

### Jib Container Configuration

The container includes:

- **Base Image**: Eclipse Temurin 17 JRE (Ubuntu Jammy)
- **Working Directory**: `/opt/hms-mirror`
- **Exposed Ports**: 8080, 8443
- **User**: 1001 (non-root)
- **JVM Memory**: 4GB min, 8GB max (configurable)
- **Included Drivers**: All Hive JDBC drivers in `/opt/hms-mirror/drivers`

### Customize Jib Build

Override properties at build time:

```bash
mvn clean package -Pjib \
  -Djib.to.image=my-repo/hms-mirror:custom-tag \
  -Djib.container.user=1000 \
  -Djib.container.jvmFlags="-Xmx16g"
```

### Environment Variables

Configure via environment variables:

```bash
docker run -p 8080:8080 \
  -e JAVA_TOOL_OPTIONS="-Xmx16g" \
  -e HMS_MIRROR_ROCKSDB_ENABLED=true \
  -e SERVER_PORT=8080 \
  hms-mirror:4.0.0.0
```

## Build Profiles Summary

| Profile | Command | Output | Use Case |
|---------|---------|--------|----------|
| (default) | `mvn package` | JAR + tar.gz | Standard deployment |
| `native` | `mvn package -Pnative` | Native executable | Fast startup, low memory |
| `jib` | `mvn package -Pjib` | OCI container image | Production deployment |
| `jib-docker` | `mvn package -Pjib-docker` | Local Docker image | Local testing |

## Performance Comparison

| Build Type | Startup Time | Memory Usage | Build Time | File Size |
|------------|--------------|--------------|------------|-----------|
| Standard JAR | ~15s | ~512MB | ~2 min | ~150MB |
| Native Image | ~0.1s | ~100MB | ~8 min | ~80MB |
| Jib Container | ~15s | ~512MB | ~3 min | ~400MB |

## Continuous Integration

### GitHub Actions Example

```yaml
name: Build All Variants

jobs:
  standard:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-java@v3
        with:
          java-version: '17'
      - run: mvn clean package

  native:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: graalvm/setup-graalvm@v1
        with:
          version: 'latest'
          java-version: '17'
          components: 'native-image'
      - run: mvn clean package -Pnative -DskipTests

  container:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-java@v3
        with:
          java-version: '17'
      - run: mvn clean package -Pjib
```

## Kubernetes Deployment

### Deploy Jib Container to Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: hms-mirror
spec:
  replicas: 1
  selector:
    matchLabels:
      app: hms-mirror
  template:
    metadata:
      labels:
        app: hms-mirror
    spec:
      containers:
      - name: hms-mirror
        image: hms-mirror:4.0.0.0
        ports:
        - containerPort: 8080
        env:
        - name: JAVA_TOOL_OPTIONS
          value: "-Xmx8g"
        - name: HMS_MIRROR_ROCKSDB_ENABLED
          value: "true"
        resources:
          requests:
            memory: "4Gi"
            cpu: "2"
          limits:
            memory: "8Gi"
            cpu: "4"
        volumeMounts:
        - name: config
          mountPath: /opt/hms-mirror/configs
        - name: data
          mountPath: /opt/hms-mirror/data
      volumes:
      - name: config
        configMap:
          name: hms-mirror-config
      - name: data
        persistentVolumeClaim:
          claimName: hms-mirror-data
---
apiVersion: v1
kind: Service
metadata:
  name: hms-mirror
spec:
  selector:
    app: hms-mirror
  ports:
  - port: 8080
    targetPort: 8080
  type: LoadBalancer
```

## Additional Resources

- [Spring Boot Native Image Docs](https://docs.spring.io/spring-boot/docs/current/reference/html/native-image.html)
- [GraalVM Native Image](https://www.graalvm.org/latest/reference-manual/native-image/)
- [Jib Documentation](https://github.com/GoogleContainerTools/jib)
- [Cloud Native Buildpacks](https://buildpacks.io/)
