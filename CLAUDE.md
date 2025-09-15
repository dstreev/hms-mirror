# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

HMS-Mirror is a Hadoop Hive Metastore migration utility that helps transfer data and metadata between different Hive environments. It's built with Java 17+ and Spring Boot, providing both CLI and web interfaces for Hive table migrations across different clusters.

## Build and Development Commands

### Maven Build
- **Build project**: `mvn clean package`
- **Run tests**: `mvn test`
- **Skip tests**: `mvn clean package -DskipTests`
- **Generate distribution**: `mvn clean package` (creates tar.gz in target/)

### Application Execution
- **CLI mode**: `./bin/hms-mirror [options]` (after installation via setup.sh)
- **Web service mode**: `./bin/hms-mirror --service`
- **Stop service**: `./bin/hms-mirror --stop`
- **Setup installation**: `./bin/setup.sh` (installs to ~/.hms-mirror or /usr/local/hms-mirror)

### Java Requirements
- **Minimum Java version**: 17
- **Memory settings**: Default -Xms4096m -Xmx8192m -XX:+UseG1GC
- Uses both standalone and thin JAR variants

## Architecture Overview

### New Layered Architecture (v4.0+)
The application is transitioning to a layered architecture that separates pure business logic from Spring infrastructure:

#### Core Business Logic Layer (`com.cloudera.utils.hms.mirror.core`)
- **`core.api`**: Business interfaces (TableOperations, LocationTranslator) - pure business logic without Spring dependencies
- **`core.impl`**: Business logic implementations - testable without Spring context
- **`core.model`**: Business models and DTOs (TableMigrationRequest, ValidationResult, etc.)
- **`core.exception`**: Business-specific exceptions

#### Infrastructure Layer (`com.cloudera.utils.hms.mirror.infrastructure`) 
- **`infrastructure.connection`**: Connection management abstractions and adapters
- **`infrastructure.configuration`**: Configuration loading and management
- **`infrastructure.persistence`**: Data access abstractions
- **`infrastructure.reporting`**: Report generation infrastructure

#### Service Layer (`com.cloudera.utils.hms.mirror.service`)
- **Thin Spring service wrappers** that delegate to core business logic
- Handle Spring-specific concerns (transactions, security, caching)
- Bridge between web/CLI interfaces and core business logic

### Legacy Package Structure
- **`com.cloudera.utils.hms.mirror.cli`**: Command-line interface and Spring Boot main class
- **`com.cloudera.utils.hms.mirror.domain`**: Core domain models (HmsMirrorConfig, TableMirror, Cluster, etc.)
- **`com.cloudera.utils.hms.mirror.datastrategy`**: Migration strategy implementations (SCHEMA_ONLY, LINKED, HYBRID, SQL, etc.)
- **`com.cloudera.utils.hms.mirror.connections`**: Database connection pooling (DBCP2, Hikari, Hybrid implementations)
- **`com.cloudera.utils.hms.mirror.web`**: Web interface controllers and configuration

### Key Design Patterns
- **Layered Architecture**: Separation of business logic, infrastructure, and service layers
- **Strategy Pattern**: DataStrategy implementations for different migration approaches
- **Adapter Pattern**: Infrastructure adapters bridge legacy services with new core APIs
- **Dependency Inversion**: Core business logic depends on abstractions, not implementations
- **Configuration-driven**: YAML configuration files drive migration behavior
- **Multi-interface**: Supports CLI, web UI, and REST API interfaces

### Migration Strategies
The application implements multiple data movement strategies:
- **SCHEMA_ONLY**: Metadata migration only
- **LINKED**: Creates external tables pointing to original data
- **HYBRID**: Combines schema and limited data movement
- **SQL**: Uses SQL export/import for data transfer
- **EXPORT_IMPORT**: Hive export/import mechanism
- **STORAGE_MIGRATION**: In-place storage format migrations

### Connection Management
- Supports multiple connection pool types (DBCP2, Hikari, Hybrid)
- Direct metastore connections and HiveServer2 connections
- Database-specific drivers for MySQL/MariaDB, PostgreSQL, Oracle

### Configuration System
- Main config: `configs/default.template.yaml`
- Environment-specific configurations in test resources
- Supports encrypted passwords
- Warehouse plans for namespace translations

## Development Notes

- **Spring Boot application** with both CLI and web modes
- **Lombok** used extensively for reducing boilerplate
- **Testing framework**: JUnit with Spring Boot Test
- **Assembly plugin** creates distributable tar.gz packages
- **Multi-profile builds** support different Hadoop/CDP versions
- **Asynchronous processing** enabled via @EnableAsync

## Migration to New Architecture

### Current Status
The project is transitioning from a monolithic Spring service layer to a layered architecture:

1. **Foundation Complete**: Core business interfaces and infrastructure abstractions are defined
2. **Example Implementations**: `TableOperationsImpl` demonstrates pure business logic separation
3. **Adapter Layer**: Spring adapters bridge existing services with new core APIs
4. **Gradual Migration**: New `TableServiceV2` shows how existing services can be refactored

### Key Benefits of New Architecture
- **Reusability**: Core business logic can be used in non-Spring applications
- **Testability**: Business logic tests don't require Spring context
- **Maintainability**: Clear separation between business rules and infrastructure
- **Performance**: Reduced Spring overhead for core operations

### Migration Approach
1. **Phase 1**: Extract table operations from `TableService` to `TableOperationsImpl`
2. **Phase 2**: Extract database operations from `DatabaseService` 
3. **Phase 3**: Create migration orchestration layer
4. **Phase 4**: Move infrastructure concerns to dedicated packages
5. **Phase 5**: Update existing services to use core APIs

### Example Usage
```java
// Old approach (Spring-coupled)
@Service
public class TableService {
    @Autowired ConfigService configService;
    // Business logic mixed with Spring concerns
}

// New approach (business logic separated)
public class TableOperationsImpl implements TableOperations {
    // Pure business logic, no Spring dependencies
}

@Service
public class TableServiceV2 {
    private final TableOperations tableOperations;
    // Thin Spring wrapper delegating to core business logic
}
```