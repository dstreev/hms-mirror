# HMS Mirror Core Business Logic Usage

This document demonstrates how the HMS Mirror core business logic can be used independently of Spring framework and the main application context.

## Architecture Overview

The core business logic has been extracted into a clean layered architecture:

```
┌─────────────────────────────────────────┐
│           Application Layer             │
│  (Spring Services, CLI, Web API, etc.) │
└─────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────┐
│         Infrastructure Layer           │
│    (Configuration, Connection          │
│     Providers - Adapters)              │
└─────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────┐
│           Core Business Layer           │
│  (Pure Business Logic - Framework      │
│   Independent)                          │
└─────────────────────────────────────────┘
```

## Key Benefits

1. **Framework Independence**: Business logic doesn't depend on Spring or any specific framework
2. **Testability**: Easy to unit test with mocked dependencies
3. **Reusability**: Same logic can be used in CLI tools, web services, batch jobs, etc.
4. **Clean Architecture**: Clear separation of concerns

## Core Interfaces

### LocationTranslator
Handles all location translation business logic:
- Global Location Map processing
- Table location translation
- Partition location translation

### TableOperations  
Handles table-related business logic:
- Table filtering and validation
- Migration decisions
- Table processing workflows

## Usage Examples

### 1. Using LocationTranslator Independently

```java
// Create configuration provider (no Spring needed)
ConfigurationProvider configProvider = new MyConfigProvider();

// Create the core business logic
LocationTranslator locationTranslator = new LocationTranslatorImpl(configProvider);

// Use Global Location Map processing
GlobalLocationMapResult result = locationTranslator.processGlobalLocationMap(
    "hdfs://old-cluster/data/tables", 
    true  // isExternalTable
);

if (result.isMapped()) {
    System.out.println("Original: " + result.getOriginalDir());
    System.out.println("Mapped: " + result.getMappedDir());
}
```

### 2. Using TableOperations Independently

```java
// Create infrastructure providers
ConfigurationProvider configProvider = new MyConfigProvider();
ConnectionProvider connectionProvider = new MyConnectionProvider();

// Create the core business logic  
TableOperations tableOperations = new TableOperationsImpl(
    connectionProvider, 
    configProvider
);

// Validate table against business rules
ValidationResult validation = tableOperations.validateTableFilter(
    tableMirror, 
    Environment.LEFT
);

if (validation.isValid()) {
    System.out.println("Table passes all filters");
} else {
    System.out.println("Table filtered out: " + validation.getMessage());
}
```

### 3. Different Application Contexts

The same core business logic can be used across different contexts:

#### CLI Application
```java
public class CliMigrationTool {
    public static void main(String[] args) {
        // Parse command line args into configuration
        ConfigurationProvider config = new CliConfigurationProvider(args);
        
        // Use core business logic
        LocationTranslator translator = new LocationTranslatorImpl(config);
        
        // Process migration...
    }
}
```

#### Web Service
```java
@RestController
public class MigrationController {
    
    @Autowired
    private LocationTranslator locationTranslator;  // Injected via Spring
    
    @PostMapping("/translate-location")  
    public LocationTranslationResult translateLocation(@RequestBody LocationRequest request) {
        // Use the same business logic in web context
        return locationTranslator.translateTableLocation(request);
    }
}
```

#### Batch Processing Job
```java
public class BatchMigrationProcessor {
    
    public void processBatch(List<TableInfo> tables) {
        // Create business logic for batch context
        ConfigurationProvider config = new BatchConfigurationProvider();
        TableOperations tableOps = new TableOperationsImpl(connectionProvider, config);
        
        for (TableInfo table : tables) {
            ValidationResult result = tableOps.validateTableFilter(table, Environment.LEFT);
            // Process based on validation result...
        }
    }
}
```

## Implementation Details

### Configuration Provider Interface
```java
public interface ConfigurationProvider {
    HmsMirrorConfig getConfig();
    void updateConfig(HmsMirrorConfig config);
    boolean validateConfig();
    void reloadConfig();
}
```

You can implement this interface to load configuration from:
- Properties files
- Environment variables  
- Command line arguments
- Database
- Remote configuration service
- In-memory objects (for testing)

### Connection Provider Interface
```java
public interface ConnectionProvider extends AutoCloseable {
    Connection getConnection(Environment environment);
    void closeConnection(Environment environment);
    boolean isConnectionValid(Environment environment);
}
```

This can be implemented to provide connections from:
- Connection pools
- Direct JDBC connections
- Mock connections (for testing)
- Cloud database services

## Testing Benefits

The core business logic is easy to test because:

```java
@Test
public void testLocationTranslation() {
    // Arrange: Create mock configuration
    ConfigurationProvider mockConfig = mock(ConfigurationProvider.class);
    when(mockConfig.getConfig()).thenReturn(testConfiguration);
    
    // Act: Use pure business logic
    LocationTranslator translator = new LocationTranslatorImpl(mockConfig);
    GlobalLocationMapResult result = translator.processGlobalLocationMap("test-location", true);
    
    // Assert: Verify business logic behavior
    assertTrue(result.isMapped());
    assertEquals("expected-mapped-location", result.getMappedDir());
}
```

No Spring context or complex setup required - just mock the infrastructure dependencies and test the business logic directly.

## Migration from Spring Services

The existing Spring services have been updated to delegate to the core business logic:

### Before (Tightly Coupled)
```java
@Service
public class TranslatorService {
    // Business logic mixed with Spring concerns
    public String translateLocation(...) {
        // Complex business logic here mixed with Spring dependencies
    }
}
```

### After (Layered Architecture)  
```java
@Service  
public class TranslatorService {
    
    @Autowired
    private LocationTranslator locationTranslator;  // Core business logic
    
    public String translateLocation(...) {
        // Delegate to pure business logic
        LocationTranslationResult result = locationTranslator.translateTableLocation(request);
        
        // Handle Spring-specific concerns (logging, error handling, etc.)
        if (!result.isSuccess()) {
            log.error("Translation failed: {}", result.getMessage());
            throw new MismatchException(result.getMessage());
        }
        
        return result.getTranslatedLocation();
    }
}
```

The Spring service now focuses on:
- Dependency injection coordination
- Exception handling and error mapping
- Logging and monitoring
- Transaction management  
- Request/response handling

While the core business logic focuses purely on:
- Business rules and validation
- Data transformation
- Algorithm implementation
- Domain logic

This creates a clean separation where business logic is reusable and testable, while infrastructure concerns are handled by the appropriate layer.