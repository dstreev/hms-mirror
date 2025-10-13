# HMS Mirror Dataset Import - YAML Specification

## Overview

This document defines the YAML specification for the HMS Mirror `DatasetDto` class, which is used for importing dataset configurations into HMS Mirror. A dataset represents a collection of databases and their associated tables for processing by HMS Mirror migration workflows.

## Version Information

- **HMS Mirror Version**: 4.x
- **API Version**: v1
- **Last Updated**: October 2025

## YAML Schema Definition

### Root Object: DatasetDto

The root YAML object represents a complete dataset configuration.

```yaml
# Dataset name (required)
name: string

# Dataset description (optional)
description: string

# List of database specifications (required)
databases:
  - # Database specification object
    databaseName: string
    tables: [string]  # Optional - list of specific table names
    filter: # Optional - table filter configuration (mutually exclusive with tables)
      includePattern: string
      excludePattern: string
      tableTypes: [string]
      minPartitions: integer
      maxPartitions: integer
      minSizeBytes: long
      maxSizeBytes: long
    warehouse: # Optional - warehouse configuration
      source: string  # PLAN (only valid value for DatasetDto)
      externalDirectory: string
      managedDirectory: string

# Metadata fields (optional - typically managed by system)
createdDate: string     # ISO 8601 timestamp
modifiedDate: string    # ISO 8601 timestamp
```

## JSON Schema Definition

The following JSON Schema can be used to validate DatasetDto YAML/JSON files:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://docs.cloudera.com/schemas/hms-mirror/dataset-dto.schema.json",
  "title": "HMS Mirror DatasetDto",
  "description": "Schema for HMS Mirror dataset configuration import",
  "type": "object",
  "properties": {
    "name": {
      "type": "string",
      "description": "Unique identifier for the dataset",
      "pattern": "^[a-zA-Z0-9][a-zA-Z0-9_-]*[a-zA-Z0-9]$",
      "minLength": 2,
      "maxLength": 64
    },
    "description": {
      "type": "string",
      "description": "Human-readable description of the dataset",
      "maxLength": 1000
    },
    "databases": {
      "type": "array",
      "description": "List of database specifications",
      "minItems": 1,
      "items": {
        "$ref": "#/$defs/DatabaseSpec"
      }
    },
    "createdDate": {
      "type": "string",
      "description": "ISO 8601 timestamp when dataset was created",
      "format": "date-time"
    },
    "modifiedDate": {
      "type": "string", 
      "description": "ISO 8601 timestamp when dataset was last modified",
      "format": "date-time"
    }
  },
  "required": ["name", "databases"],
  "additionalProperties": false,
  "$defs": {
    "DatabaseSpec": {
      "type": "object",
      "description": "Database specification within a dataset",
      "properties": {
        "databaseName": {
          "type": "string",
          "description": "Name of the database in the Hive metastore",
          "pattern": "^[a-zA-Z][a-zA-Z0-9_]*$",
          "minLength": 1,
          "maxLength": 128
        },
        "tables": {
          "type": "array",
          "description": "Explicit list of table names (mutually exclusive with filter)",
          "items": {
            "type": "string",
            "pattern": "^[a-zA-Z][a-zA-Z0-9_]*$",
            "minLength": 1,
            "maxLength": 128
          },
          "uniqueItems": true
        },
        "filter": {
          "$ref": "#/$defs/TableFilter",
          "description": "Table filter configuration (mutually exclusive with tables)"
        },
        "warehouse": {
          "$ref": "#/$defs/Warehouse",
          "description": "Warehouse configuration for the database"
        }
      },
      "required": ["databaseName"],
      "not": {
        "allOf": [
          {
            "properties": {
              "tables": {
                "minItems": 1
              }
            },
            "required": ["tables"]
          },
          {
            "properties": {
              "filter": {
                "type": "object"
              }
            },
            "required": ["filter"]
          }
        ]
      },
      "additionalProperties": false
    },
    "TableFilter": {
      "type": "object",
      "description": "Filter criteria for selecting tables within a database",
      "properties": {
        "includePattern": {
          "type": "string",
          "description": "Regular expression pattern for table names to include",
          "maxLength": 256
        },
        "excludePattern": {
          "type": "string", 
          "description": "Regular expression pattern for table names to exclude",
          "maxLength": 256
        },
        "tableTypes": {
          "type": "array",
          "description": "Filter by table types",
          "items": {
            "type": "string",
            "enum": [
              "MANAGED_TABLE",
              "EXTERNAL_TABLE", 
              "VIRTUAL_VIEW",
              "MATERIALIZED_VIEW",
              "INDEX_TABLE"
            ]
          },
          "uniqueItems": true
        },
        "minPartitions": {
          "type": "integer",
          "description": "Minimum number of partitions (0 = no minimum)",
          "minimum": 0,
          "default": 0
        },
        "maxPartitions": {
          "type": "integer",
          "description": "Maximum number of partitions (0 = no maximum)", 
          "minimum": 0,
          "default": 0
        },
        "minSizeBytes": {
          "type": "integer",
          "description": "Minimum table size in bytes (0 = no minimum)",
          "minimum": 0,
          "default": 0
        },
        "maxSizeBytes": {
          "type": "integer",
          "description": "Maximum table size in bytes (0 = no maximum)",
          "minimum": 0,
          "default": 0
        }
      },
      "additionalProperties": false
    },
    "Warehouse": {
      "type": "object",
      "description": "Warehouse configuration for database storage locations",
      "properties": {
        "source": {
          "type": "string",
          "description": "Source of the warehouse configuration",
          "enum": ["PLAN"],
          "default": "PLAN"
        },
        "externalDirectory": {
          "type": "string",
          "description": "Base directory for external tables (database name will be appended)",
          "pattern": "^/.*[^/]$"
        },
        "managedDirectory": {
          "type": "string",
          "description": "Base directory for managed tables (database name will be appended)",
          "pattern": "^/.*[^/]$"
        }
      },
      "additionalProperties": false
    }
  }
}
```

### Schema Validation Rules

The JSON Schema enforces the following validation rules:

1. **Required Fields**: `name` and `databases` are mandatory
2. **Name Pattern**: Dataset names must be alphanumeric with underscores/hyphens, 2-64 characters
3. **Database Names**: Must start with letter, contain only letters/numbers/underscores, max 128 characters
4. **Table Names**: Same pattern as database names
5. **Mutual Exclusivity**: Cannot specify both `tables` and `filter` in the same database specification
6. **Directory Paths**: Must start with `/` and not end with `/`
7. **Enum Values**: `source` and `tableTypes` must use predefined values
8. **Numeric Constraints**: Partition counts and sizes must be non-negative

### Using the Schema

The schema can be used with various validation tools:

**YAML Validation (with yajsv):**
```bash
yajsv -s dataset-dto.schema.json dataset.yaml
```

**JSON Validation (with ajv-cli):**
```bash
ajv validate -s dataset-dto.schema.json -d dataset.json
```

**Python Validation:**
```python
import yaml
import jsonschema

# Load schema and data
with open('dataset-dto.schema.json', 'r') as schema_file:
    schema = json.load(schema_file)
    
with open('dataset.yaml', 'r') as yaml_file:
    data = yaml.safe_load(yaml_file)

# Validate
jsonschema.validate(data, schema)
```

## Field Specifications

### Root Level Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Unique identifier for the dataset. Must be unique within the HMS Mirror instance. |
| `description` | string | No | Human-readable description of the dataset and its purpose. |
| `databases` | array | Yes | List of database specifications included in this dataset. Must contain at least one database. |
| `createdDate` | string | No | ISO 8601 timestamp when the dataset was created. Typically managed by the system. |
| `modifiedDate` | string | No | ISO 8601 timestamp when the dataset was last modified. Typically managed by the system. |

### Database Specification Fields

Each database object in the `databases` array supports the following fields:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `databaseName` | string | Yes | Name of the database in the Hive metastore. |
| `tables` | array[string] | No | Explicit list of table names to include. Mutually exclusive with `filter`. |
| `filter` | object | No | Table filter configuration for pattern-based table selection. Mutually exclusive with `tables`. |
| `warehouse` | object | No | Warehouse directory configuration for the database. |

### Table Filter Configuration

The `filter` object supports pattern-based table selection:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `includePattern` | string | No | Regular expression pattern for table names to include. |
| `excludePattern` | string | No | Regular expression pattern for table names to exclude. |
| `tableTypes` | array[string] | No | Filter by table types (e.g., `EXTERNAL_TABLE`, `MANAGED_TABLE`). |
| `minPartitions` | integer | No | Minimum number of partitions. Use `0` for no minimum. Default: `0`. |
| `maxPartitions` | integer | No | Maximum number of partitions. Use `0` for no maximum. Default: `0`. |
| `minSizeBytes` | long | No | Minimum table size in bytes. Use `0` for no minimum. Default: `0`. |
| `maxSizeBytes` | long | No | Maximum table size in bytes. Use `0` for no maximum. Default: `0`. |

### Warehouse Configuration

The `warehouse` object defines directory locations for database storage:

| Field | Type | Required | Description                                                                                                                 |
|-------|------|----------|-----------------------------------------------------------------------------------------------------------------------------|
| `source` | string | No | Source of the warehouse configuration. Valid values: `PLAN`. Default: `PLAN`.                                               |
| `externalDirectory` | string | No | Base directory for external tables. Database name will be appended automatically. Must start with `/` and not end with `/`. |
| `managedDirectory` | string | No | Base directory for managed tables. Database name will be appended automatically. Must start with `/` and not end with `/`.  |

#### Warehouse Source Values

- `PLAN`: Configuration comes from a user-defined plan

## Examples

### Example 1: Dataset with Explicit Table Lists

```yaml
name: production-analytics
description: Production analytics databases for migration to CDP
databases:
  - databaseName: sales_db
    tables:
      - customer_data
      - order_history
      - sales_metrics
    warehouse:
      source: PLAN
      externalDirectory: /warehouse/sales_external
      managedDirectory: /warehouse/sales_managed
      
  - databaseName: marketing_db
    tables:
      - campaign_data
      - customer_segments
```

### Example 2: Dataset with Table Filters

```yaml
name: data-lake-migration
description: Migrate all fact and dimension tables from the data lake
databases:
  - databaseName: warehouse_db
    filter:
      includePattern: "^(fact_|dim_).*"
      excludePattern: ".*_(temp|staging|backup)$"
      tableTypes:
        - EXTERNAL_TABLE
        - MANAGED_TABLE
      minPartitions: 1
      maxSizeBytes: 10737418240  # 10 GB limit
    warehouse:
      source: PLAN
      externalDirectory: /data/warehouse/external
      managedDirectory: /data/warehouse/managed
      
  - databaseName: logs_db
    filter:
      includePattern: "access_log_.*"
      minPartitions: 0
      maxPartitions: 365  # Max 365 partitions (daily for 1 year)
```

### Example 3: Mixed Configuration

```yaml
name: hybrid-migration
description: Mixed dataset with both explicit tables and filtered tables
databases:
  # Critical tables specified explicitly
  - databaseName: finance_db
    tables:
      - general_ledger
      - accounts_payable
      - accounts_receivable
    warehouse:
      source: PLAN
      externalDirectory: /finance/external
      managedDirectory: /finance/managed
      
  # Analytics tables selected by pattern
  - databaseName: analytics_db
    filter:
      includePattern: "analytics_.*"
      excludePattern: ".*_temp$"
      tableTypes:
        - EXTERNAL_TABLE
      minSizeBytes: 1048576  # Minimum 1 MB

# System-managed metadata (typically not included in import files)
createdDate: "2025-01-13T10:30:00Z"
modifiedDate: "2025-01-13T14:45:00Z"
```

## Import Process

### File Format
- File extension: `.yaml` or `.yml`
- Character encoding: UTF-8
- YAML version: 1.2

### Import Validation
The import process validates:

1. **Schema Compliance**: All required fields are present and correctly typed
2. **Business Rules**: 
   - Dataset name uniqueness
   - Database name validity
   - Mutual exclusivity between `tables` and `filter` configurations
   - Valid regex patterns in filter expressions
   - Valid warehouse directory paths (must start with `/`, must not end with `/`)
3. **Referential Integrity**: Database names must exist in the target metastore

### Error Handling
Import errors are categorized as:

- **Schema Errors**: Invalid YAML structure or missing required fields
- **Validation Errors**: Business rule violations or invalid values
- **Runtime Errors**: Database connectivity issues or metastore access problems

## Best Practices

### Dataset Organization
- Use descriptive dataset names that reflect the migration scope
- Include meaningful descriptions for dataset documentation
- Group related databases in the same dataset for logical organization

### Table Selection Strategy
- Use explicit table lists for critical or sensitive tables requiring precise control
- Use filter patterns for large numbers of tables following naming conventions
- Combine both approaches in the same dataset when appropriate

### Warehouse Configuration
- Use `PLAN` source for custom directory structures
- Use `GLOBAL` source to inherit from HMS Mirror global configuration
- Ensure directory paths are consistent with target environment storage policies

### Performance Considerations
- Use size and partition filters to exclude very large or very small tables when appropriate
- Consider breaking large datasets into smaller, focused datasets for better processing performance
- Use descriptive regex patterns that are efficient and maintainable

## API Integration

### REST Endpoints
The dataset import functionality is accessible via the HMS Mirror REST API:

- `POST /api/v1/datasets/import` - Import a dataset from YAML/JSON content
- `PUT /api/v1/datasets/{name}/import` - Update existing dataset from YAML/JSON content
- `GET /api/v1/datasets/{name}/export` - Export dataset as YAML
- `GET /api/v1/datasets/schema` - Retrieve the JSON Schema for validation

### Content Types
The API supports both YAML and JSON content types:

- **YAML Import**: `Content-Type: application/x-yaml` or `text/yaml`
- **JSON Import**: `Content-Type: application/json`
- **Schema Validation**: All imports are automatically validated against the JSON Schema

### Response Format
Import operations return a standard HMS Mirror API response:

```yaml
status: SUCCESS | ERROR | WARNING
message: string
data:
  datasetName: string
  databasesProcessed: integer
  tablesDiscovered: integer
errors:
  - field: string
    message: string
    code: string
```

## Migration Strategy Integration

HMS Mirror datasets integrate with migration strategies as follows:

- **SCHEMA_ONLY**: Use datasets to define scope of metadata migration
- **LINKED**: Create external table links for all tables in dataset
- **HYBRID**: Selective data movement based on dataset table selection
- **SQL**: Generate SQL export/import scripts for dataset tables
- **EXPORT_IMPORT**: Use Hive export/import for dataset table data

## Troubleshooting

### Common Issues

1. **Invalid Regex Patterns**: Test regex patterns with sample table names before import
2. **Directory Path Issues**: Ensure warehouse directories exist and are accessible
3. **Large Dataset Timeouts**: Consider breaking large datasets into smaller chunks
4. **Filter Conflicts**: Verify include/exclude patterns don't conflict

### Validation Tools
HMS Mirror provides validation utilities:

- YAML schema validation
- Regex pattern testing
- Database connectivity verification
- Warehouse directory access testing

---

**Note**: This specification is for HMS Mirror v4.x. For older versions, consult the version-specific documentation or use the HMS Mirror CLI help system.