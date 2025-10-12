# Connection Management Wizard

This wizard is used to define and save cluster connection profiles that can be reused across multiple Data Strategy configurations. Connection profiles contain all necessary information to connect to Hive clusters.

## Purpose

Create reusable cluster connection profiles that can be assigned as either source or target connections in Data Strategy configurations. Each profile contains complete connection information for a specific Hive cluster environment.

These saved connections are referenced by Data Strategy wizards and assigned as LEFT (source) or RIGHT (target) during configuration, eliminating the need to re-enter connection details for each migration.

## Wizard Flow

The wizard consists of 4 streamlined steps:

### Step 1: Basic Information
Define basic information for the cluster connection profile.

| Field | Property Mapped | Input Type | Description/Validation |
|-------|-----------------|------------|------------------------|
| Connection Name | `name` | Text | Required. Unique name for this connection profile |
| Description | `description` | Text area | Optional. Description of this cluster environment |
| Environment Type | `environment` | Dropdown | Required. Environment classification (DEV, TEST, PROD, UAT, STAGING). Default: DEV |
| Platform Type | `platformType` | Dropdown | Required. Target platform (CDP7_1, CDP7_2, HDP2, HDP3, CDH5, CDH6, APACHE, EMR, GENERIC) |
| HDFS Namespace | `hcfsNamespace` | Text | Required. HDFS namespace URI (e.g., hdfs://namenode:8020) |

**Next**: Proceed to HiveServer2 Connection Settings.

### Step 2: HiveServer2 Connection
Configure HiveServer2 connection parameters.

| Field | Property Mapped | Input Type | Description/Validation |
|-------|-----------------|------------|------------------------|
| HiveServer2 URI | `hs2Uri` | Text | Required. HiveServer2 connection URI (e.g., jdbc:hive2://host:10000/default) |
| Username | `hs2Username` | Text | Required. Database username for authentication |
| Password | `hs2Password` | Password | Required. Database password for authentication |
| Driver Class Name | `hs2DriverClassName` | Text | Optional. JDBC driver class. Auto-populated based on platform |
| JAR File Path | `hs2JarFile` | Text | Optional. Path to JDBC driver JAR file. Recommended for production environments |
| Disconnected Mode | `hs2Disconnected` | Checkbox | Optional. Enable disconnected mode for testing without actual database connection |

**Connection Properties** (Optional)
Additional JDBC connection properties can be specified as key-value pairs.

**Next**: Proceed to Metastore Direct (Optional).

### Step 3: Metastore Direct (Optional)
Configure optional direct metastore database connection for enhanced performance.

| Field | Property Mapped | Input Type | Description/Validation |
|-------|-----------------|------------|------------------------|
| Enable Metastore Direct | `metastoreDirectEnabled` | Checkbox | Optional. Enable direct metastore database connection |
| Database URI | `metastoreDirectUri` | Text | Required if enabled. Direct metastore database URI |
| Database Type | `metastoreDirectType` | Dropdown | Required if enabled. Database type (MYSQL, POSTGRESQL, ORACLE) |
| Username | `metastoreDirectUsername` | Text | Required if enabled. Database username |
| Password | `metastoreDirectPassword` | Password | Required if enabled. Database password |
| Min Connections | `metastoreDirectMinConnections` | Number | Optional. Minimum connections in pool (default: 2) |
| Max Connections | `metastoreDirectMaxConnections` | Number | Optional. Maximum connections in pool (default: 10) |

**Connection Pool Settings**
| Field | Property Mapped | Input Type | Description/Validation |
|-------|-----------------|------------|------------------------|
| Pool Library | `connectionPoolLib` | Dropdown | Auto-selected based on platform. Options: DBCP2, HIKARI, HYBRID |

**Advanced Settings** (Optional)
| Field | Property Mapped | Input Type | Description/Validation |
|-------|-----------------|------------|------------------------|
| Auto Partition Discovery | `partitionDiscoveryAuto` | Checkbox | Default: true. Automatically discover table partitions |
| Initialize MSCK | `partitionDiscoveryInitMSCK` | Checkbox | Default: true. Run MSCK REPAIR during partition discovery |
| Partition Bucket Limit | `partitionBucketLimit` | Number | Default: 100. Maximum partition buckets to process |
| Create If Not Exists | `createIfNotExists` | Checkbox | Default: false. Create databases/tables if they don't exist |
| Enable Auto Table Stats | `enableAutoTableStats` | Checkbox | Default: false. Automatically gather table statistics |
| Enable Auto Column Stats | `enableAutoColumnStats` | Checkbox | Default: false. Automatically gather column statistics |

**Next**: Proceed to Test & Save.

### Step 4: Test & Save
Validate connection and save the profile.

**Connection Summary**
- Review all configured connection parameters
- Verify platform type, URIs, and authentication settings

**Connection Test** (Optional)
- **Test Connection**: Button to validate all connection parameters
- **Test Results**: Display connection status for each component (HDFS, HIVESERVER2, METASTORE)
- Shows response times and connection details

**Save Options**
| Field | Input Type | Description |
|-------|------------|-------------|
| Save test results with connection | Checkbox | Default: true. Store test results with the connection profile |

**Actions**:
- **Save Connection**: Save connection profile to repository and return to connections list
- **Test Again**: Re-run connection validation tests
- **Back**: Return to previous step to modify settings

**Success**: Connection profile saved and available for selection in Data Strategy wizards.

## Connection Profile Storage

Saved connection profiles include:
- All cluster connection parameters
- Authentication settings
- Pool configuration
- Environment properties
- Test validation status

These profiles are referenced by name in Data Strategy wizards, enabling rapid configuration reuse across multiple migration projects.

Total wizard time: ~5-8 minutes per connection profile.