### Connection Management Wizard

This wizard is used to define and save cluster connection profiles that can be reused across multiple Data Strategy configurations. Connection profiles contain all necessary information to connect to Hive clusters.

## Purpose

Create reusable cluster connection profiles that can be assigned as either source or target connections in Data Strategy configurations. Each profile contains complete connection information for a specific Hive cluster environment.

These saved connections are referenced by Data Strategy wizards and assigned as LEFT (source) or RIGHT (target) during configuration, eliminating the need to re-enter connection details for each migration.

## Wizard Flow

#### Screen 1: Connection Profile Definition
Define basic information for the cluster connection profile.

| Field | Input Type | Description/Validation |
|-------|------------|------------------------|
| Connection Name | Text | Required. Unique name for this connection profile |
| Description | Text area | Optional. Description of this cluster environment |
| Environment Type | Dropdown | Optional. Environment classification (DEV, TEST, PROD, etc.) |

**Next**: Proceed to Basic Connection Settings.

#### Screen 2: Basic Connection Settings
Configure core connection parameters.

| Field | Property Mapped | Input Type | Description/Validation |
|-------|-----------------|------------|------------------------|
| Legacy | `legacy` | Checkbox (default: false) | Use legacy Hive connection mode. Required. |
| HDFS Namespace | `hcfsNamespace` | Text | HDFS namespace URI (e.g., hdfs://namenode:8020). Required. |
| Hive Server 2 URI | `hs2` | Text | HiveServer2 connection URI. Required. |
| Metastore URI | `metastoreDirectUri` | Text | Direct metastore connection URI. Optional. |

**Next**: Proceed to Authentication.

#### Screen 3: Authentication Settings
Configure authentication and security.

| Field | Property Mapped | Input Type | Description/Validation |
|-------|-----------------|------------|------------------------|
| Kerberos Enabled | `kerberosConfig.enabled` | Checkbox (default: false) | Enable Kerberos authentication. Required. |

**Kerberos Settings (shown if Kerberos enabled)**
| Field | Property Mapped | Input Type | Description/Validation |
|-------|-----------------|------------|------------------------|
| Principal | `kerberosConfig.principal` | Text | Kerberos principal. Required if Kerberos enabled. |
| Keytab | `kerberosConfig.keytab` | File path | Path to keytab file. Required if Kerberos enabled. |

**Next**: Proceed to Connection Pool.

#### Screen 4: Connection Pool Configuration
Configure database connection pooling settings.

| Field | Property Mapped | Input Type | Description/Validation |
|-------|-----------------|------------|------------------------|
| Pool Type | `connectionPoolLib` | Dropdown | Connection pool implementation. Options: DBCP2, HIKARI, HYBRID. Default: DBCP2. |
| Max Pool Size | `connectionPool.max` | Number (default: 10) | Maximum connections in pool. Required; positive integer. |
| Driver Class Name | `jarFile` | Text | JDBC driver class. Required. |

**Advanced Pool Settings (Optional)**
| Field | Property Mapped | Input Type | Description/Validation |
|-------|-----------------|------------|------------------------|
| Initial Pool Size | `connectionPool.initial` | Number (default: 3) | Initial connections created. Optional; non-negative. |
| Min Idle | `connectionPool.minIdle` | Number (default: 3) | Minimum idle connections. Optional; non-negative. |
| Max Idle | `connectionPool.maxIdle` | Number (default: 5) | Maximum idle connections. Optional; positive. |

**Next**: Proceed to Environment Settings.

#### Screen 5: Environment and Properties
Configure environment-specific settings.

| Field | Property Mapped | Input Type | Description/Validation |
|-------|-----------------|------------|------------------------|
| Environment | `environment` | Text | Environment identifier (e.g., DEV, PROD). Optional. |
| Partition Bucket Limit | `partitionDiscovery.partitionBucketLimit` | Number (default: 100) | Limit for partition bucket discovery. Optional; positive. |

**Custom Properties (Optional)**
| Field | Property Mapped | Input Type | Description/Validation |
|-------|-----------------|------------|------------------------|
| Hive Properties | `hiveVar` | Text area | Custom Hive variables (key=value pairs). Optional. |
| Environment Properties | `envVar` | Text area | Environment variables (key=value pairs). Optional. |

**Next**: Proceed to Connection Test and Save.

#### Screen 6: Connection Test and Save
Validate connection and save the profile.

**Connection Test**
- **Test Connection**: Button to validate all connection parameters
- **Test Results**: Display connection status and any errors
- **Connection Summary**: Show final connection configuration

**Save Connection Profile**
| Field | Input Type | Description/Validation |
|-------|------------|------------------------|
| Profile Name | Text (pre-filled) | Confirmation of connection profile name. Required. |
| Make Default | Checkbox | Set as default connection profile. Optional. |

**Actions**:
- **Save Profile**: Save connection profile to repository
- **Save and Create Another**: Save and start new connection wizard
- **Cancel**: Discard connection profile
- **Back to Edit**: Return to modify settings

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