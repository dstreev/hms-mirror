# HMS-Mirror MCP Server

This MCP (Model Context Protocol) server provides programmatic access to HMS-Mirror functionality, enabling AI assistants and other tools to interact with HMS-Mirror for Hive metastore migrations.

## Installation

1. Install the MCP server:
```bash
cd mcp-server
npm install
npm run build
```

2. Configure Claude Desktop to use the MCP server by adding to `~/Library/Application Support/Claude/claude_desktop_config.json`:
```json
{
  "mcpServers": {
    "hms-mirror": {
      "command": "node",
      "args": ["/path/to/hms-mirror/mcp-server/dist/index.js"],
      "env": {
        "HMS_MIRROR_HOME": "/path/to/.hms-mirror"
      }
    }
  }
}
```

## Available Tools

### validate_config
Validates an HMS-Mirror configuration file for correctness.
- **Input**: `configPath` - Path to configuration file
- **Output**: Validation results with errors and warnings

### run_migration
Executes a migration using HMS-Mirror.
- **Inputs**:
  - `configPath` - Configuration file path (required)
  - `database` - Specific database to migrate (optional)
  - `dryRun` - Perform dry run without changes (optional)
  - `strategy` - Migration strategy to use (optional)
- **Output**: Migration results and report location

### list_reports
Lists available migration reports.
- **Inputs**:
  - `database` - Filter by database name (optional)
  - `limit` - Maximum reports to return (default: 20)
- **Output**: List of reports with metadata

### get_report
Gets detailed information about a specific migration report.
- **Input**: `reportPath` - Path to report directory
- **Output**: Complete report details including status, configuration, and files

### analyze_tables
Analyzes tables in a database for migration readiness.
- **Inputs**:
  - `configPath` - Configuration file path
  - `database` - Database to analyze
- **Output**: Table analysis with recommended strategies

### generate_distcp_script
Generates executable distcp commands from migration plans.
- **Inputs**:
  - `reportPath` - Report directory path
  - `environment` - LEFT or RIGHT environment
- **Output**: Shell script with distcp commands

### check_prerequisites
Verifies HMS-Mirror installation and prerequisites.
- **Output**: Status of all prerequisite checks

## Available Resources

### hms://configs
Lists all available configuration files in the HMS-Mirror configs directory.

### hms://reports
Provides access to all migration reports.

### hms://strategies
Describes all available migration strategies and their use cases.

### hms://config/{name}
Reads a specific configuration file.

### hms://report/{path}
Reads a specific migration report.

## Available Prompts

### migration_plan
Generates a comprehensive migration plan for a database.
- **Arguments**:
  - `database` - Database name (required)
  - `strategy` - Migration strategy (optional)

### troubleshoot
Provides troubleshooting guidance for failed migrations.
- **Arguments**:
  - `reportPath` - Path to failed migration report

## Usage Examples

### Validate a Configuration
```typescript
await mcp.callTool('validate_config', {
  configPath: '/home/user/.hms-mirror/configs/production.yaml'
});
```

### Run a Migration
```typescript
await mcp.callTool('run_migration', {
  configPath: '/home/user/.hms-mirror/configs/production.yaml',
  database: 'sales_db',
  dryRun: true,
  strategy: 'HYBRID'
});
```

### Generate DistCp Script
```typescript
await mcp.callTool('generate_distcp_script', {
  reportPath: 'customer/acme/2024-01-15_10-30-00',
  environment: 'LEFT'
});
```

### Get Migration Plan
```typescript
await mcp.getPrompt('migration_plan', {
  database: 'analytics_db',
  strategy: 'STORAGE_MIGRATION'
});
```

## Environment Variables

- `HMS_MIRROR_HOME`: Path to HMS-Mirror installation directory (default: `~/.hms-mirror`)

## Integration with AI Assistants

This MCP server enables AI assistants to:
1. Validate and analyze migration configurations
2. Execute and monitor migrations
3. Generate data transfer scripts
4. Troubleshoot migration issues
5. Provide migration recommendations
6. Access historical migration reports

## Error Handling

The server provides detailed error messages for:
- Invalid configurations
- Missing prerequisites
- Failed migrations
- Resource access errors

All errors follow the MCP error format with appropriate error codes.

## Security Considerations

- The server runs with the permissions of the user executing it
- Access to configuration files and reports is controlled by filesystem permissions
- Sensitive information in configurations (passwords, keys) should be encrypted
- The server does not expose network endpoints directly

## Development

### Building from Source
```bash
npm install
npm run build
```

### Running in Development Mode
```bash
npm run dev
```

### TypeScript Support
The server is written in TypeScript with full type definitions for all MCP operations.

## Support

For issues or questions about the MCP server, please refer to:
- HMS-Mirror documentation
- MCP specification: https://modelcontextprotocol.io
- GitHub issues: https://github.com/cloudera-labs/hms-mirror