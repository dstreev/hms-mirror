#!/usr/bin/env node
import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
  ListResourcesRequestSchema,
  ReadResourceRequestSchema,
  ListPromptsRequestSchema,
  GetPromptRequestSchema,
  ErrorCode,
  McpError
} from '@modelcontextprotocol/sdk/types.js';
import { z } from 'zod';
import { exec } from 'child_process';
import { promisify } from 'util';
import * as fs from 'fs/promises';
import * as path from 'path';
import * as yaml from 'js-yaml';
import { glob } from 'glob';
import axios, { AxiosInstance } from 'axios';

const execAsync = promisify(exec);

// Configuration
const HMS_MIRROR_HOME = process.env.HMS_MIRROR_HOME || path.join(process.env.HOME || '', '.hms-mirror');
const HMS_MIRROR_BIN = path.join(HMS_MIRROR_HOME, 'bin', 'hms-mirror');
const REPORTS_DIR = path.join(HMS_MIRROR_HOME, 'reports');
const CONFIGS_DIR = path.join(HMS_MIRROR_HOME, 'configs');

// Web service configuration
const WEB_SERVICE_URL = process.env.HMS_MIRROR_SERVICE_URL || 'http://localhost:8080/hms-mirror';
const WEB_SERVICE_TIMEOUT = parseInt(process.env.HMS_MIRROR_SERVICE_TIMEOUT || '30000');

// Integration mode enum
enum IntegrationMode {
  CLI_ONLY = 'cli',
  WEB_SERVICE_ONLY = 'web',
  HYBRID = 'hybrid'  // Try web service first, fall back to CLI
}

// Server implementation
class HmsMirrorMcpServer {
  private server: Server;
  private transport: StdioServerTransport;
  private httpClient: AxiosInstance;
  private integrationMode: IntegrationMode;
  private webServiceAvailable: boolean = false;

  constructor() {
    this.server = new Server(
      {
        name: 'hms-mirror-mcp',
        version: '2.0.0',
      },
      {
        capabilities: {
          tools: {},
          resources: {},
          prompts: {}
        },
      }
    );

    this.transport = new StdioServerTransport();
    
    // Configure HTTP client for web service
    this.httpClient = axios.create({
      baseURL: WEB_SERVICE_URL,
      timeout: WEB_SERVICE_TIMEOUT,
      headers: {
        'Content-Type': 'application/json'
      }
    });

    // Determine integration mode
    const mode = process.env.HMS_MIRROR_INTEGRATION_MODE?.toLowerCase();
    this.integrationMode = mode === 'cli' ? IntegrationMode.CLI_ONLY :
                          mode === 'web' ? IntegrationMode.WEB_SERVICE_ONLY :
                          IntegrationMode.HYBRID;

    this.setupHandlers();
    this.checkWebServiceAvailability();
  }

  private async checkWebServiceAvailability() {
    if (this.integrationMode === IntegrationMode.CLI_ONLY) {
      return;
    }

    try {
      const response = await this.httpClient.get('/api/health');
      this.webServiceAvailable = response.status === 200;
      console.error(`HMS-Mirror web service available: ${this.webServiceAvailable}`);
    } catch (error) {
      this.webServiceAvailable = false;
      if (this.integrationMode === IntegrationMode.WEB_SERVICE_ONLY) {
        console.error('WARNING: Web service not available but mode is WEB_SERVICE_ONLY');
      }
    }
  }

  private setupHandlers() {
    // Tool handlers
    this.server.setRequestHandler(ListToolsRequestSchema, async () => ({
      tools: [
        {
          name: 'validate_config',
          description: 'Validate an HMS-Mirror configuration file',
          inputSchema: {
            type: 'object',
            properties: {
              configPath: {
                type: 'string',
                description: 'Path to the configuration file to validate'
              },
              configContent: {
                type: 'string',
                description: 'Configuration content as YAML string (alternative to configPath)'
              }
            }
          }
        },
        {
          name: 'run_migration',
          description: 'Execute a migration using HMS-Mirror',
          inputSchema: {
            type: 'object',
            properties: {
              configPath: {
                type: 'string',
                description: 'Path to the configuration file'
              },
              database: {
                type: 'string',
                description: 'Database to migrate (optional, migrates all if not specified)'
              },
              dryRun: {
                type: 'boolean',
                description: 'Perform a dry run without executing changes',
                default: false
              },
              strategy: {
                type: 'string',
                description: 'Migration strategy to use',
                enum: ['SCHEMA_ONLY', 'LINKED', 'SQL', 'EXPORT_IMPORT', 'HYBRID', 'STORAGE_MIGRATION', 'COMMON']
              },
              async: {
                type: 'boolean',
                description: 'Run migration asynchronously (web service only)',
                default: false
              }
            },
            required: ['configPath']
          }
        },
        {
          name: 'list_reports',
          description: 'List available migration reports',
          inputSchema: {
            type: 'object',
            properties: {
              database: {
                type: 'string',
                description: 'Filter reports by database name (optional)'
              },
              limit: {
                type: 'number',
                description: 'Maximum number of reports to return',
                default: 20
              }
            }
          }
        },
        {
          name: 'get_report',
          description: 'Get details of a specific migration report',
          inputSchema: {
            type: 'object',
            properties: {
              reportPath: {
                type: 'string',
                description: 'Path to the report directory'
              }
            },
            required: ['reportPath']
          }
        },
        {
          name: 'get_table_details',
          description: 'Get detailed information about a specific table in a report',
          inputSchema: {
            type: 'object',
            properties: {
              reportPath: {
                type: 'string',
                description: 'Path to the report directory'
              },
              tableName: {
                type: 'string',
                description: 'Name of the table'
              },
              environment: {
                type: 'string',
                description: 'Environment (LEFT or RIGHT)',
                enum: ['LEFT', 'RIGHT']
              }
            },
            required: ['reportPath', 'tableName', 'environment']
          }
        },
        {
          name: 'analyze_tables',
          description: 'Analyze tables in a database for migration readiness',
          inputSchema: {
            type: 'object',
            properties: {
              configPath: {
                type: 'string',
                description: 'Path to the configuration file'
              },
              database: {
                type: 'string',
                description: 'Database to analyze'
              }
            },
            required: ['configPath', 'database']
          }
        },
        {
          name: 'generate_distcp_script',
          description: 'Generate distcp commands for data migration',
          inputSchema: {
            type: 'object',
            properties: {
              reportPath: {
                type: 'string',
                description: 'Path to the report directory containing distcp plans'
              },
              environment: {
                type: 'string',
                description: 'Environment (LEFT or RIGHT)',
                enum: ['LEFT', 'RIGHT']
              }
            },
            required: ['reportPath', 'environment']
          }
        },
        {
          name: 'check_prerequisites',
          description: 'Check if HMS-Mirror prerequisites are met',
          inputSchema: {
            type: 'object',
            properties: {}
          }
        },
        {
          name: 'get_migration_status',
          description: 'Get the status of an ongoing or completed migration (web service only)',
          inputSchema: {
            type: 'object',
            properties: {
              sessionId: {
                type: 'string',
                description: 'Migration session ID'
              }
            },
            required: ['sessionId']
          }
        },
        {
          name: 'list_active_sessions',
          description: 'List active migration sessions (web service only)',
          inputSchema: {
            type: 'object',
            properties: {}
          }
        }
      ]
    }));

    this.server.setRequestHandler(CallToolRequestSchema, async (request) => {
      const { name, arguments: args } = request.params;

      try {
        switch (name) {
          case 'validate_config':
            return await this.validateConfig(args);
          case 'run_migration':
            return await this.runMigration(args);
          case 'list_reports':
            return await this.listReports(args);
          case 'get_report':
            return await this.getReport(args);
          case 'get_table_details':
            return await this.getTableDetails(args);
          case 'analyze_tables':
            return await this.analyzeTables(args);
          case 'generate_distcp_script':
            return await this.generateDistcpScript(args);
          case 'check_prerequisites':
            return await this.checkPrerequisites();
          case 'get_migration_status':
            return await this.getMigrationStatus(args);
          case 'list_active_sessions':
            return await this.listActiveSessions();
          default:
            throw new McpError(
              ErrorCode.MethodNotFound,
              `Unknown tool: ${name}`
            );
        }
      } catch (error) {
        if (error instanceof McpError) throw error;
        throw new McpError(
          ErrorCode.InternalError,
          `Tool execution failed: ${error}`
        );
      }
    });

    // Resource handlers
    this.server.setRequestHandler(ListResourcesRequestSchema, async () => ({
      resources: [
        {
          uri: 'hms://configs',
          name: 'HMS-Mirror Configurations',
          description: 'List of available configuration files',
          mimeType: 'application/json'
        },
        {
          uri: 'hms://reports',
          name: 'Migration Reports',
          description: 'List of migration reports',
          mimeType: 'application/json'
        },
        {
          uri: 'hms://strategies',
          name: 'Migration Strategies',
          description: 'Available migration strategies and their descriptions',
          mimeType: 'application/json'
        },
        {
          uri: 'hms://service/status',
          name: 'Service Status',
          description: 'Current status of HMS-Mirror service',
          mimeType: 'application/json'
        }
      ]
    }));

    this.server.setRequestHandler(ReadResourceRequestSchema, async (request) => {
      const { uri } = request.params;

      try {
        if (uri === 'hms://configs') {
          return await this.listConfigurations();
        } else if (uri === 'hms://reports') {
          return await this.listAllReports();
        } else if (uri === 'hms://strategies') {
          return await this.getStrategiesInfo();
        } else if (uri === 'hms://service/status') {
          return await this.getServiceStatus();
        } else if (uri.startsWith('hms://config/')) {
          const configName = uri.replace('hms://config/', '');
          return await this.getConfiguration(configName);
        } else if (uri.startsWith('hms://report/')) {
          const reportPath = uri.replace('hms://report/', '');
          return await this.getReportResource(reportPath);
        } else {
          throw new McpError(
            ErrorCode.InvalidRequest,
            `Unknown resource: ${uri}`
          );
        }
      } catch (error) {
        if (error instanceof McpError) throw error;
        throw new McpError(
          ErrorCode.InternalError,
          `Resource read failed: ${error}`
        );
      }
    });

    // Prompt handlers
    this.server.setRequestHandler(ListPromptsRequestSchema, async () => ({
      prompts: [
        {
          name: 'migration_plan',
          description: 'Generate a migration plan for a database',
          arguments: [
            {
              name: 'database',
              description: 'Database name to migrate',
              required: true
            },
            {
              name: 'strategy',
              description: 'Migration strategy to use',
              required: false
            }
          ]
        },
        {
          name: 'troubleshoot',
          description: 'Troubleshoot a failed migration',
          arguments: [
            {
              name: 'reportPath',
              description: 'Path to the failed migration report',
              required: true
            }
          ]
        },
        {
          name: 'optimize_config',
          description: 'Optimize a configuration for better performance',
          arguments: [
            {
              name: 'configPath',
              description: 'Path to the configuration file',
              required: true
            },
            {
              name: 'targetThroughput',
              description: 'Target throughput (tables/hour)',
              required: false
            }
          ]
        }
      ]
    }));

    this.server.setRequestHandler(GetPromptRequestSchema, async (request) => {
      const { name, arguments: args } = request.params;

      if (name === 'migration_plan') {
        const database = args?.database as string;
        const strategy = args?.strategy as string || 'HYBRID';
        return {
          description: `Migration plan for database ${database}`,
          messages: [
            {
              role: 'user',
              content: {
                type: 'text',
                text: `Create a migration plan for the database "${database}" using the ${strategy} strategy. Include:
1. Pre-migration checklist
2. Table analysis and categorization
3. Recommended strategy per table type
4. Data validation steps
5. Rollback procedures`
              }
            }
          ]
        };
      } else if (name === 'troubleshoot') {
        const reportPath = args?.reportPath as string;
        const report = await this.loadReport(reportPath);
        return {
          description: `Troubleshooting guide for migration report`,
          messages: [
            {
              role: 'user',
              content: {
                type: 'text',
                text: `Analyze this failed migration and provide troubleshooting steps:\n\n${JSON.stringify(report, null, 2)}`
              }
            }
          ]
        };
      } else if (name === 'optimize_config') {
        const configPath = args?.configPath as string;
        const targetThroughput = (args?.targetThroughput as unknown as number) || 100;
        const config = await fs.readFile(configPath, 'utf-8');
        return {
          description: `Configuration optimization recommendations`,
          messages: [
            {
              role: 'user',
              content: {
                type: 'text',
                text: `Optimize this HMS-Mirror configuration for ${targetThroughput} tables/hour throughput:\n\n${config}\n\nProvide specific recommendations for connection pooling, parallelism, and strategy selection.`
              }
            }
          ]
        };
      }

      throw new McpError(
        ErrorCode.InvalidRequest,
        `Unknown prompt: ${name}`
      );
    });
  }

  // Tool implementations with web service support
  private async validateConfig(args: any) {
    const { configPath, configContent } = args;
    
    // Try web service first if available
    if (this.shouldUseWebService()) {
      try {
        let config;
        if (configContent) {
          config = yaml.load(configContent);
        } else {
          const content = await fs.readFile(configPath, 'utf-8');
          config = yaml.load(content);
        }

        const response = await this.httpClient.post('/api/config/validate', config);
        
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify(response.data, null, 2)
            }
          ]
        };
      } catch (error: any) {
        if (this.integrationMode === IntegrationMode.WEB_SERVICE_ONLY) {
          throw error;
        }
        // Fall back to CLI/local validation
      }
    }

    // CLI/local validation
    try {
      const configContent = await fs.readFile(configPath, 'utf-8');
      const config = yaml.load(configContent) as any;
      
      // Basic validation
      const errors: string[] = [];
      const warnings: string[] = [];
      
      if (!config.clusters?.LEFT) {
        errors.push('Missing LEFT cluster configuration');
      }
      if (!config.clusters?.RIGHT) {
        errors.push('Missing RIGHT cluster configuration');
      }
      if (!config.dataStrategy) {
        warnings.push('No data strategy specified, will use default');
      }
      
      // Check connection settings
      if (config.clusters?.LEFT?.hiveServer2 && !config.clusters.LEFT.hiveServer2.uri) {
        errors.push('LEFT cluster HiveServer2 URI is missing');
      }
      if (config.clusters?.RIGHT?.hiveServer2 && !config.clusters.RIGHT.hiveServer2.uri) {
        errors.push('RIGHT cluster HiveServer2 URI is missing');
      }
      
      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify({
              valid: errors.length === 0,
              errors,
              warnings,
              config: {
                dataStrategy: config.dataStrategy,
                leftCluster: config.clusters?.LEFT?.environment,
                rightCluster: config.clusters?.RIGHT?.environment,
                databases: config.databases || []
              }
            }, null, 2)
          }
        ]
      };
    } catch (error) {
      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify({
              valid: false,
              error: `Failed to parse configuration: ${error}`
            }, null, 2)
          }
        ]
      };
    }
  }

  private async runMigration(args: any) {
    const { configPath, database, dryRun, strategy, async } = args;
    
    // Try web service first if available
    if (this.shouldUseWebService()) {
      try {
        const config = await fs.readFile(configPath, 'utf-8');
        const configObj = yaml.load(config) as any;
        
        // Override settings if provided
        if (database) {
          configObj.databases = [database];
        }
        if (strategy) {
          configObj.dataStrategy = strategy;
        }
        if (dryRun !== undefined) {
          configObj.dryRun = dryRun;
        }

        // Start migration via web service
        const response = await this.httpClient.post('/api/migration/start', {
          config: configObj,
          async: async || false
        });

        if (async) {
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify({
                  success: true,
                  sessionId: response.data.sessionId,
                  message: 'Migration started asynchronously',
                  status: response.data.status
                }, null, 2)
              }
            ]
          };
        }

        // Wait for completion if synchronous
        let status = response.data;
        while (status.state === 'RUNNING') {
          await new Promise(resolve => setTimeout(resolve, 2000));
          const statusResponse = await this.httpClient.get(`/api/migration/status/${status.sessionId}`);
          status = statusResponse.data;
        }

        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({
                success: status.state === 'COMPLETED',
                sessionId: status.sessionId,
                reportPath: status.reportPath,
                summary: status.summary,
                errors: status.errors
              }, null, 2)
            }
          ]
        };
      } catch (error: any) {
        if (this.integrationMode === IntegrationMode.WEB_SERVICE_ONLY) {
          throw error;
        }
        // Fall back to CLI
      }
    }

    // CLI execution
    let command = `${HMS_MIRROR_BIN} -cfg ${configPath}`;
    if (database) command += ` -db ${database}`;
    if (dryRun) command += ` -dr`;
    if (strategy) command += ` -ds ${strategy}`;
    
    try {
      const { stdout, stderr } = await execAsync(command, {
        cwd: HMS_MIRROR_HOME,
        maxBuffer: 10 * 1024 * 1024 // 10MB buffer
      });
      
      // Parse output to find report location
      const reportMatch = stdout.match(/Report saved to: (.+)/);
      const reportPath = reportMatch ? reportMatch[1] : null;
      
      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify({
              success: !stderr.includes('ERROR'),
              reportPath,
              output: stdout.substring(0, 5000), // First 5000 chars
              errors: stderr
            }, null, 2)
          }
        ]
      };
    } catch (error: any) {
      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify({
              success: false,
              error: error.message,
              stdout: error.stdout?.substring(0, 5000),
              stderr: error.stderr
            }, null, 2)
          }
        ]
      };
    }
  }

  private async listReports(args: any) {
    const { database, limit = 20 } = args;
    
    // Try web service first if available
    if (this.shouldUseWebService()) {
      try {
        const response = await this.httpClient.get('/api/reports', {
          params: { database, limit }
        });
        
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify(response.data, null, 2)
            }
          ]
        };
      } catch (error) {
        if (this.integrationMode === IntegrationMode.WEB_SERVICE_ONLY) {
          throw error;
        }
        // Fall back to file system
      }
    }

    // File system approach
    try {
      const reportDirs = await glob('**/run-status.yaml', {
        cwd: REPORTS_DIR,
        maxDepth: 5
      });
      
      const reports = [];
      for (const reportDir of reportDirs.slice(0, limit)) {
        const dir = path.dirname(reportDir);
        const statusPath = path.join(REPORTS_DIR, reportDir);
        
        try {
          const statusContent = await fs.readFile(statusPath, 'utf-8');
          const status = yaml.load(statusContent) as any;
          
          if (!database || status.databases?.includes(database)) {
            reports.push({
              path: dir,
              timestamp: status.timestamp,
              databases: status.databases,
              status: status.status,
              totalTables: status.totalTables,
              successfulTables: status.successfulTables
            });
          }
        } catch (err) {
          // Skip invalid reports
        }
      }
      
      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify({
              count: reports.length,
              reports: reports.sort((a, b) => 
                new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime()
              )
            }, null, 2)
          }
        ]
      };
    } catch (error) {
      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify({
              error: `Failed to list reports: ${error}`
            }, null, 2)
          }
        ]
      };
    }
  }

  private async getReport(args: any) {
    const { reportPath } = args;
    
    // Try web service first if available
    if (this.shouldUseWebService()) {
      try {
        const response = await this.httpClient.get('/api/reports/details', {
          params: { path: reportPath }
        });
        
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify(response.data, null, 2)
            }
          ]
        };
      } catch (error) {
        if (this.integrationMode === IntegrationMode.WEB_SERVICE_ONLY) {
          throw error;
        }
        // Fall back to file system
      }
    }

    // File system approach
    try {
      const report = await this.loadReport(reportPath);
      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify(report, null, 2)
          }
        ]
      };
    } catch (error) {
      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify({
              error: `Failed to load report: ${error}`
            }, null, 2)
          }
        ]
      };
    }
  }

  private async getTableDetails(args: any) {
    const { reportPath, tableName, environment } = args;
    
    // Try web service first if available
    if (this.shouldUseWebService()) {
      try {
        const response = await this.httpClient.get('/api/reports/table-details', {
          params: { path: reportPath, table: tableName, environment }
        });
        
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify(response.data, null, 2)
            }
          ]
        };
      } catch (error) {
        if (this.integrationMode === IntegrationMode.WEB_SERVICE_ONLY) {
          throw error;
        }
      }
    }

    // File system approach - read table YAML file
    try {
      const tableFile = path.join(REPORTS_DIR, reportPath, `${tableName}.${environment}.yaml`);
      const tableContent = await fs.readFile(tableFile, 'utf-8');
      const tableData = yaml.load(tableContent);
      
      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify(tableData, null, 2)
          }
        ]
      };
    } catch (error) {
      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify({
              error: `Failed to load table details: ${error}`
            }, null, 2)
          }
        ]
      };
    }
  }

  private async getMigrationStatus(args: any) {
    const { sessionId } = args;
    
    if (!this.webServiceAvailable) {
      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify({
              error: 'Web service not available. This operation requires HMS-Mirror web service.'
            }, null, 2)
          }
        ]
      };
    }

    try {
      const response = await this.httpClient.get(`/api/migration/status/${sessionId}`);
      
      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify(response.data, null, 2)
          }
        ]
      };
    } catch (error: any) {
      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify({
              error: `Failed to get migration status: ${error.message}`
            }, null, 2)
          }
        ]
      };
    }
  }

  private async listActiveSessions() {
    if (!this.webServiceAvailable) {
      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify({
              error: 'Web service not available. This operation requires HMS-Mirror web service.'
            }, null, 2)
          }
        ]
      };
    }

    try {
      const response = await this.httpClient.get('/api/migration/sessions/active');
      
      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify(response.data, null, 2)
          }
        ]
      };
    } catch (error: any) {
      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify({
              error: `Failed to list active sessions: ${error.message}`
            }, null, 2)
          }
        ]
      };
    }
  }

  private async analyzeTables(args: any) {
    const { configPath, database } = args;
    
    // Run HMS-Mirror in analysis mode
    const command = `${HMS_MIRROR_BIN} -cfg ${configPath} -db ${database} -dr -ro`;
    
    try {
      const { stdout } = await execAsync(command, {
        cwd: HMS_MIRROR_HOME,
        maxBuffer: 10 * 1024 * 1024
      });
      
      // Parse analysis output
      const analysis = {
        database,
        tables: [] as any[],
        recommendations: [] as string[]
      };
      
      // Extract table information from output
      const tableMatches = stdout.matchAll(/Table: (\S+)\s+Type: (\S+)\s+Strategy: (\S+)/g);
      for (const match of tableMatches) {
        analysis.tables.push({
          name: match[1],
          type: match[2],
          recommendedStrategy: match[3]
        });
      }
      
      // Add recommendations based on table types
      if (analysis.tables.some(t => t.type === 'ACID')) {
        analysis.recommendations.push('ACID tables detected - consider using HYBRID or SQL strategy');
      }
      if (analysis.tables.some(t => t.type === 'EXTERNAL')) {
        analysis.recommendations.push('External tables can use LINKED strategy for faster migration');
      }
      
      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify(analysis, null, 2)
          }
        ]
      };
    } catch (error: any) {
      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify({
              error: `Analysis failed: ${error.message}`
            }, null, 2)
          }
        ]
      };
    }
  }

  private async generateDistcpScript(args: any) {
    const { reportPath, environment } = args;
    
    try {
      const distcpPlanPath = path.join(
        REPORTS_DIR,
        reportPath,
        `*_${environment}_distcp_plans.yaml`
      );
      
      const files = await glob(distcpPlanPath);
      if (files.length === 0) {
        throw new Error(`No distcp plan found for ${environment} environment`);
      }
      
      const planContent = await fs.readFile(files[0], 'utf-8');
      const plan = yaml.load(planContent) as any;
      
      // Generate distcp commands
      const commands: string[] = [];
      for (const [, targets] of Object.entries(plan)) {
        for (const [target, sources] of Object.entries(targets as any)) {
          for (const source of sources as string[]) {
            commands.push(`hadoop distcp -pb -update -skipcrccheck "${source}" "${target}"`);
          }
        }
      }
      
      // Create shell script
      const script = `#!/bin/bash
# HMS-Mirror DistCp Script
# Generated from: ${reportPath}
# Environment: ${environment}
# Date: ${new Date().toISOString()}

set -e

echo "Starting distcp operations..."

${commands.join('\n')}

echo "Distcp operations completed successfully"
`;
      
      return {
        content: [
          {
            type: 'text',
            text: script
          }
        ]
      };
    } catch (error) {
      return {
        content: [
          {
            type: 'text',
            text: `Error generating distcp script: ${error}`
          }
        ]
      };
    }
  }

  private async checkPrerequisites() {
    const checks = {
      hmsMirrorInstalled: false,
      javaVersion: '',
      configsDirectory: false,
      reportsDirectory: false,
      webServiceAvailable: this.webServiceAvailable,
      integrationMode: this.integrationMode
    };
    
    try {
      // Check HMS-Mirror installation
      await fs.access(HMS_MIRROR_BIN);
      checks.hmsMirrorInstalled = true;
      
      // Check Java version
      const { stdout: javaOut } = await execAsync('java -version 2>&1');
      const versionMatch = javaOut.match(/version "(\d+)\.(\d+)/);
      if (versionMatch) {
        checks.javaVersion = versionMatch[1] === '1' ? versionMatch[2] : versionMatch[1];
      }
      
      // Check directories
      try {
        await fs.access(CONFIGS_DIR);
        checks.configsDirectory = true;
      } catch {}
      
      try {
        await fs.access(REPORTS_DIR);
        checks.reportsDirectory = true;
      } catch {}
      
    } catch (error) {
      // Some checks may fail
    }
    
    return {
      content: [
        {
          type: 'text',
          text: JSON.stringify({
            ...checks,
            javaVersionOk: parseInt(checks.javaVersion) >= 17,
            allChecksPassed: checks.hmsMirrorInstalled && 
                           parseInt(checks.javaVersion) >= 17 &&
                           checks.configsDirectory &&
                           checks.reportsDirectory
          }, null, 2)
        }
      ]
    };
  }

  // Resource implementations
  private async listConfigurations() {
    try {
      const configs = await glob('*.yaml', { cwd: CONFIGS_DIR });
      const configList = [];
      
      for (const configFile of configs) {
        const configPath = path.join(CONFIGS_DIR, configFile);
        const content = await fs.readFile(configPath, 'utf-8');
        const config = yaml.load(content) as any;
        
        configList.push({
          name: configFile,
          dataStrategy: config.dataStrategy,
          leftCluster: config.clusters?.LEFT?.environment,
          rightCluster: config.clusters?.RIGHT?.environment
        });
      }
      
      return {
        contents: [
          {
            uri: 'hms://configs',
            mimeType: 'application/json',
            text: JSON.stringify(configList, null, 2)
          }
        ]
      };
    } catch (error) {
      throw new McpError(
        ErrorCode.InternalError,
        `Failed to list configurations: ${error}`
      );
    }
  }

  private async listAllReports() {
    const args = { limit: 50 };
    const result = await this.listReports(args);
    return {
      contents: [
        {
          uri: 'hms://reports',
          mimeType: 'application/json',
          text: result.content[0].text
        }
      ]
    };
  }

  private async getStrategiesInfo() {
    const strategies = {
      SCHEMA_ONLY: {
        description: 'Migrates only table schemas without data',
        useCases: ['Testing', 'Schema validation', 'Structure migration'],
        supportsACID: false,
        requiresDistcp: false
      },
      LINKED: {
        description: 'Creates external tables pointing to original data locations',
        useCases: ['Read-only access', 'Quick migration', 'No data copy'],
        supportsACID: false,
        requiresDistcp: false
      },
      SQL: {
        description: 'Uses SQL export/import for data transfer',
        useCases: ['Small datasets', 'ACID tables', 'Schema changes'],
        supportsACID: true,
        requiresDistcp: false
      },
      EXPORT_IMPORT: {
        description: 'Uses Hive EXPORT/IMPORT commands',
        useCases: ['Complete table migration', 'Metadata preservation'],
        supportsACID: true,
        requiresDistcp: true
      },
      HYBRID: {
        description: 'Combines strategies based on table type',
        useCases: ['Mixed workloads', 'Optimized migration', 'Large databases'],
        supportsACID: true,
        requiresDistcp: true
      },
      STORAGE_MIGRATION: {
        description: 'Migrates data between storage systems',
        useCases: ['Storage upgrade', 'Format conversion'],
        supportsACID: false,
        requiresDistcp: true
      },
      COMMON: {
        description: 'Shared storage migration strategy',
        useCases: ['Shared filesystem', 'In-place upgrade'],
        supportsACID: true,
        requiresDistcp: false
      }
    };
    
    return {
      contents: [
        {
          uri: 'hms://strategies',
          mimeType: 'application/json',
          text: JSON.stringify(strategies, null, 2)
        }
      ]
    };
  }

  private async getServiceStatus() {
    const status: any = {
      webServiceAvailable: this.webServiceAvailable,
      webServiceUrl: WEB_SERVICE_URL,
      integrationMode: this.integrationMode,
      hmsMirrorHome: HMS_MIRROR_HOME,
      timestamp: new Date().toISOString()
    };

    if (this.webServiceAvailable) {
      try {
        const response = await this.httpClient.get('/api/health');
        status.serviceHealth = response.data;
      } catch {}
    }

    return {
      contents: [
        {
          uri: 'hms://service/status',
          mimeType: 'application/json',
          text: JSON.stringify(status, null, 2)
        }
      ]
    };
  }

  private async getConfiguration(configName: string) {
    try {
      const configPath = path.join(CONFIGS_DIR, configName);
      const content = await fs.readFile(configPath, 'utf-8');
      
      return {
        contents: [
          {
            uri: `hms://config/${configName}`,
            mimeType: 'text/yaml',
            text: content
          }
        ]
      };
    } catch (error) {
      throw new McpError(
        ErrorCode.InvalidRequest,
        `Configuration not found: ${configName}`
      );
    }
  }

  private async getReportResource(reportPath: string) {
    try {
      const report = await this.loadReport(reportPath);
      
      return {
        contents: [
          {
            uri: `hms://report/${reportPath}`,
            mimeType: 'application/json',
            text: JSON.stringify(report, null, 2)
          }
        ]
      };
    } catch (error) {
      throw new McpError(
        ErrorCode.InvalidRequest,
        `Report not found: ${reportPath}`
      );
    }
  }

  // Helper methods
  private shouldUseWebService(): boolean {
    return this.integrationMode !== IntegrationMode.CLI_ONLY && this.webServiceAvailable;
  }

  private async loadReport(reportPath: string) {
    const fullPath = path.join(REPORTS_DIR, reportPath);
    const report: any = {
      path: reportPath,
      files: []
    };
    
    // Load run-status.yaml
    try {
      const statusPath = path.join(fullPath, 'run-status.yaml');
      const statusContent = await fs.readFile(statusPath, 'utf-8');
      report.status = yaml.load(statusContent);
    } catch {}
    
    // Load session-config.yaml
    try {
      const configPath = path.join(fullPath, 'session-config.yaml');
      const configContent = await fs.readFile(configPath, 'utf-8');
      report.config = yaml.load(configContent);
    } catch {}
    
    // List all files in report directory
    try {
      const files = await fs.readdir(fullPath);
      report.files = files;
      
      // Count SQL files
      report.sqlFiles = files.filter(f => f.endsWith('.sql')).length;
      
      // Check for distcp plans
      report.hasLeftDistcpPlan = files.some(f => f.includes('LEFT_distcp_plans.yaml'));
      report.hasRightDistcpPlan = files.some(f => f.includes('RIGHT_distcp_plans.yaml'));
    } catch {}
    
    return report;
  }

  async start() {
    await this.server.connect(this.transport);
    console.error(`HMS-Mirror MCP server started (v2.0.0)`);
    console.error(`Integration mode: ${this.integrationMode}`);
    console.error(`Web service available: ${this.webServiceAvailable}`);
  }
}

// Start the server
const server = new HmsMirrorMcpServer();
server.start().catch((error) => {
  console.error('Failed to start HMS-Mirror MCP server:', error);
  process.exit(1);
});