// Connection Management Types

export interface HiveServer2Config {
  uri: string;
  connectionProperties: {
    user?: string;
    password?: string;
    [key: string]: string | undefined;
  };
  driverClassName: string;
  jarFile: string;
  disconnected: boolean;
}

export interface MetastoreDirectConfig {
  uri: string;
  type: 'MYSQL' | 'POSTGRES' | 'ORACLE';
  connectionProperties: {
    user: string;
    password: string;
    [key: string]: string | undefined;
  };
  connectionPool: {
    min: number;
    max: number;
  };
}

export interface PartitionDiscoveryConfig {
  auto: boolean;
  initMSCK: boolean;
  partitionBucketLimit: number;
}

export interface ConnectionConfig {
  // Platform configuration
  platformType: 'HDP2' | 'HDP3' | 'CDP7_1' | 'CDP7_2' | 'CDH5' | 'CDH6' | 'APACHE' | 'EMR' | 'GENERIC';
  
  // Core configuration
  hcfsNamespace: string;
  
  // HiveServer2 configuration
  hiveServer2: HiveServer2Config;
  
  // Optional metastore direct configuration (for optimization)
  metastoreDirect?: MetastoreDirectConfig;
  
  // Connection pool library preference
  connectionPoolLib: 'DBCP2' | 'HIKARI' | 'HYBRID';
  
  // Partition discovery settings
  partitionDiscovery?: PartitionDiscoveryConfig;
  
  // Additional cluster settings
  createIfNotExists?: boolean;
  enableAutoTableStats?: boolean;
  enableAutoColumnStats?: boolean;
}

export interface TestResult {
  component: 'HDFS' | 'HIVESERVER2' | 'METASTORE';
  status: 'SUCCESS' | 'FAILED' | 'TESTING' | 'PENDING';
  message?: string;
  responseTime?: number;
  details?: Record<string, any>;
}

export interface ConnectionTestResults {
  status: 'SUCCESS' | 'FAILED' | 'NEVER_TESTED' | 'TESTING';
  lastTested?: string;
  duration?: number;
  results?: TestResult[];
  logs?: string[];
}

export interface Connection {
  id: string;
  name: string;
  description?: string;
  environment?: 'DEV' | 'TEST' | 'PROD' | 'UAT' | 'STAGING';
  config: ConnectionConfig;
  testResults?: ConnectionTestResults;
  created: string;
  modified: string;
  isDefault: boolean;
}

export interface ConnectionFormData {
  // Basic Information
  name: string;
  description: string;
  environment: 'DEV' | 'TEST' | 'PROD' | 'UAT' | 'STAGING' | '';
  
  // Platform Configuration
  platformType: 'HDP2' | 'HDP3' | 'CDP7_1' | 'CDP7_2' | 'CDH5' | 'CDH6' | 'APACHE' | 'EMR' | 'GENERIC' | '';
  hcfsNamespace: string;
  
  // HiveServer2 Configuration
  hs2Uri: string;
  hs2Username: string;
  hs2Password: string;
  hs2DriverClassName: string;
  hs2JarFile: string;
  hs2Disconnected: boolean;
  hs2ConnectionProperties: { [key: string]: string };
  
  // Metastore Direct Configuration (optional)
  metastoreDirectEnabled: boolean;
  metastoreDirectUri: string;
  metastoreDirectType: 'MYSQL' | 'POSTGRES' | 'ORACLE' | '';
  metastoreDirectUsername: string;
  metastoreDirectPassword: string;
  metastoreDirectMinConnections: number;
  metastoreDirectMaxConnections: number;
  
  // Connection Pool Configuration
  connectionPoolLib: 'DBCP2' | 'HIKARI' | 'HYBRID';
  
  // Partition Discovery Configuration
  partitionDiscoveryAuto: boolean;
  partitionDiscoveryInitMSCK: boolean;
  partitionBucketLimit: number;
  
  // Additional Settings
  createIfNotExists: boolean;
  enableAutoTableStats: boolean;
  enableAutoColumnStats: boolean;
}

export interface ConnectionWizardStep {
  id: string;
  title: string;
  isValid: (data: Partial<ConnectionFormData>) => boolean;
  isComplete: (data: Partial<ConnectionFormData>) => boolean;
}

export interface ConnectionListFilters {
  search: string;
  environment: string;
  status: 'all' | 'success' | 'failed' | 'never_tested';
}

export interface ConnectionBulkAction {
  type: 'test' | 'delete' | 'export';
  connectionIds: string[];
}

// API Response types
export interface ConnectionListResponse {
  connections: Connection[];
  total: number;
}

export interface ConnectionTestResponse {
  testId: string;
  status: 'STARTED' | 'IN_PROGRESS' | 'COMPLETED' | 'FAILED';
  results?: ConnectionTestResults;
}

// Form validation
export interface ValidationResult {
  isValid: boolean;
  errors: Record<string, string>;
}

// Constants
export const ENVIRONMENT_OPTIONS = [
  { value: 'DEV', label: 'Development' },
  { value: 'TEST', label: 'Testing' },
  { value: 'PROD', label: 'Production' },
  { value: 'UAT', label: 'User Acceptance Testing' },
  { value: 'STAGING', label: 'Staging' }
];

export const PLATFORM_TYPE_OPTIONS = [
  { value: 'HDP2', label: 'Hortonworks HDP 2.x' },
  { value: 'HDP3', label: 'Hortonworks HDP 3.x' },
  { value: 'CDP7_1', label: 'Cloudera CDP 7.1' },
  { value: 'CDP7_2', label: 'Cloudera CDP 7.2+' },
  { value: 'CDH5', label: 'Cloudera CDH 5.x' },
  { value: 'CDH6', label: 'Cloudera CDH 6.x' },
  { value: 'APACHE', label: 'Apache Hive' },
  { value: 'EMR', label: 'Amazon EMR' },
  { value: 'GENERIC', label: 'Generic/Other' }
];

export const POOL_TYPE_OPTIONS = [
  { value: 'DBCP2', label: 'DBCP2 (Default)' },
  { value: 'HIKARI', label: 'HikariCP' },
  { value: 'HYBRID', label: 'Hybrid' }
];

export const METASTORE_DB_TYPE_OPTIONS = [
  { value: 'MYSQL', label: 'MySQL / MariaDB' },
  { value: 'POSTGRES', label: 'PostgreSQL' },
  { value: 'ORACLE', label: 'Oracle Database' }
];

export const HIVE_DRIVER_OPTIONS = [
  { value: 'org.apache.hive.jdbc.HiveDriver', label: 'Apache Hive JDBC Driver (org.apache.hive.jdbc.HiveDriver)' },
  { value: 'com.cloudera.hive.jdbc.HS2Driver', label: 'Cloudera Hive JDBC Driver (com.cloudera.hive.jdbc.HS2Driver)' }
];

// Connection Examples
export const HIVESERVER2_CONNECTION_EXAMPLES = {
  kerberos: {
    uri: 'jdbc:hive2://hiveserver2.example.com:10000/default;principal=hive/_HOST@REALM.COM',
    description: 'Kerberos authentication using service principal'
  },
  userPassword: {
    uri: 'jdbc:hive2://hiveserver2.example.com:10000/default',
    description: 'Username/password authentication'
  },
  knox: {
    uri: 'jdbc:hive2://knox-gateway.example.com:8443/;ssl=true;sslTrustStore=/path/to/truststore.jks;trustStorePassword=password;transportMode=http;httpPath=gateway/cdp-proxy-api/hive',
    description: 'Knox Gateway with SSL'
  },
  zookeeper: {
    uri: 'jdbc:hive2://zk1.example.com:2181,zk2.example.com:2181,zk3.example.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2',
    description: 'ZooKeeper service discovery'
  }
};

export const METASTORE_CONNECTION_EXAMPLES = {
  mysql: {
    uri: 'jdbc:mysql://mysql-server.example.com:3306/hive_metastore?useSSL=false&allowPublicKeyRetrieval=true',
    description: 'MySQL metastore database connection'
  },
  postgres: {
    uri: 'jdbc:postgresql://postgres-server.example.com:5432/hive_metastore',
    description: 'PostgreSQL metastore database connection'
  },
  oracle: {
    uri: 'jdbc:oracle:thin:@oracle-server.example.com:1521:XE',
    description: 'Oracle metastore database connection'
  }
};

export const DEFAULT_CONNECTION_FORM: ConnectionFormData = {
  // Basic Information
  name: '',
  description: '',
  environment: '',
  
  // Platform Configuration
  platformType: '',
  hcfsNamespace: '',
  
  // HiveServer2 Configuration
  hs2Uri: '',
  hs2Username: '',
  hs2Password: '',
  hs2DriverClassName: 'org.apache.hive.jdbc.HiveDriver',
  hs2JarFile: '',
  hs2Disconnected: false,
  hs2ConnectionProperties: {},
  
  // Metastore Direct Configuration (optional)
  metastoreDirectEnabled: false,
  metastoreDirectUri: '',
  metastoreDirectType: '',
  metastoreDirectUsername: '',
  metastoreDirectPassword: '',
  metastoreDirectMinConnections: 3,
  metastoreDirectMaxConnections: 5,
  
  // Connection Pool Configuration
  connectionPoolLib: 'DBCP2',
  
  // Partition Discovery Configuration
  partitionDiscoveryAuto: true,
  partitionDiscoveryInitMSCK: true,
  partitionBucketLimit: 100,
  
  // Additional Settings
  createIfNotExists: false,
  enableAutoTableStats: false,
  enableAutoColumnStats: false
};

// Utility functions
export const getConnectionStatusColor = (connection: Connection): string => {
  if (!connection.testResults || connection.testResults.status === 'NEVER_TESTED') {
    return 'text-yellow-600';
  }
  
  const daysSinceTest = connection.testResults.lastTested 
    ? Math.floor((Date.now() - new Date(connection.testResults.lastTested).getTime()) / (1000 * 60 * 60 * 24))
    : Infinity;
    
  if (connection.testResults.status === 'SUCCESS' && daysSinceTest <= 7) {
    return 'text-green-600';
  } else if (connection.testResults.status === 'FAILED') {
    return 'text-red-600';
  } else {
    return 'text-yellow-600';
  }
};

export const getConnectionStatusIcon = (connection: Connection): string => {
  if (!connection.testResults || connection.testResults.status === 'NEVER_TESTED') {
    return '🟡';
  }
  
  const daysSinceTest = connection.testResults.lastTested 
    ? Math.floor((Date.now() - new Date(connection.testResults.lastTested).getTime()) / (1000 * 60 * 60 * 24))
    : Infinity;
    
  if (connection.testResults.status === 'SUCCESS' && daysSinceTest <= 7) {
    return '🟢';
  } else if (connection.testResults.status === 'FAILED') {
    return '🔴';
  } else {
    return '🟡';
  }
};

export const validateConnectionForm = (data: Partial<ConnectionFormData>): ValidationResult => {
  const errors: Record<string, string> = {};
  
  if (!data.name?.trim()) {
    errors.name = 'Connection name is required';
  }
  
  // HDFS namespace is optional, but if provided must be valid
  if (data.hcfsNamespace?.trim() && !data.hcfsNamespace.match(/^(hdfs|s3a?|adls|wasb|ofs|gf|viewfs|maprfs|gs):\/\//)) {
    errors.hcfsNamespace = 'HDFS namespace must start with a valid filesystem protocol (hdfs://, s3a://, etc.)';
  }
  
  if (!data.platformType?.trim()) {
    errors.platformType = 'Platform type is required';
  }
  
  // HiveServer2 validation (unless disconnected)
  if (!data.hs2Disconnected) {
    if (!data.hs2Uri?.trim()) {
      errors.hs2Uri = 'HiveServer2 URI is required';
    } else if (!data.hs2Uri.startsWith('jdbc:hive2://')) {
      errors.hs2Uri = 'HiveServer2 URI must start with jdbc:hive2://';
    }
    
    if (!data.hs2DriverClassName?.trim()) {
      errors.hs2DriverClassName = 'Driver class name is required';
    }
    
    if (!data.hs2JarFile?.trim()) {
      errors.hs2JarFile = 'JAR file path is required';
    }
  }
  
  // Metastore Direct validation (if enabled)
  if (data.metastoreDirectEnabled) {
    if (!data.metastoreDirectUri?.trim()) {
      errors.metastoreDirectUri = 'Metastore database URI is required';
    } else if (!data.metastoreDirectUri.startsWith('jdbc:')) {
      errors.metastoreDirectUri = 'Metastore URI must be a valid JDBC URL';
    }
    
    if (!data.metastoreDirectType?.trim()) {
      errors.metastoreDirectType = 'Database type is required';
    }
    
    if (!data.metastoreDirectUsername?.trim()) {
      errors.metastoreDirectUsername = 'Database username is required';
    }
    
    if (!data.metastoreDirectPassword?.trim()) {
      errors.metastoreDirectPassword = 'Database password is required';
    }
  }
  
  return {
    isValid: Object.keys(errors).length === 0,
    errors
  };
};

// Step-specific validation functions
export const validateBasicInfoStep = (data: Partial<ConnectionFormData>): ValidationResult => {
  const errors: Record<string, string> = {};
  
  if (!data.name?.trim()) {
    errors.name = 'Connection name is required';
  }
  
  if (!data.platformType?.trim()) {
    errors.platformType = 'Platform type is required';
  }
  
  return {
    isValid: Object.keys(errors).length === 0,
    errors
  };
};

export const validateConnectionSettingsStep = (data: Partial<ConnectionFormData>): ValidationResult => {
  const errors: Record<string, string> = {};
  
  // HDFS Namespace validation (optional, but if provided must be valid)
  if (data.hcfsNamespace?.trim() && !data.hcfsNamespace.match(/^(hdfs|s3a?|adls|wasb|ofs|gf|viewfs|maprfs|gs):\/\//)) {
    errors.hcfsNamespace = 'HDFS namespace must start with a valid filesystem protocol (hdfs://, s3a://, etc.)';
  }
  
  // HiveServer2 validation
  if (!data.hs2Disconnected) {
    if (!data.hs2Uri?.trim()) {
      errors.hs2Uri = 'HiveServer2 URI is required';
    } else if (!data.hs2Uri.startsWith('jdbc:hive2://')) {
      errors.hs2Uri = 'HiveServer2 URI must start with jdbc:hive2://';
    }
    
    if (!data.hs2DriverClassName?.trim()) {
      errors.hs2DriverClassName = 'Driver class name is required';
    }
    
    if (!data.hs2JarFile?.trim()) {
      errors.hs2JarFile = 'JAR file path is required';
    }
  }
  
  // Metastore Direct validation (if enabled)
  if (data.metastoreDirectEnabled) {
    if (!data.metastoreDirectUri?.trim()) {
      errors.metastoreDirectUri = 'Metastore database URI is required';
    } else if (!data.metastoreDirectUri.startsWith('jdbc:')) {
      errors.metastoreDirectUri = 'Metastore URI must be a valid JDBC URL (jdbc:mysql://, jdbc:postgresql://, etc.)';
    }
    
    if (!data.metastoreDirectType?.trim()) {
      errors.metastoreDirectType = 'Database type is required';
    }
    
    if (!data.metastoreDirectUsername?.trim()) {
      errors.metastoreDirectUsername = 'Database username is required';
    }
    
    if (!data.metastoreDirectPassword?.trim()) {
      errors.metastoreDirectPassword = 'Database password is required';
    }
  }
  
  return {
    isValid: Object.keys(errors).length === 0,
    errors
  };
};

export const validateConnectionPoolStep = (data: Partial<ConnectionFormData>): ValidationResult => {
  const errors: Record<string, string> = {};
  
  // Metastore Direct pool validation (if enabled)
  if (data.metastoreDirectEnabled) {
    if (!data.metastoreDirectMinConnections || data.metastoreDirectMinConnections < 1 || data.metastoreDirectMinConnections > 20) {
      errors.metastoreDirectMinConnections = 'Min connections must be between 1 and 20';
    }
    
    if (!data.metastoreDirectMaxConnections || data.metastoreDirectMaxConnections < 1 || data.metastoreDirectMaxConnections > 50) {
      errors.metastoreDirectMaxConnections = 'Max connections must be between 1 and 50';
    }
    
    if (data.metastoreDirectMinConnections && data.metastoreDirectMaxConnections && 
        data.metastoreDirectMinConnections > data.metastoreDirectMaxConnections) {
      errors.metastoreDirectMinConnections = 'Min connections cannot be greater than max connections';
    }
  }
  
  // Partition discovery validation
  if (data.partitionBucketLimit && (data.partitionBucketLimit < 1 || data.partitionBucketLimit > 10000)) {
    errors.partitionBucketLimit = 'Partition bucket limit must be between 1 and 10000';
  }
  
  return {
    isValid: Object.keys(errors).length === 0,
    errors
  };
};