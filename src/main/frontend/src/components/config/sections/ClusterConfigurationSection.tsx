import React, { useState, useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { useConfiguration } from '../../../contexts/ConfigurationContext';
import { ArrowLeftIcon, CheckIcon, ChevronDownIcon, ChevronRightIcon, PlusIcon, XMarkIcon, EyeIcon, EyeSlashIcon } from '@heroicons/react/24/outline';

interface HiveServer2Config {
  uri: string | null;
  disconnected: boolean;
  connectionProperties: Record<string, string>;
  driverClassName: string;
  jarFile: string | null;
  version: string | null;
}

interface MetastoreDirectConfig {
  uri: string | null;
  type: 'MYSQL' | 'POSTGRES' | 'ORACLE';
  connectionProperties: Record<string, string>;
  initSql: string | null;
  resource: string | null;
  version: string | null;
  connectionPool: {
    min: number;
    max: number;
    timeout: number;
  };
}

interface ClusterConfig {
  environment: 'LEFT' | 'RIGHT';
  platformType: string;
  hcfsNamespace: string | null;
  hiveServer2: HiveServer2Config;
  metastore_direct?: MetastoreDirectConfig | null;
  createIfNotExists: boolean;
  enableAutoTableStats: boolean;
  enableAutoColumnStats: boolean;
}

const PLATFORM_TYPES = [
  'CDH5', 'CDH6', 'CDP7_0', 'CDP7_1', 'CDP7_1_9_SP1', 'CDP7_2', 'CDP7_3',
  'HDP2', 'HDP3', 'MAPR', 'EMR', 'APACHE_HIVE1', 'APACHE_HIVE2', 'APACHE_HIVE3', 'APACHE_HIVE4'
] as const;

const DRIVER_CLASSES = [
  'org.apache.hive.jdbc.HiveDriver',
  'com.cloudera.hive.jdbc.HS2Driver'
] as const;

const DB_TYPES = ['MYSQL', 'POSTGRES', 'ORACLE'] as const;

const ClusterConfigurationSection: React.FC = () => {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const { state, updateConfig } = useConfiguration();
  const encryptedPasswords = state.config?.encryptedPasswords || false;
  const [showMetastoreDirect, setShowMetastoreDirect] = useState(false);
  const [newConnectionProperty, setNewConnectionProperty] = useState({ key: '', value: '' });
  const [showAddProperty, setShowAddProperty] = useState<{ [key: string]: boolean }>({});
  const [showPasswords, setShowPasswords] = useState<{ [key: string]: boolean }>({});

  const getDefaultLeftCluster = (): ClusterConfig => ({
    environment: 'LEFT',
    platformType: 'CDH6',
    hcfsNamespace: 'hdfs://source-cluster:8020',
    hiveServer2: {
      uri: 'jdbc:hive2://source-cluster:10000',
      disconnected: false,
      connectionProperties: {},
      driverClassName: 'org.apache.hive.jdbc.HiveDriver',
      jarFile: null,
      version: null
    },
    metastore_direct: {
      uri: '',
      type: 'MYSQL',
      connectionProperties: {},
      initSql: '',
      connectionPool: {
        min: 3,
        max: 5,
        timeout: 10000
      }
    },
    createIfNotExists: false,
    enableAutoTableStats: false,
    enableAutoColumnStats: false
  });

  const getDefaultRightCluster = (): ClusterConfig => ({
    environment: 'RIGHT',
    platformType: 'CDP7_1',
    hcfsNamespace: 'hdfs://target-cluster:8020',
    hiveServer2: {
      uri: 'jdbc:hive2://target-cluster:10000',
      disconnected: false,
      connectionProperties: {},
      driverClassName: 'org.apache.hive.jdbc.HiveDriver',
      jarFile: null,
      version: null
    },
    createIfNotExists: false,
    enableAutoTableStats: false,
    enableAutoColumnStats: false
  });

  // Get current cluster configs from server state, with defaults as fallback
  const leftCluster = state.config?.clusters?.LEFT || getDefaultLeftCluster();
  const rightCluster = state.config?.clusters?.RIGHT || getDefaultRightCluster();

  const handleLeftClusterChange = (field: keyof ClusterConfig, value: string | boolean) => {
    const updatedCluster = { ...leftCluster, [field]: value };
    // Update server state directly - this will trigger re-render via context
    updateConfig({ 
      clusters: { 
        LEFT: updatedCluster, 
        RIGHT: rightCluster 
      } 
    });
  };

  const handleLeftHiveServer2Change = (field: keyof HiveServer2Config, value: string | boolean | Record<string, string>) => {
    const updatedCluster = {
      ...leftCluster,
      hiveServer2: { ...leftCluster.hiveServer2, [field]: value }
    };
    // Update server state directly - this will trigger re-render via context
    updateConfig({ 
      clusters: { 
        LEFT: updatedCluster, 
        RIGHT: rightCluster 
      } 
    });
  };

  const handleRightClusterChange = (field: keyof ClusterConfig, value: string | boolean) => {
    const updatedCluster = { ...rightCluster, [field]: value };
    // Update server state directly - this will trigger re-render via context
    updateConfig({ 
      clusters: { 
        LEFT: leftCluster, 
        RIGHT: updatedCluster 
      } 
    });
  };

  const handleRightHiveServer2Change = (field: keyof HiveServer2Config, value: string | boolean | Record<string, string>) => {
    const updatedCluster = {
      ...rightCluster,
      hiveServer2: { ...rightCluster.hiveServer2, [field]: value }
    };
    // Update server state directly - this will trigger re-render via context
    updateConfig({ 
      clusters: { 
        LEFT: leftCluster, 
        RIGHT: updatedCluster 
      } 
    });
  };

  const handleLeftMetastoreDirectChange = (field: keyof MetastoreDirectConfig, value: string | number | Record<string, string> | object) => {
    const updatedCluster = {
      ...leftCluster,
      metastore_direct: leftCluster.metastore_direct ? { ...leftCluster.metastore_direct, [field]: value } : undefined
    };
    // Update server state directly - this will trigger re-render via context
    updateConfig({ 
      clusters: { 
        LEFT: updatedCluster, 
        RIGHT: rightCluster 
      } 
    });
  };

  const addConnectionProperty = (clusterType: 'left' | 'right', section: 'hiveServer2' | 'metastoreDirect') => {
    const key = `${clusterType}-${section}`;
    const property = newConnectionProperty;
    
    if (!property.key.trim() || !property.value.trim()) return;

    let updatedLeftCluster = leftCluster;
    let updatedRightCluster = rightCluster;

    if (clusterType === 'left') {
      if (section === 'hiveServer2') {
        updatedLeftCluster = {
          ...leftCluster,
          hiveServer2: {
            ...leftCluster.hiveServer2,
            connectionProperties: {
              ...leftCluster.hiveServer2.connectionProperties,
              [property.key]: property.value
            }
          }
        };
      } else if (section === 'metastoreDirect' && leftCluster.metastore_direct) {
        updatedLeftCluster = {
          ...leftCluster,
          metastore_direct: {
            ...leftCluster.metastore_direct,
            connectionProperties: {
              ...leftCluster.metastore_direct.connectionProperties,
              [property.key]: property.value
            }
          }
        };
      }
    } else {
      updatedRightCluster = {
        ...rightCluster,
        hiveServer2: {
          ...rightCluster.hiveServer2,
          connectionProperties: {
            ...rightCluster.hiveServer2.connectionProperties,
            [property.key]: property.value
          }
        }
      };
    }

    // Update server state directly
    updateConfig({ 
      clusters: { 
        LEFT: updatedLeftCluster, 
        RIGHT: updatedRightCluster 
      } 
    });

    setNewConnectionProperty({ key: '', value: '' });
    setShowAddProperty(prev => ({ ...prev, [key]: false }));
  };

  const removeConnectionProperty = (clusterType: 'left' | 'right', section: 'hiveServer2' | 'metastoreDirect', propertyKey: string) => {
    let updatedLeftCluster = leftCluster;
    let updatedRightCluster = rightCluster;

    if (clusterType === 'left') {
      if (section === 'hiveServer2') {
        const { [propertyKey]: removed, ...rest } = leftCluster.hiveServer2.connectionProperties;
        updatedLeftCluster = {
          ...leftCluster,
          hiveServer2: {
            ...leftCluster.hiveServer2,
            connectionProperties: rest
          }
        };
      } else if (section === 'metastoreDirect' && leftCluster.metastore_direct) {
        const { [propertyKey]: removed, ...rest } = leftCluster.metastore_direct.connectionProperties;
        updatedLeftCluster = {
          ...leftCluster,
          metastore_direct: {
            ...leftCluster.metastore_direct,
            connectionProperties: rest
          }
        };
      }
    } else {
      const { [propertyKey]: removed, ...rest } = rightCluster.hiveServer2.connectionProperties;
      updatedRightCluster = {
        ...rightCluster,
        hiveServer2: {
          ...rightCluster.hiveServer2,
          connectionProperties: rest
        }
      };
    }

    // Update server state directly
    updateConfig({ 
      clusters: { 
        LEFT: updatedLeftCluster, 
        RIGHT: updatedRightCluster 
      } 
    });
  };

  // Save functionality removed - now handled by global configuration context

  const handleBack = () => {
    if (id === 'current') {
      navigate('/config/current');
    } else {
      navigate(`/config/edit/${id}`);
    }
  };

  const renderConnectionProperties = (
    properties: Record<string, string>,
    clusterType: 'left' | 'right',
    section: 'hiveServer2' | 'metastoreDirect',
    title: string
  ) => {
    const key = `${clusterType}-${section}`;
    const showAdd = showAddProperty[key] || false;

    return (
      <div className="mt-4">
        <div className="flex items-center justify-between mb-2">
          <h5 className="text-sm font-medium text-gray-700">{title}</h5>
          <button
            type="button"
            onClick={() => setShowAddProperty(prev => ({ ...prev, [key]: !showAdd }))}
            className="text-blue-600 hover:text-blue-700 text-sm flex items-center"
            disabled={encryptedPasswords}
          >
            <PlusIcon className="h-4 w-4 mr-1" />
            Add Property
          </button>
        </div>
        
        {encryptedPasswords && (
          <p className="text-xs text-amber-600 mb-2">
            Properties cannot be modified while passwords are encrypted. Decrypt in Advanced Options first.
          </p>
        )}
        
        {Object.entries(properties).filter(([key]) => key !== 'user' && key !== 'password').length > 0 && (
          <div className="space-y-2 mb-3">
            {Object.entries(properties).filter(([key]) => key !== 'user' && key !== 'password').map(([propKey, propValue]) => (
              <div key={propKey} className="flex items-center space-x-2 bg-gray-100 p-2 rounded">
                <span className="text-sm font-medium text-gray-600 min-w-0 flex-1">{propKey}:</span>
                <span className="text-sm text-gray-800 min-w-0 flex-1">{propValue}</span>
                <button
                  type="button"
                  onClick={() => removeConnectionProperty(clusterType, section, propKey)}
                  className="text-red-600 hover:text-red-700 flex-shrink-0 disabled:opacity-50 disabled:cursor-not-allowed"
                  disabled={encryptedPasswords}
                >
                  <XMarkIcon className="h-4 w-4" />
                </button>
              </div>
            ))}
          </div>
        )}
        
        {showAdd && !encryptedPasswords && (
          <div className="flex items-center space-x-2 mb-3">
            <input
              type="text"
              placeholder="Property key"
              value={newConnectionProperty.key}
              onChange={(e) => setNewConnectionProperty(prev => ({ ...prev, key: e.target.value }))}
              className="flex-1 p-2 border border-gray-300 rounded-md text-sm focus:ring-1 focus:ring-blue-500 focus:border-blue-500"
            />
            <input
              type="text"
              placeholder="Property value"
              value={newConnectionProperty.value}
              onChange={(e) => setNewConnectionProperty(prev => ({ ...prev, value: e.target.value }))}
              className="flex-1 p-2 border border-gray-300 rounded-md text-sm focus:ring-1 focus:ring-blue-500 focus:border-blue-500"
            />
            <button
              type="button"
              onClick={() => addConnectionProperty(clusterType, section)}
              className="px-3 py-2 bg-blue-600 text-white text-sm rounded-md hover:bg-blue-700 flex-shrink-0"
            >
              Add
            </button>
            <button
              type="button"
              onClick={() => {
                setShowAddProperty(prev => ({ ...prev, [key]: false }));
                setNewConnectionProperty({ key: '', value: '' });
              }}
              className="px-3 py-2 bg-gray-300 text-gray-700 text-sm rounded-md hover:bg-gray-400 flex-shrink-0"
            >
              Cancel
            </button>
          </div>
        )}
      </div>
    );
  };

  const renderMetastoreDirectConfig = () => {
    if (!leftCluster.metastore_direct) return null;

    return (
      <div className="border border-gray-200 rounded-lg p-4 bg-gray-50">
        <button
          type="button"
          onClick={() => setShowMetastoreDirect(!showMetastoreDirect)}
          className="flex items-center justify-between w-full mb-3"
        >
          <h4 className="text-md font-medium text-gray-900">Metastore Direct Configuration (Optional)</h4>
          {showMetastoreDirect ? (
            <ChevronDownIcon className="h-5 w-5 text-gray-500" />
          ) : (
            <ChevronRightIcon className="h-5 w-5 text-gray-500" />
          )}
        </button>
        
        {showMetastoreDirect && (
          <div className="space-y-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                JDBC URI
              </label>
              <input
                type="text"
                value={leftCluster.metastore_direct.uri || ''}
                onChange={(e) => handleLeftMetastoreDirectChange('uri', e.target.value)}
                className="w-full p-3 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                placeholder="jdbc:mysql://hostname:3306/hive_metastore"
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Database Type
              </label>
              <select
                value={leftCluster.metastore_direct.type}
                onChange={(e) => handleLeftMetastoreDirectChange('type', e.target.value)}
                className="w-full p-3 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
              >
                {DB_TYPES.map(type => (
                  <option key={type} value={type}>{type}</option>
                ))}
              </select>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Username
              </label>
              <input
                type="text"
                value={leftCluster.metastore_direct.connectionProperties?.user || ''}
                onChange={(e) => handleLeftMetastoreDirectChange('connectionProperties', {
                  ...leftCluster.metastore_direct.connectionProperties,
                  user: e.target.value
                })}
                className="w-full p-3 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                placeholder="Username for Metastore Direct connection"
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Password
              </label>
              <div className="relative">
                <input
                  type={showPasswords['left-metastore'] ? 'text' : 'password'}
                  value={leftCluster.metastore_direct.connectionProperties?.password || ''}
                  onChange={(e) => handleLeftMetastoreDirectChange('connectionProperties', {
                    ...leftCluster.metastore_direct.connectionProperties,
                    password: e.target.value
                  })}
                  readOnly={encryptedPasswords}
                  className={`w-full p-3 pr-12 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500 ${
                    encryptedPasswords ? 'bg-gray-100 cursor-not-allowed' : ''
                  }`}
                  placeholder={encryptedPasswords ? "Password is encrypted" : "Password for Metastore Direct connection"}
                />
                <button
                  type="button"
                  onClick={() => setShowPasswords(prev => ({ 
                    ...prev, 
                    ['left-metastore']: !prev['left-metastore']
                  }))}
                  className="absolute inset-y-0 right-0 flex items-center pr-3 text-gray-500 hover:text-gray-700"
                  disabled={encryptedPasswords}
                >
                  {showPasswords['left-metastore'] ? (
                    <EyeSlashIcon className="h-5 w-5" />
                  ) : (
                    <EyeIcon className="h-5 w-5" />
                  )}
                </button>
              </div>
              {encryptedPasswords && (
                <p className="text-xs text-amber-600 mt-1">
                  Passwords are encrypted. To change them, decrypt first in Advanced Options.
                </p>
              )}
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Init SQL
              </label>
              <input
                type="text"
                value={leftCluster.metastore_direct.initSql || ''}
                onChange={(e) => handleLeftMetastoreDirectChange('initSql', e.target.value)}
                className="w-full p-3 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                placeholder="SET sql_mode='';SET names utf8;"
              />
            </div>

            <div className="grid grid-cols-3 gap-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Min Connections
                </label>
                <input
                  type="number"
                  value={leftCluster.metastore_direct.connectionPool.min}
                  onChange={(e) => handleLeftMetastoreDirectChange('connectionPool', {
                    ...leftCluster.metastore_direct.connectionPool,
                    min: parseInt(e.target.value) || 0
                  })}
                  className="w-full p-3 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                  min="1"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Max Connections
                </label>
                <input
                  type="number"
                  value={leftCluster.metastore_direct.connectionPool.max}
                  onChange={(e) => handleLeftMetastoreDirectChange('connectionPool', {
                    ...leftCluster.metastore_direct.connectionPool,
                    max: parseInt(e.target.value) || 0
                  })}
                  className="w-full p-3 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                  min="1"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Timeout (ms)
                </label>
                <input
                  type="number"
                  value={leftCluster.metastore_direct.connectionPool.timeout}
                  onChange={(e) => handleLeftMetastoreDirectChange('connectionPool', {
                    ...leftCluster.metastore_direct.connectionPool,
                    timeout: parseInt(e.target.value) || 0
                  })}
                  className="w-full p-3 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                  min="1000"
                />
              </div>
            </div>

            {renderConnectionProperties(
              leftCluster.metastore_direct.connectionProperties,
              'left',
              'metastoreDirect',
              'Connection Properties'
            )}
          </div>
        )}
      </div>
    );
  };

  const renderClusterForm = (
    cluster: ClusterConfig, 
    onChange: (field: keyof ClusterConfig, value: string | boolean) => void,
    onHiveServer2Change: (field: keyof HiveServer2Config, value: string | boolean | Record<string, string>) => void,
    title: string,
    description: string
  ) => (
    <div className="bg-white rounded-lg shadow-lg p-6">
      <h3 className="text-lg font-medium text-gray-900 mb-2">{title}</h3>
      <p className="text-sm text-gray-600 mb-6">{description}</p>
      
      <div className="space-y-6">
        {/* Platform Type */}
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            Platform Type *
          </label>
          <select
            value={cluster.platformType}
            onChange={(e) => onChange('platformType', e.target.value)}
            className="w-full p-3 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
            required
          >
            {PLATFORM_TYPES.map(type => (
              <option key={type} value={type}>{type}</option>
            ))}
          </select>
          <p className="text-xs text-gray-500 mt-1">Select the platform type for your cluster</p>
        </div>

        {/* HCFS Namespace */}
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            HCFS Namespace *
          </label>
          <input
            type="text"
            value={cluster.hcfsNamespace}
            onChange={(e) => onChange('hcfsNamespace', e.target.value)}
            className="w-full p-3 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
            placeholder="hdfs://namenode:8020 or s3a://bucket"
            required
          />
          <p className="text-xs text-gray-500 mt-1">The namespace for the HCFS system (HDFS, S3, etc.)</p>
        </div>

        {/* HiveServer2 Configuration */}
        <div className="border border-gray-200 rounded-lg p-4 bg-gray-50">
          <h4 className="text-md font-medium text-gray-900 mb-3">HiveServer2 Configuration</h4>
          
          <div className="space-y-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                JDBC URI *
              </label>
              <input
                type="text"
                value={cluster.hiveServer2.uri}
                onChange={(e) => onHiveServer2Change('uri', e.target.value)}
                className="w-full p-3 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                placeholder="jdbc:hive2://hostname:10000/database_name"
                required
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Driver Class Name
              </label>
              <select
                value={cluster.hiveServer2.driverClassName}
                onChange={(e) => onHiveServer2Change('driverClassName', e.target.value)}
                className="w-full p-3 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
              >
                {DRIVER_CLASSES.map(driver => (
                  <option key={driver} value={driver}>{driver}</option>
                ))}
              </select>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                JAR File Path
              </label>
              <input
                type="text"
                value={cluster.hiveServer2.jarFile || ''}
                onChange={(e) => onHiveServer2Change('jarFile', e.target.value || null)}
                className="w-full p-3 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                placeholder="/path/to/hive-jdbc.jar"
              />
              <p className="text-xs text-gray-500 mt-1">Path to JDBC driver JAR file (leave empty for Kerberos connections)</p>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Username
              </label>
              <input
                type="text"
                value={cluster.hiveServer2.connectionProperties?.user || ''}
                onChange={(e) => onHiveServer2Change('connectionProperties', {
                  ...cluster.hiveServer2.connectionProperties,
                  user: e.target.value
                })}
                className="w-full p-3 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                placeholder="Username for HiveServer2 connection"
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Password
              </label>
              <div className="relative">
                <input
                  type={showPasswords[`${cluster.environment}-hs2`] ? 'text' : 'password'}
                  value={cluster.hiveServer2.connectionProperties?.password || ''}
                  onChange={(e) => onHiveServer2Change('connectionProperties', {
                    ...cluster.hiveServer2.connectionProperties,
                    password: e.target.value
                  })}
                  readOnly={encryptedPasswords}
                  className={`w-full p-3 pr-12 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500 ${
                    encryptedPasswords ? 'bg-gray-100 cursor-not-allowed' : ''
                  }`}
                  placeholder={encryptedPasswords ? "Password is encrypted" : "Password for HiveServer2 connection"}
                />
                <button
                  type="button"
                  onClick={() => setShowPasswords(prev => ({ 
                    ...prev, 
                    [`${cluster.environment}-hs2`]: !prev[`${cluster.environment}-hs2`]
                  }))}
                  className="absolute inset-y-0 right-0 flex items-center pr-3 text-gray-500 hover:text-gray-700"
                  disabled={encryptedPasswords}
                >
                  {showPasswords[`${cluster.environment}-hs2`] ? (
                    <EyeSlashIcon className="h-5 w-5" />
                  ) : (
                    <EyeIcon className="h-5 w-5" />
                  )}
                </button>
              </div>
              {encryptedPasswords && (
                <p className="text-xs text-amber-600 mt-1">
                  Passwords are encrypted. To change them, decrypt first in Advanced Options.
                </p>
              )}
            </div>

            <div className="flex items-center">
              <input
                type="checkbox"
                id={`${cluster.environment}-disconnected`}
                checked={cluster.hiveServer2.disconnected}
                onChange={(e) => onHiveServer2Change('disconnected', e.target.checked)}
                className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
              />
              <label htmlFor={`${cluster.environment}-disconnected`} className="ml-2 block text-sm text-gray-900">
                Disconnected Mode
              </label>
            </div>

            {renderConnectionProperties(
              cluster.hiveServer2.connectionProperties,
              cluster.environment === 'LEFT' ? 'left' : 'right',
              'hiveServer2',
              'Connection Properties'
            )}
          </div>
        </div>

        {/* Metastore Direct Configuration - LEFT cluster only */}
        {cluster.environment === 'LEFT' && renderMetastoreDirectConfig()}

        {/* Additional Options */}
        <div className="space-y-3">
          <h4 className="text-md font-medium text-gray-900">Additional Options</h4>
          
          <div className="flex items-center">
            <input
              type="checkbox"
              id={`${cluster.environment}-createIfNotExists`}
              checked={cluster.createIfNotExists}
              onChange={(e) => onChange('createIfNotExists', e.target.checked)}
              className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
            />
            <label htmlFor={`${cluster.environment}-createIfNotExists`} className="ml-2 block text-sm text-gray-900">
              Use IF NOT EXISTS syntax in CREATE statements
            </label>
          </div>

          <div className="flex items-center">
            <input
              type="checkbox"
              id={`${cluster.environment}-autoTableStats`}
              checked={cluster.enableAutoTableStats}
              onChange={(e) => onChange('enableAutoTableStats', e.target.checked)}
              className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
            />
            <label htmlFor={`${cluster.environment}-autoTableStats`} className="ml-2 block text-sm text-gray-900">
              Enable automatic table statistics
            </label>
          </div>

          <div className="flex items-center">
            <input
              type="checkbox"
              id={`${cluster.environment}-autoColumnStats`}
              checked={cluster.enableAutoColumnStats}
              onChange={(e) => onChange('enableAutoColumnStats', e.target.checked)}
              className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
            />
            <label htmlFor={`${cluster.environment}-autoColumnStats`} className="ml-2 block text-sm text-gray-900">
              Enable automatic column statistics
            </label>
          </div>
        </div>
      </div>
    </div>
  );

  return (
    <div className="p-8">
      <div className="max-w-6xl mx-auto">
        {/* Header */}
        <div className="mb-8">
          <button
            onClick={handleBack}
            className="flex items-center text-blue-600 hover:text-blue-700 mb-4"
            disabled={state.isLoading}
          >
            <ArrowLeftIcon className="h-4 w-4 mr-2" />
            Back to Configuration Overview
          </button>
          <h1 className="text-3xl font-bold text-gray-900">Cluster Configuration</h1>
          <p className="text-gray-600 mt-2">Configure source and target cluster connections</p>
        </div>

        {state.error && (
          <div className="mb-6 bg-red-50 border border-red-200 rounded-md p-4">
            <div className="text-sm text-red-700">{state.error}</div>
          </div>
        )}

        {/* Cluster Forms */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 mb-8">
          {renderClusterForm(
            leftCluster,
            handleLeftClusterChange,
            handleLeftHiveServer2Change,
            'Source Cluster (LEFT)',
            'Configuration for the source Hive cluster you are migrating FROM'
          )}
          
          {renderClusterForm(
            rightCluster,
            handleRightClusterChange,
            handleRightHiveServer2Change,
            'Target Cluster (RIGHT)',
            'Configuration for the target Hive cluster you are migrating TO'
          )}
        </div>

        {/* Actions */}
        <div className="mt-8 text-center">
          <div className="text-sm text-gray-500">
            Changes are applied but NOT saved. Use the 'Save or Save As' to persist the current configuration
          </div>
        </div>
      </div>
    </div>
  );
};

export default ClusterConfigurationSection;