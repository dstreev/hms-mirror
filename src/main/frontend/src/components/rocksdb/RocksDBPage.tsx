import React, { useState, useEffect } from 'react';
import {
  CircleStackIcon,
  WrenchScrewdriverIcon,
  MagnifyingGlassIcon,
  ArrowPathIcon,
  TrashIcon,
  ExclamationTriangleIcon,
  InformationCircleIcon,
  FolderIcon,
  DocumentIcon,
  ChevronRightIcon,
  ChevronDownIcon,
  ArchiveBoxIcon,
  CloudArrowDownIcon,
  ClipboardDocumentIcon
} from '@heroicons/react/24/outline';

interface RocksDBHealth {
  status: string;
  message: string;
  statistics?: Record<string, any>;
}

interface ColumnFamily {
  name: string;
  keysCount?: number;
  dataSize?: string;
}

interface RocksDBKey {
  key: string;
  value?: string;
  isDirectory?: boolean;
  children?: RocksDBKey[];
}

interface BackupInfo {
  name: string;
  timestamp: string;
  size: string;
  path: string;
}

const RocksDBPage: React.FC = () => {
  const [activeTab, setActiveTab] = useState<'browser' | 'maintenance'>('browser');
  const [health, setHealth] = useState<RocksDBHealth | null>(null);
  const [columnFamilies, setColumnFamilies] = useState<ColumnFamily[]>([]);
  const [selectedColumnFamily, setSelectedColumnFamily] = useState<string>('default');
  const [keys, setKeys] = useState<RocksDBKey[]>([]);
  const [selectedKey, setSelectedKey] = useState<string>('');
  const [selectedValue, setSelectedValue] = useState<string>('');
  const [expandedKeys, setExpandedKeys] = useState<Set<string>>(new Set());
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [searchQuery, setSearchQuery] = useState('');
  const [useRegex, setUseRegex] = useState(false);
  const [currentPath, setCurrentPath] = useState<string[]>([]);
  const [backups, setBackups] = useState<BackupInfo[]>([]);
  const [backupLoading, setBackupLoading] = useState(false);
  const [copyStatus, setCopyStatus] = useState<string>('');

  useEffect(() => {
    loadRocksDBHealth();
    loadColumnFamilies();
    loadBackups();
  }, []);

  useEffect(() => {
    if (selectedColumnFamily) {
      loadKeys();
    }
  }, [selectedColumnFamily, currentPath]);

  const keyMatchesSearch = (keyObj: RocksDBKey, query: string): boolean => {
    if (query === '') return true;
    
    let matches = false;
    
    if (useRegex) {
      // Use as regex pattern
      try {
        const regex = new RegExp(query, 'i'); // case-insensitive
        matches = regex.test(keyObj.key);
        
        // Debug logging for regex search
        if (query === '.*') {
          console.log(`Regex '.*' testing key '${keyObj.key}': ${matches}`);
        }
      } catch (e) {
        console.warn(`Invalid regex pattern: ${query}, falling back to string matching`);
        // If regex is invalid, fall back to simple string matching
        matches = keyObj.key.toLowerCase().includes(query.toLowerCase());
      }
    } else {
      // Simple substring matching for plain text
      matches = keyObj.key.toLowerCase().includes(query.toLowerCase());
    }
    
    if (matches) {
      return true;
    }
    
    // Check if any child matches (recursive search)
    if (keyObj.children) {
      return keyObj.children.some(child => keyMatchesSearch(child, query));
    }
    
    return false;
  };

  useEffect(() => {
    // Auto-expand directories that contain search matches
    if (searchQuery && keys.length > 0) {
      const expandDirectoriesWithMatches = (keyList: RocksDBKey[]) => {
        keyList.forEach(keyObj => {
          if (keyObj.isDirectory && keyObj.children) {
            // Check if this directory or any child matches the search
            const hasMatchingChild = keyObj.children.some(child => 
              keyMatchesSearch(child, searchQuery)
            );
            if (hasMatchingChild) {
              setExpandedKeys(prev => new Set([...prev, keyObj.key]));
              // Recursively expand child directories
              expandDirectoriesWithMatches(keyObj.children);
            }
          }
        });
      };
      expandDirectoriesWithMatches(keys);
    }
  }, [searchQuery, keys, useRegex]);

  const loadRocksDBHealth = async () => {
    try {
      const response = await fetch('/hms-mirror/api/v1/rocksdb/health');
      if (response.ok) {
        const data = await response.json();
        setHealth(data);
      }
    } catch (err) {
      console.error('Error loading RocksDB health:', err);
    }
  };

  const loadColumnFamilies = async () => {
    try {
      const response = await fetch('/hms-mirror/api/v1/rocksdb/column-families');
      if (response.ok) {
        const data = await response.json();
        setColumnFamilies(data.columnFamilies || []);
      }
    } catch (err) {
      console.error('Error loading column families:', err);
    }
  };

  const loadKeys = async () => {
    setLoading(true);
    try {
      const pathParam = currentPath.length > 0 ? `?prefix=${encodeURIComponent(currentPath.join('/'))}` : '';
      const response = await fetch(`/hms-mirror/api/v1/rocksdb/data/${selectedColumnFamily}/keys${pathParam}`);
      if (response.ok) {
        const data = await response.json();
        console.log(`Loaded ${data.keys?.length || 0} keys for column family: ${selectedColumnFamily}`);
        setKeys(buildKeyTree(data.keys || []));
      }
      setError(null);
    } catch (err) {
      console.error('Error loading keys:', err);
      setError(err instanceof Error ? err.message : 'Failed to load keys');
      setKeys([]);
    } finally {
      setLoading(false);
    }
  };

  const buildKeyTree = (flatKeys: string[]): RocksDBKey[] => {
    const tree: RocksDBKey[] = [];
    const pathMap = new Map<string, RocksDBKey>();

    // Group keys by their path components
    flatKeys.forEach(key => {
      const parts = key.split('/');
      let currentPath = '';
      
      parts.forEach((part, index) => {
        const isLast = index === parts.length - 1;
        currentPath = currentPath ? `${currentPath}/${part}` : part;
        
        if (!pathMap.has(currentPath)) {
          const keyObj: RocksDBKey = {
            key: currentPath,
            isDirectory: !isLast,
            children: []
          };
          pathMap.set(currentPath, keyObj);
          
          if (index === 0) {
            tree.push(keyObj);
          } else {
            const parentPath = parts.slice(0, index).join('/');
            const parent = pathMap.get(parentPath);
            if (parent) {
              parent.children = parent.children || [];
              parent.children.push(keyObj);
            }
          }
        }
      });
    });

    return tree;
  };

  const loadValue = async (key: string) => {
    try {
      setSelectedKey(key);
      setSelectedValue('Loading...');
      const response = await fetch(`/hms-mirror/api/v1/rocksdb/data/${selectedColumnFamily}?key=${encodeURIComponent(key)}`);
      if (response.ok) {
        const data = await response.json();
        setSelectedValue(JSON.stringify(data.value, null, 2));
      } else {
        setSelectedValue('Error loading value');
      }
    } catch (err) {
      setSelectedValue('Error loading value: ' + (err instanceof Error ? err.message : 'Unknown error'));
    }
  };

  const toggleKeyExpansion = (key: string) => {
    const newExpanded = new Set(expandedKeys);
    if (newExpanded.has(key)) {
      newExpanded.delete(key);
    } else {
      newExpanded.add(key);
    }
    setExpandedKeys(newExpanded);
  };

  const performMaintenance = async (action: string) => {
    try {
      setLoading(true);
      let response;

      if (action === 'compact') {
        // Use the existing compaction endpoint
        response = await fetch('/hms-mirror/api/v1/rocksdb/compaction', {
          method: 'POST'
        });
      } else {
        // Use the new maintenance endpoint for other actions
        response = await fetch(`/hms-mirror/api/v1/rocksdb/maintenance/${action}`, {
          method: 'POST'
        });
      }

      if (response.ok) {
        const result = await response.json();
        alert(`${action} completed: ${result.message}`);
        if (action === 'compact') {
          loadRocksDBHealth(); // Refresh statistics
        }
      } else {
        alert(`${action} failed`);
      }
    } catch (err) {
      alert(`Error performing ${action}: ${err instanceof Error ? err.message : 'Unknown error'}`);
    } finally {
      setLoading(false);
    }
  };

  const clearColumnFamilyData = async (columnFamily: string) => {
    try {
      setLoading(true);
      const response = await fetch(`/hms-mirror/api/v1/rocksdb/data/${columnFamily}/all`, {
        method: 'DELETE'
      });

      if (response.ok) {
        const result = await response.json();
        alert(`Column family data cleared: ${result.message}`);
        // Refresh keys for the current column family
        loadKeys();
        // Refresh statistics
        loadRocksDBHealth();
        loadColumnFamilies();
      } else {
        const error = await response.json();
        alert(`Failed to clear column family data: ${error.error || 'Unknown error'}`);
      }
    } catch (err) {
      alert(`Error clearing column family data: ${err instanceof Error ? err.message : 'Unknown error'}`);
    } finally {
      setLoading(false);
    }
  };

  const loadBackups = async () => {
    try {
      const response = await fetch('/hms-mirror/api/v1/rocksdb/backups');
      if (response.ok) {
        const data = await response.json();
        setBackups(data.backups || []);
      }
    } catch (err) {
      console.error('Error loading backups:', err);
    }
  };

  const createBackup = async () => {
    try {
      setBackupLoading(true);
      const response = await fetch('/hms-mirror/api/v1/rocksdb/backup', {
        method: 'POST'
      });
      
      if (response.ok) {
        const result = await response.json();
        alert(`Backup completed: ${result.message}`);
        loadBackups(); // Refresh backups list
      } else {
        const error = await response.text();
        alert(`Backup failed: ${error}`);
      }
    } catch (err) {
      alert(`Error creating backup: ${err instanceof Error ? err.message : 'Unknown error'}`);
    } finally {
      setBackupLoading(false);
    }
  };

  const deleteBackup = async (backupName: string) => {
    if (!confirm(`Are you sure you want to delete backup '${backupName}'? This action cannot be undone.`)) {
      return;
    }

    try {
      const response = await fetch(`/hms-mirror/api/v1/rocksdb/backup/${encodeURIComponent(backupName)}`, {
        method: 'DELETE'
      });
      
      if (response.ok) {
        const result = await response.json();
        alert(`Backup deleted: ${result.message}`);
        loadBackups(); // Refresh backups list
      } else {
        const error = await response.text();
        alert(`Delete failed: ${error}`);
      }
    } catch (err) {
      alert(`Error deleting backup: ${err instanceof Error ? err.message : 'Unknown error'}`);
    }
  };

  const restoreBackup = async (backupName: string) => {
    if (!confirm(`Are you sure you want to restore from backup '${backupName}'? This will replace the current database and requires an application restart.`)) {
      return;
    }

    try {
      setBackupLoading(true);
      const response = await fetch(`/hms-mirror/api/v1/rocksdb/restore/${encodeURIComponent(backupName)}`, {
        method: 'POST'
      });
      
      if (response.ok) {
        const result = await response.json();
        alert(`Restore completed: ${result.message}\n\nIMPORTANT: Application restart is required for the restored data to take effect.`);
        loadBackups(); // Refresh backups list
      } else {
        const error = await response.text();
        alert(`Restore failed: ${error}`);
      }
    } catch (err) {
      alert(`Error restoring backup: ${err instanceof Error ? err.message : 'Unknown error'}`);
    } finally {
      setBackupLoading(false);
    }
  };

  const copyToClipboard = async () => {
    if (!selectedValue || selectedValue === 'Loading...' || selectedValue.startsWith('Error')) {
      setCopyStatus('No valid content to copy');
      setTimeout(() => setCopyStatus(''), 2000);
      return;
    }

    try {
      await navigator.clipboard.writeText(selectedValue);
      setCopyStatus('Copied!');
      setTimeout(() => setCopyStatus(''), 2000);
    } catch (err) {
      console.error('Failed to copy to clipboard:', err);
      setCopyStatus('Copy failed');
      setTimeout(() => setCopyStatus(''), 2000);
    }
  };

  const renderKeyTree = (keyList: RocksDBKey[], level: number = 0) => {
    const filteredKeys = keyList.filter(keyObj => keyMatchesSearch(keyObj, searchQuery));
    
    // Debug logging for search filtering
    if (searchQuery && level === 0) {
      console.log(`Search query: "${searchQuery}", useRegex: ${useRegex}`);
      console.log(`Total keys: ${keyList.length}, Filtered keys: ${filteredKeys.length}`);
    }
    
    return filteredKeys.map(keyObj => (
        <div key={keyObj.key} style={{ marginLeft: `${level * 16}px` }}>
          <div
            className={`flex items-center py-1 px-2 hover:bg-gray-100 cursor-pointer rounded ${
              selectedKey === keyObj.key ? 'bg-blue-100' : ''
            }`}
            onClick={() => {
              if (keyObj.isDirectory) {
                toggleKeyExpansion(keyObj.key);
              } else {
                loadValue(keyObj.key);
              }
            }}
          >
            {keyObj.isDirectory ? (
              <>
                {expandedKeys.has(keyObj.key) ? (
                  <ChevronDownIcon className="h-4 w-4 mr-1" />
                ) : (
                  <ChevronRightIcon className="h-4 w-4 mr-1" />
                )}
                <FolderIcon className="h-4 w-4 mr-2 text-blue-500" />
              </>
            ) : (
              <>
                <div className="w-5 mr-1" />
                <DocumentIcon className="h-4 w-4 mr-2 text-gray-500" />
              </>
            )}
            <span className="text-sm">{keyObj.key.split('/').pop()}</span>
          </div>
          {keyObj.isDirectory && keyObj.children && expandedKeys.has(keyObj.key) && (
            <div>
              {renderKeyTree(keyObj.children, level + 1)}
            </div>
          )}
        </div>
      ));
  };

  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      {/* Page Header */}
      <div className="mb-8">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold text-gray-900 flex items-center">
              <CircleStackIcon className="h-8 w-8 mr-3 text-blue-600" />
              RocksDB Management
            </h1>
            <p className="text-gray-600 mt-2">Database maintenance and data browser</p>
          </div>
          <button
            onClick={loadRocksDBHealth}
            className="inline-flex items-center px-4 py-2 border border-gray-300 rounded-md shadow-sm text-sm font-medium text-gray-700 bg-white hover:bg-gray-50"
          >
            <ArrowPathIcon className="h-4 w-4 mr-2" />
            Refresh
          </button>
        </div>
      </div>

      {/* Health Status */}
      {health && (
        <div className={`mb-6 p-4 rounded-md ${
          health.status === 'healthy' ? 'bg-green-50 border border-green-200' : 'bg-red-50 border border-red-200'
        }`}>
          <div className="flex items-center">
            <InformationCircleIcon className={`h-5 w-5 mr-2 ${
              health.status === 'healthy' ? 'text-green-500' : 'text-red-500'
            }`} />
            <h3 className={`text-sm font-medium ${
              health.status === 'healthy' ? 'text-green-800' : 'text-red-800'
            }`}>
              RocksDB Status: {health.status}
            </h3>
          </div>
          <p className={`mt-2 text-sm ${
            health.status === 'healthy' ? 'text-green-700' : 'text-red-700'
          }`}>
            {health.message}
          </p>
        </div>
      )}

      {/* Tabbed Card */}
      <div className="bg-white rounded-lg shadow">
        {/* Tab Navigation */}
        <div className="border-b border-gray-200">
          <nav className="-mb-px flex" aria-label="Tabs">
            <button
              onClick={() => setActiveTab('browser')}
              className={`${
                activeTab === 'browser'
                  ? 'border-blue-500 text-blue-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
              } flex-1 whitespace-nowrap py-4 px-1 border-b-2 font-medium text-sm flex items-center justify-center`}
            >
              <MagnifyingGlassIcon className="h-5 w-5 mr-2" />
              Data Browser
            </button>
            <button
              onClick={() => setActiveTab('maintenance')}
              className={`${
                activeTab === 'maintenance'
                  ? 'border-blue-500 text-blue-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
              } flex-1 whitespace-nowrap py-4 px-1 border-b-2 font-medium text-sm flex items-center justify-center`}
            >
              <WrenchScrewdriverIcon className="h-5 w-5 mr-2" />
              Maintenance Operations
            </button>
          </nav>
        </div>

        {/* Tab Content */}
        <div className="p-6">
          {/* Data Browser Tab */}
          {activeTab === 'browser' && (
            <div className="space-y-4">
              {/* Column Family Selector */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Column Family
                </label>
                <select
                  value={selectedColumnFamily}
                  onChange={(e) => setSelectedColumnFamily(e.target.value)}
                  className="block w-full border border-gray-300 rounded-md px-3 py-2 text-sm"
                >
                  {columnFamilies.map(cf => (
                    <option key={cf.name} value={cf.name}>{cf.name}</option>
                  ))}
                </select>
              </div>

              {/* Search */}
              <div>
                <div className="relative">
                  <MagnifyingGlassIcon className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-400" />
                  <input
                    type="text"
                    placeholder="Search keys..."
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(e.target.value)}
                    className="block w-full pl-10 pr-3 py-2 border border-gray-300 rounded-md text-sm placeholder-gray-500 focus:outline-none focus:ring-1 focus:ring-blue-500 focus:border-blue-500"
                  />
                </div>
                <div className="mt-2 flex items-center">
                  <input
                    type="checkbox"
                    id="useRegex"
                    checked={useRegex}
                    onChange={(e) => setUseRegex(e.target.checked)}
                    className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                  />
                  <label htmlFor="useRegex" className="ml-2 block text-sm text-gray-700">
                    Use regular expressions (e.g., .*, ^session, connection$)
                  </label>
                </div>
              </div>

              {/* Keys Tree */}
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 h-96">
                <div className="border border-gray-300 rounded-md p-3 overflow-y-auto">
                  <h3 className="text-sm font-medium text-gray-700 mb-2">Keys</h3>
                  {loading ? (
                    <div className="flex items-center justify-center py-8">
                      <div className="animate-spin rounded-full h-6 w-6 border-b-2 border-blue-600"></div>
                      <span className="ml-2 text-sm text-gray-600">Loading...</span>
                    </div>
                  ) : error ? (
                    <div className="text-red-600 text-sm">{error}</div>
                  ) : (
                    <div className="space-y-1">
                      {renderKeyTree(keys)}
                    </div>
                  )}
                </div>

                <div className="border border-gray-300 rounded-md p-3 overflow-y-auto">
                  <div className="flex items-center justify-between mb-2">
                    <h3 className="text-sm font-medium text-gray-700">Value</h3>
                    {selectedKey && selectedValue && !selectedValue.startsWith('Loading') && !selectedValue.startsWith('Error') && (
                      <div className="flex items-center space-x-2">
                        {copyStatus && (
                          <span className={`text-xs ${copyStatus === 'Copied!' ? 'text-green-600' : 'text-red-600'}`}>
                            {copyStatus}
                          </span>
                        )}
                        <button
                          onClick={copyToClipboard}
                          className="inline-flex items-center px-2 py-1 text-xs font-medium text-gray-700 bg-white border border-gray-300 rounded hover:bg-gray-50 focus:outline-none focus:ring-1 focus:ring-blue-500"
                          title="Copy value to clipboard"
                        >
                          <ClipboardDocumentIcon className="h-3 w-3 mr-1" />
                          Copy
                        </button>
                      </div>
                    )}
                  </div>
                  {selectedKey ? (
                    <div>
                      <div className="text-xs text-gray-500 mb-2">Key: {selectedKey}</div>
                      <pre className="text-xs bg-gray-50 p-2 rounded overflow-x-auto whitespace-pre-wrap">
                        {selectedValue}
                      </pre>
                    </div>
                  ) : (
                    <div className="text-gray-500 text-sm">Select a key to view its value</div>
                  )}
                </div>
              </div>
            </div>
          )}

          {/* Maintenance Operations Tab */}
          {activeTab === 'maintenance' && (
            <div className="space-y-4">
            <div>
              <h3 className="text-sm font-medium text-gray-700 mb-2">Database Operations</h3>
              <div className="space-y-2">
                <button
                  onClick={() => performMaintenance('compact')}
                  disabled={loading}
                  className="w-full inline-flex items-center justify-center px-4 py-2 border border-gray-300 rounded-md shadow-sm text-sm font-medium text-gray-700 bg-white hover:bg-gray-50 disabled:opacity-50"
                >
                  <CircleStackIcon className="h-4 w-4 mr-2" />
                  Manual Compaction
                </button>
                <button
                  onClick={() => performMaintenance('flush')}
                  disabled={loading}
                  className="w-full inline-flex items-center justify-center px-4 py-2 border border-gray-300 rounded-md shadow-sm text-sm font-medium text-gray-700 bg-white hover:bg-gray-50 disabled:opacity-50"
                >
                  <ArrowPathIcon className="h-4 w-4 mr-2" />
                  Flush Memtables
                </button>
              </div>
            </div>

            <div>
              <h3 className="text-sm font-medium text-gray-700 mb-2">Column Family Operations</h3>
              <div className="flex space-x-2">
                <select
                  value={selectedColumnFamily}
                  onChange={(e) => setSelectedColumnFamily(e.target.value)}
                  className="flex-1 border border-gray-300 rounded-md px-3 py-2 text-sm"
                >
                  {columnFamilies.map(cf => (
                    <option key={cf.name} value={cf.name}>{cf.name}</option>
                  ))}
                </select>
                <button
                  onClick={() => performMaintenance(`compact-cf/${selectedColumnFamily}`)}
                  disabled={loading}
                  className="inline-flex items-center px-3 py-2 border border-gray-300 rounded-md shadow-sm text-sm font-medium text-gray-700 bg-white hover:bg-gray-50 disabled:opacity-50"
                >
                  Compact CF
                </button>
                <button
                  onClick={() => {
                    if (confirm(`Are you sure you want to delete ALL data in the '${selectedColumnFamily}' column family? This action cannot be undone.`)) {
                      clearColumnFamilyData(selectedColumnFamily);
                    }
                  }}
                  disabled={loading}
                  className="inline-flex items-center px-3 py-2 border border-red-300 rounded-md shadow-sm text-sm font-medium text-red-700 bg-white hover:bg-red-50 disabled:opacity-50"
                  title="Delete all data in column family"
                >
                  <TrashIcon className="h-4 w-4 mr-1" />
                  Clear Data
                </button>
              </div>
            </div>

            <div className="pt-4 border-t border-gray-200">
              <h3 className="text-sm font-medium text-gray-700 mb-2 flex items-center">
                <ArchiveBoxIcon className="h-4 w-4 mr-1" />
                Database Backup
              </h3>
              <div className="space-y-2">
                <button
                  onClick={createBackup}
                  disabled={backupLoading}
                  className="w-full inline-flex items-center justify-center px-4 py-2 border border-gray-300 rounded-md shadow-sm text-sm font-medium text-gray-700 bg-white hover:bg-gray-50 disabled:opacity-50"
                >
                  <CloudArrowDownIcon className="h-4 w-4 mr-2" />
                  {backupLoading ? 'Creating Backup...' : 'Create Backup'}
                </button>
                
                {backups.length > 0 && (
                  <div className="bg-gray-50 rounded-md p-3">
                    <h4 className="text-xs font-medium text-gray-700 mb-2">Existing Backups ({backups.length})</h4>
                    <div className="max-h-32 overflow-y-auto space-y-1">
                      {backups.map(backup => (
                        <div key={backup.name} className="flex items-center justify-between text-xs bg-white rounded px-2 py-1 border">
                          <div className="flex-1 min-w-0">
                            <div className="font-medium text-gray-900 truncate">{backup.name}</div>
                            <div className="text-gray-500">{backup.timestamp} • {backup.size}</div>
                          </div>
                          <div className="flex items-center gap-1">
                            <button
                              onClick={() => restoreBackup(backup.name)}
                              disabled={backupLoading}
                              className="text-blue-600 hover:text-blue-900 disabled:opacity-50"
                              title="Restore from this backup"
                            >
                              <CloudArrowDownIcon className="h-3 w-3" />
                            </button>
                            <button
                              onClick={() => deleteBackup(backup.name)}
                              disabled={backupLoading}
                              className="text-red-600 hover:text-red-900 disabled:opacity-50"
                              title="Delete backup"
                            >
                              <TrashIcon className="h-3 w-3" />
                            </button>
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            </div>

            <div className="pt-4 border-t border-gray-200">
              <h3 className="text-sm font-medium text-red-700 mb-2 flex items-center">
                <ExclamationTriangleIcon className="h-4 w-4 mr-1" />
                Danger Zone
              </h3>
              <button
                onClick={() => {
                  if (confirm('Are you sure you want to clear all data? This action cannot be undone.')) {
                    performMaintenance('clear');
                  }
                }}
                disabled={loading}
                className="w-full inline-flex items-center justify-center px-4 py-2 border border-red-300 rounded-md shadow-sm text-sm font-medium text-red-700 bg-red-50 hover:bg-red-100 disabled:opacity-50"
              >
                <TrashIcon className="h-4 w-4 mr-2" />
                Clear All Data
              </button>
            </div>
          </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default RocksDBPage;