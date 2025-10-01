import React, { useState, useMemo } from 'react';
import yaml from 'js-yaml';
import { 
  ChevronRightIcon, 
  ChevronDownIcon,
  DocumentTextIcon,
  FolderIcon,
  KeyIcon,
  CubeIcon,
  ServerIcon,
  CogIcon
} from '@heroicons/react/24/outline';

interface YamlViewerProps {
  content: string;
  title: string;
}

interface TreeNodeProps {
  keyName: string;
  value: any;
  level: number;
  isLast?: boolean;
}

const TreeNode: React.FC<TreeNodeProps> = ({ keyName, value, level, isLast }) => {
  const [isExpanded, setIsExpanded] = useState(level < 2); // Auto-expand first 2 levels
  
  const isObject = value !== null && typeof value === 'object' && !Array.isArray(value);
  const isArray = Array.isArray(value);
  const hasChildren = isObject || isArray;
  
  // Get appropriate icon based on key name or value type
  const getIcon = () => {
    const lowerKey = keyName.toLowerCase();
    if (lowerKey.includes('cluster') || lowerKey.includes('server')) {
      return <ServerIcon className="h-4 w-4" />;
    }
    if (lowerKey.includes('config') || lowerKey.includes('setting')) {
      return <CogIcon className="h-4 w-4" />;
    }
    if (lowerKey.includes('database') || lowerKey.includes('table')) {
      return <CubeIcon className="h-4 w-4" />;
    }
    if (lowerKey.includes('key') || lowerKey.includes('password') || lowerKey.includes('secret')) {
      return <KeyIcon className="h-4 w-4" />;
    }
    if (hasChildren) {
      return <FolderIcon className="h-4 w-4" />;
    }
    return <DocumentTextIcon className="h-4 w-4" />;
  };

  // Get color based on value type or content
  const getValueColor = (val: any) => {
    if (typeof val === 'boolean') {
      return val ? 'text-green-600' : 'text-red-600';
    }
    if (typeof val === 'number') {
      return 'text-blue-600';
    }
    if (typeof val === 'string') {
      if (val.toLowerCase() === 'success' || val.toLowerCase() === 'completed') {
        return 'text-green-600';
      }
      if (val.toLowerCase() === 'error' || val.toLowerCase() === 'failed') {
        return 'text-red-600';
      }
      if (val.toLowerCase() === 'warning' || val.toLowerCase() === 'partial') {
        return 'text-yellow-600';
      }
      return 'text-gray-800';
    }
    return 'text-gray-600';
  };

  // Format value for display
  const formatValue = (val: any): string => {
    if (val === null || val === undefined) {
      return 'null';
    }
    if (typeof val === 'boolean') {
      return val ? 'true' : 'false';
    }
    if (typeof val === 'string') {
      // Mask sensitive values
      const lowerKey = keyName.toLowerCase();
      if (lowerKey.includes('password') || lowerKey.includes('secret') || lowerKey.includes('key')) {
        return '••••••••';
      }
      return val;
    }
    return String(val);
  };

  const indent = level * 24;

  return (
    <div className={`${!isLast ? 'border-l border-gray-200' : ''}`} style={{ marginLeft: `${indent}px` }}>
      <div className="flex items-center py-1 hover:bg-gray-50 rounded px-2 -ml-2">
        {hasChildren && (
          <button
            onClick={() => setIsExpanded(!isExpanded)}
            className="mr-1 text-gray-400 hover:text-gray-600"
          >
            {isExpanded ? (
              <ChevronDownIcon className="h-4 w-4" />
            ) : (
              <ChevronRightIcon className="h-4 w-4" />
            )}
          </button>
        )}
        {!hasChildren && <div className="w-5" />}
        
        <span className="text-gray-400 mr-2">{getIcon()}</span>
        
        <span className="font-medium text-gray-700">{keyName}</span>
        
        {!hasChildren && (
          <>
            <span className="text-gray-400 mx-2">:</span>
            <span className={`${getValueColor(value)} font-mono text-sm`}>
              {formatValue(value)}
            </span>
          </>
        )}
        
        {isArray && (
          <span className="ml-2 text-xs text-gray-500">
            [{value.length} items]
          </span>
        )}
      </div>
      
      {hasChildren && isExpanded && (
        <div className="ml-2">
          {isObject && Object.entries(value).map(([key, val], index) => (
            <TreeNode
              key={key}
              keyName={key}
              value={val}
              level={level + 1}
              isLast={index === Object.entries(value).length - 1}
            />
          ))}
          
          {isArray && value.map((item: any, index: number) => (
            <TreeNode
              key={index}
              keyName={`[${index}]`}
              value={item}
              level={level + 1}
              isLast={index === value.length - 1}
            />
          ))}
        </div>
      )}
    </div>
  );
};

const YamlViewer: React.FC<YamlViewerProps> = ({ content, title }) => {
  const [viewMode, setViewMode] = useState<'tree' | 'raw'>('tree');
  const [searchTerm, setSearchTerm] = useState('');
  
  const parsedYaml = useMemo(() => {
    try {
      return yaml.load(content);
    } catch (error) {
      console.error('Failed to parse YAML:', error);
      return null;
    }
  }, [content]);

  const filteredContent = useMemo(() => {
    if (!searchTerm || viewMode === 'raw') return parsedYaml;
    
    // Simple filter implementation - could be enhanced
    const filterObject = (obj: any): any => {
      if (!obj || typeof obj !== 'object') return obj;
      
      const filtered: any = {};
      Object.entries(obj).forEach(([key, value]) => {
        if (key.toLowerCase().includes(searchTerm.toLowerCase()) ||
            JSON.stringify(value).toLowerCase().includes(searchTerm.toLowerCase())) {
          filtered[key] = value;
        }
      });
      return Object.keys(filtered).length > 0 ? filtered : null;
    };
    
    return filterObject(parsedYaml);
  }, [parsedYaml, searchTerm, viewMode]);

  if (!parsedYaml) {
    return (
      <div className="bg-red-50 border border-red-200 rounded-lg p-4">
        <p className="text-red-700">Failed to parse YAML content</p>
        <pre className="mt-2 text-xs text-red-600 font-mono whitespace-pre-wrap">
          {content.substring(0, 500)}...
        </pre>
      </div>
    );
  }

  return (
    <div className="bg-white rounded-lg border border-gray-200">
      <div className="px-4 py-3 border-b border-gray-200">
        <div className="flex items-center justify-between">
          <h3 className="text-lg font-medium text-gray-900 flex items-center">
            <DocumentTextIcon className="h-5 w-5 mr-2 text-gray-400" />
            {title}
          </h3>
          
          <div className="flex items-center space-x-4">
            {/* Search box */}
            {viewMode === 'tree' && (
              <div className="relative">
                <input
                  type="text"
                  placeholder="Search..."
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                  className="pl-8 pr-3 py-1 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                />
                <svg className="absolute left-2 top-1.5 h-4 w-4 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
                </svg>
              </div>
            )}
            
            {/* View mode toggle */}
            <div className="flex bg-gray-100 rounded-md p-1">
              <button
                onClick={() => setViewMode('tree')}
                className={`px-3 py-1 text-sm font-medium rounded ${
                  viewMode === 'tree'
                    ? 'bg-white text-blue-600 shadow-sm'
                    : 'text-gray-600 hover:text-gray-900'
                }`}
              >
                Tree View
              </button>
              <button
                onClick={() => setViewMode('raw')}
                className={`px-3 py-1 text-sm font-medium rounded ${
                  viewMode === 'raw'
                    ? 'bg-white text-blue-600 shadow-sm'
                    : 'text-gray-600 hover:text-gray-900'
                }`}
              >
                Raw YAML
              </button>
            </div>
          </div>
        </div>
      </div>
      
      <div className="p-4 max-h-[600px] overflow-y-auto">
        {viewMode === 'tree' ? (
          filteredContent ? (
            <div className="font-mono text-sm">
              {Object.entries(filteredContent).map(([key, value], index) => (
                <TreeNode
                  key={key}
                  keyName={key}
                  value={value}
                  level={0}
                  isLast={index === Object.entries(filteredContent).length - 1}
                />
              ))}
            </div>
          ) : (
            <div className="text-gray-500 text-center py-8">
              No matching content found
            </div>
          )
        ) : (
          <pre className="text-xs font-mono text-gray-800 whitespace-pre-wrap bg-gray-50 rounded p-4">
            {content}
          </pre>
        )}
      </div>
    </div>
  );
};

export default YamlViewer;