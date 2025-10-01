import React, { useState, useMemo } from 'react';
import yaml from 'js-yaml';
import { 
  ChevronRightIcon, 
  ChevronDownIcon,
  CircleStackIcon,
  FolderIcon,
  ArrowRightIcon,
  ServerIcon,
  DocumentDuplicateIcon,
  ArrowsRightLeftIcon
} from '@heroicons/react/24/outline';

interface DistcpPlanViewerProps {
  content: string;
  title: string;
  environment: 'LEFT' | 'RIGHT';
}

interface DistcpPlan {
  [database: string]: {
    [target: string]: string[];
  };
}

interface PlanNodeProps {
  database: string;
  targets: { [target: string]: string[] };
  environment: 'LEFT' | 'RIGHT';
}

const PlanNode: React.FC<PlanNodeProps> = ({ database, targets, environment }) => {
  const [isExpanded, setIsExpanded] = useState(true);
  const [expandedTargets, setExpandedTargets] = useState<{ [key: string]: boolean }>({});

  const toggleTarget = (target: string) => {
    setExpandedTargets(prev => ({
      ...prev,
      [target]: !prev[target]
    }));
  };

  const formatPath = (path: string) => {
    // Extract just the meaningful part of the path for display
    const parts = path.split('/');
    if (parts.length > 3) {
      return `.../${parts.slice(-3).join('/')}`;
    }
    return path;
  };

  const getProtocol = (path: string) => {
    const match = path.match(/^([a-z]+):\/\//);
    return match ? match[1].toUpperCase() : 'HDFS';
  };

  const totalSources = Object.values(targets).reduce((acc, sources) => acc + sources.length, 0);
  const envColor = environment === 'LEFT' ? 'blue' : 'green';

  return (
    <div className="border border-gray-200 rounded-lg p-4 mb-4">
      <div 
        className="flex items-center cursor-pointer hover:bg-gray-50 rounded p-2 -m-2"
        onClick={() => setIsExpanded(!isExpanded)}
      >
        <button className="mr-2 text-gray-400 hover:text-gray-600">
          {isExpanded ? (
            <ChevronDownIcon className="h-5 w-5" />
          ) : (
            <ChevronRightIcon className="h-5 w-5" />
          )}
        </button>
        <CircleStackIcon className={`h-5 w-5 text-${envColor}-500 mr-2`} />
        <span className="font-semibold text-gray-900">{database}</span>
        <span className="ml-auto text-sm text-gray-500">
          {Object.keys(targets).length} target{Object.keys(targets).length !== 1 ? 's' : ''}, 
          {' '}{totalSources} source{totalSources !== 1 ? 's' : ''}
        </span>
      </div>

      {isExpanded && (
        <div className="mt-4 ml-7">
          {Object.entries(targets).map(([target, sources], targetIndex) => (
            <div key={targetIndex} className="mb-4 last:mb-0">
              <div 
                className="flex items-start cursor-pointer hover:bg-gray-50 rounded p-2 -mx-2"
                onClick={() => toggleTarget(target)}
              >
                <button className="mr-2 mt-0.5 text-gray-400 hover:text-gray-600">
                  {expandedTargets[target] ? (
                    <ChevronDownIcon className="h-4 w-4" />
                  ) : (
                    <ChevronRightIcon className="h-4 w-4" />
                  )}
                </button>
                <div className="flex-1">
                  <div className="flex items-center">
                    <FolderIcon className="h-4 w-4 text-green-500 mr-2" />
                    <span className="text-sm font-medium text-gray-700">Target Location</span>
                    <span className={`ml-2 px-2 py-0.5 text-xs rounded-full bg-${envColor}-100 text-${envColor}-700`}>
                      {getProtocol(target)}
                    </span>
                    <span className="ml-auto text-xs text-gray-500">
                      {sources.length} source{sources.length !== 1 ? 's' : ''}
                    </span>
                  </div>
                  <div className="mt-1 font-mono text-xs text-gray-600 break-all bg-gray-50 rounded px-2 py-1">
                    {target}
                  </div>
                </div>
              </div>

              {(expandedTargets[target] !== false) && (
                <div className="ml-8 mt-2">
                  <div className="text-xs font-medium text-gray-500 mb-2 flex items-center">
                    <ArrowRightIcon className="h-3 w-3 mr-1" />
                    Source Directories
                  </div>
                  <div className="space-y-2">
                    {sources.map((source, sourceIndex) => {
                      const sourceProtocol = getProtocol(source);
                      return (
                        <div 
                          key={sourceIndex}
                          className="flex items-start bg-white border border-gray-200 rounded-lg p-3 hover:shadow-sm transition-shadow"
                        >
                          <ServerIcon className="h-4 w-4 text-gray-400 mr-2 mt-0.5 flex-shrink-0" />
                          <div className="flex-1 min-w-0">
                            <div className="flex items-center mb-1">
                              <span className="px-2 py-0.5 text-xs rounded-full bg-gray-100 text-gray-700 font-medium">
                                {sourceProtocol}
                              </span>
                              <ArrowsRightLeftIcon className="h-3 w-3 mx-2 text-gray-400" />
                              <span className={`px-2 py-0.5 text-xs rounded-full bg-${envColor}-100 text-${envColor}-700 font-medium`}>
                                {getProtocol(target)}
                              </span>
                            </div>
                            <div className="font-mono text-xs text-gray-700 break-all">
                              {source}
                            </div>
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </div>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
};

const DistcpPlanViewer: React.FC<DistcpPlanViewerProps> = ({ content, title, environment }) => {
  const [searchTerm, setSearchTerm] = useState('');
  const [viewMode, setViewMode] = useState<'visual' | 'raw'>('visual');

  const parsedPlan = useMemo(() => {
    try {
      return yaml.load(content) as DistcpPlan;
    } catch (error) {
      console.error('Failed to parse DistCp plan YAML:', error);
      return null;
    }
  }, [content]);

  const filteredPlan = useMemo(() => {
    if (!parsedPlan || !searchTerm || viewMode === 'raw') return parsedPlan;

    const filtered: DistcpPlan = {};
    
    Object.entries(parsedPlan).forEach(([database, targets]) => {
      // Check if database matches
      if (database.toLowerCase().includes(searchTerm.toLowerCase())) {
        filtered[database] = targets;
        return;
      }

      // Check targets and sources
      const filteredTargets: { [target: string]: string[] } = {};
      Object.entries(targets).forEach(([target, sources]) => {
        const matchingTarget = target.toLowerCase().includes(searchTerm.toLowerCase());
        const matchingSources = sources.filter(source => 
          source.toLowerCase().includes(searchTerm.toLowerCase())
        );

        if (matchingTarget || matchingSources.length > 0) {
          filteredTargets[target] = matchingTarget ? sources : matchingSources;
        }
      });

      if (Object.keys(filteredTargets).length > 0) {
        filtered[database] = filteredTargets;
      }
    });

    return Object.keys(filtered).length > 0 ? filtered : null;
  }, [parsedPlan, searchTerm, viewMode]);

  const statistics = useMemo(() => {
    if (!parsedPlan) return null;

    let totalDatabases = 0;
    let totalTargets = 0;
    let totalSources = 0;
    const protocols = new Set<string>();

    Object.entries(parsedPlan).forEach(([_, targets]) => {
      totalDatabases++;
      Object.entries(targets).forEach(([target, sources]) => {
        totalTargets++;
        totalSources += sources.length;
        
        // Extract protocols
        const targetMatch = target.match(/^([a-z]+):\/\//);
        if (targetMatch) protocols.add(targetMatch[1].toUpperCase());
        
        sources.forEach(source => {
          const sourceMatch = source.match(/^([a-z]+):\/\//);
          if (sourceMatch) protocols.add(sourceMatch[1].toUpperCase());
        });
      });
    });

    return {
      databases: totalDatabases,
      targets: totalTargets,
      sources: totalSources,
      protocols: Array.from(protocols)
    };
  }, [parsedPlan]);

  if (!parsedPlan) {
    return (
      <div className="bg-red-50 border border-red-200 rounded-lg p-4">
        <p className="text-red-700">Failed to parse DistCp plan YAML</p>
        <pre className="mt-2 text-xs text-red-600 font-mono whitespace-pre-wrap">
          {content.substring(0, 500)}...
        </pre>
      </div>
    );
  }

  const envColor = environment === 'LEFT' ? 'blue' : 'green';
  const envIcon = environment === 'LEFT' ? '←' : '→';

  return (
    <div className="bg-white rounded-lg border border-gray-200">
      <div className="px-4 py-3 border-b border-gray-200">
        <div className="flex items-center justify-between">
          <div className="flex items-center">
            <DocumentDuplicateIcon className="h-5 w-5 mr-2 text-gray-400" />
            <h3 className="text-lg font-medium text-gray-900">{title}</h3>
            <span className={`ml-3 px-2.5 py-1 text-xs font-medium rounded-full bg-${envColor}-100 text-${envColor}-700 border border-${envColor}-200`}>
              {envIcon} {environment}
            </span>
          </div>
          
          <div className="flex items-center space-x-4">
            {viewMode === 'visual' && (
              <div className="relative">
                <input
                  type="text"
                  placeholder="Search databases, paths..."
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                  className="pl-8 pr-3 py-1 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                />
                <svg className="absolute left-2 top-1.5 h-4 w-4 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
                </svg>
              </div>
            )}
            
            <div className="flex bg-gray-100 rounded-md p-1">
              <button
                onClick={() => setViewMode('visual')}
                className={`px-3 py-1 text-sm font-medium rounded ${
                  viewMode === 'visual'
                    ? 'bg-white text-blue-600 shadow-sm'
                    : 'text-gray-600 hover:text-gray-900'
                }`}
              >
                Visual
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

        {statistics && viewMode === 'visual' && (
          <div className="mt-3 flex items-center space-x-4 text-sm text-gray-600">
            <span>
              <span className="font-medium">{statistics.databases}</span> database{statistics.databases !== 1 ? 's' : ''}
            </span>
            <span>•</span>
            <span>
              <span className="font-medium">{statistics.targets}</span> target{statistics.targets !== 1 ? 's' : ''}
            </span>
            <span>•</span>
            <span>
              <span className="font-medium">{statistics.sources}</span> source{statistics.sources !== 1 ? 's' : ''}
            </span>
            {statistics.protocols.length > 0 && (
              <>
                <span>•</span>
                <span>
                  Protocols: {statistics.protocols.map(p => (
                    <span key={p} className="px-1.5 py-0.5 ml-1 text-xs bg-gray-100 rounded">
                      {p}
                    </span>
                  ))}
                </span>
              </>
            )}
          </div>
        )}
      </div>
      
      <div className="p-4 max-h-[600px] overflow-y-auto">
        {viewMode === 'visual' ? (
          filteredPlan ? (
            <div className="space-y-4">
              {Object.entries(filteredPlan).map(([database, targets]) => (
                <PlanNode
                  key={database}
                  database={database}
                  targets={targets}
                  environment={environment}
                />
              ))}
            </div>
          ) : (
            <div className="text-gray-500 text-center py-8">
              {searchTerm ? 'No matching content found' : 'No DistCp plans available'}
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

export default DistcpPlanViewer;