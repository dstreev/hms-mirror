// src/components/cards/BaseCard.tsx
import React from 'react';
import { motion } from 'framer-motion';
import clsx from 'clsx';

interface BaseCardProps {
  title: string;
  description?: string;
  icon?: React.ReactNode;
  children?: React.ReactNode;
  className?: string;
  expandable?: boolean;
  expanded?: boolean;
  onExpand?: () => void;
  onClick?: () => void;
  status?: 'idle' | 'loading' | 'success' | 'error' | 'warning';
  badge?: string | number;
  disabled?: boolean;
  size?: 'small' | 'medium' | 'large';
}

export const BaseCard: React.FC<BaseCardProps> = ({
  title,
  description,
  icon,
  children,
  className,
  expandable = false,
  expanded = false,
  onExpand,
  onClick,
  status = 'idle',
  badge,
  disabled = false,
  size = 'medium'
}) => {
  const cardVariants = {
    idle: { scale: 1, boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.1)' },
    hover: { scale: 1.02, boxShadow: '0 10px 15px -3px rgba(0, 0, 0, 0.1)' },
    tap: { scale: 0.98 },
    expanded: { scale: 1, boxShadow: '0 20px 25px -5px rgba(0, 0, 0, 0.1)' }
  };

  const statusColors = {
    idle: 'border-gray-200 bg-white',
    loading: 'border-blue-200 bg-blue-50',
    success: 'border-green-200 bg-green-50',
    error: 'border-red-200 bg-red-50',
    warning: 'border-yellow-200 bg-yellow-50'
  };

  const sizeClasses = {
    small: 'p-4',
    medium: 'p-6',
    large: 'p-8'
  };

  return (
    <motion.div
      className={clsx(
        'relative rounded-lg border cursor-pointer transition-all duration-200',
        statusColors[status],
        sizeClasses[size],
        disabled && 'opacity-50 cursor-not-allowed',
        expanded && 'col-span-full row-span-2',
        className
      )}
      variants={cardVariants}
      initial="idle"
      whileHover={!disabled ? "hover" : "idle"}
      whileTap={!disabled ? "tap" : "idle"}
      animate={expanded ? "expanded" : "idle"}
      onClick={() => {
        if (!disabled) {
          if (expandable && onExpand) {
            onExpand();
          } else if (onClick) {
            onClick();
          }
        }
      }}
    >
      {/* Badge */}
      {badge && (
        <div className="absolute -top-2 -right-2 bg-blue-500 text-white text-xs rounded-full h-6 w-6 flex items-center justify-center">
          {badge}
        </div>
      )}

      {/* Header */}
      <div className="flex items-center space-x-3 mb-4">
        {icon && (
          <div className={clsx(
            'flex-shrink-0 p-2 rounded-lg',
            status === 'success' && 'bg-green-100 text-green-600',
            status === 'error' && 'bg-red-100 text-red-600',
            status === 'warning' && 'bg-yellow-100 text-yellow-600',
            status === 'loading' && 'bg-blue-100 text-blue-600',
            status === 'idle' && 'bg-gray-100 text-gray-600'
          )}>
            {icon}
          </div>
        )}
        
        <div className="flex-1">
          <h3 className="text-lg font-semibold text-gray-900">{title}</h3>
          {description && (
            <p className="text-sm text-gray-500 mt-1">{description}</p>
          )}
        </div>

        {expandable && (
          <motion.div
            animate={{ rotate: expanded ? 180 : 0 }}
            transition={{ duration: 0.2 }}
          >
            <svg className="w-5 h-5 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
            </svg>
          </motion.div>
        )}
      </div>

      {/* Content */}
      {children && (
        <motion.div
          initial={false}
          animate={{
            height: expanded ? 'auto' : children ? 'auto' : 0,
            opacity: expanded ? 1 : children ? 1 : 0
          }}
          transition={{ duration: 0.3 }}
          className="overflow-hidden"
        >
          {children}
        </motion.div>
      )}

      {/* Status Indicator */}
      {status === 'loading' && (
        <div className="absolute inset-0 flex items-center justify-center bg-white bg-opacity-75">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500"></div>
        </div>
      )}
    </motion.div>
  );
};

// src/components/cards/ConfigCard.tsx
import React, { useState } from 'react';
import { CogIcon, DocumentTextIcon, PlusIcon } from '@heroicons/react/24/outline';
import { BaseCard } from './BaseCard';

interface ConfigCardProps {
  onNewConfig: () => void;
  onLoadConfig: () => void;
  onTemplates: () => void;
  hasConfig: boolean;
  configName?: string;
}

export const ConfigCard: React.FC<ConfigCardProps> = ({
  onNewConfig,
  onLoadConfig,
  onTemplates,
  hasConfig,
  configName
}) => {
  const [expanded, setExpanded] = useState(false);

  const quickActions = [
    { label: 'New Configuration', icon: <PlusIcon className="w-5 h-5" />, onClick: onNewConfig },
    { label: 'Load Configuration', icon: <DocumentTextIcon className="w-5 h-5" />, onClick: onLoadConfig },
    { label: 'Browse Templates', icon: <CogIcon className="w-5 h-5" />, onClick: onTemplates }
  ];

  return (
    <BaseCard
      title="Configuration"
      description={hasConfig ? `Current: ${configName}` : 'Set up migration parameters'}
      icon={<CogIcon className="w-6 h-6" />}
      expandable
      expanded={expanded}
      onExpand={() => setExpanded(!expanded)}
      status={hasConfig ? 'success' : 'idle'}
      badge={hasConfig ? '✓' : undefined}
    >
      {expanded && (
        <div className="space-y-3">
          <h4 className="font-medium text-gray-900 mb-3">Quick Actions</h4>
          <div className="grid grid-cols-1 gap-2">
            {quickActions.map((action, index) => (
              <motion.button
                key={index}
                className="flex items-center space-x-3 p-3 rounded-md bg-gray-50 hover:bg-gray-100 transition-colors"
                onClick={action.onClick}
                whileHover={{ scale: 1.02 }}
                whileTap={{ scale: 0.98 }}
              >
                <div className="text-gray-600">{action.icon}</div>
                <span className="text-sm font-medium text-gray-900">{action.label}</span>
              </motion.button>
            ))}
          </div>
          
          {hasConfig && (
            <div className="mt-4 pt-4 border-t border-gray-200">
              <h5 className="text-sm font-medium text-gray-900 mb-2">Configuration Details</h5>
              <div className="text-xs text-gray-600 space-y-1">
                <div>Strategy: HYBRID</div>
                <div>Databases: 3 selected</div>
                <div>Last Modified: 2 hours ago</div>
              </div>
            </div>
          )}
        </div>
      )}
    </BaseCard>
  );
};

// src/components/cards/ExecuteCard.tsx
import React from 'react';
import { PlayIcon, PauseIcon, StopIcon } from '@heroicons/react/24/outline';
import { BaseCard } from './BaseCard';

interface ExecuteCardProps {
  status: 'idle' | 'running' | 'paused' | 'completed' | 'error';
  progress?: number;
  onStart: () => void;
  onPause: () => void;
  onStop: () => void;
  canExecute: boolean;
}

export const ExecuteCard: React.FC<ExecuteCardProps> = ({
  status,
  progress = 0,
  onStart,
  onPause,
  onStop,
  canExecute
}) => {
  const getStatusInfo = () => {
    switch (status) {
      case 'idle':
        return { text: 'Ready to execute', color: 'text-gray-600', cardStatus: 'idle' as const };
      case 'running':
        return { text: `Running (${Math.round(progress)}%)`, color: 'text-blue-600', cardStatus: 'loading' as const };
      case 'paused':
        return { text: 'Paused', color: 'text-yellow-600', cardStatus: 'warning' as const };
      case 'completed':
        return { text: 'Completed successfully', color: 'text-green-600', cardStatus: 'success' as const };
      case 'error':
        return { text: 'Execution failed', color: 'text-red-600', cardStatus: 'error' as const };
      default:
        return { text: 'Unknown status', color: 'text-gray-600', cardStatus: 'idle' as const };
    }
  };

  const statusInfo = getStatusInfo();

  return (
    <BaseCard
      title="Execute Migration"
      description={statusInfo.text}
      icon={<PlayIcon className="w-6 h-6" />}
      status={statusInfo.cardStatus}
      disabled={!canExecute && status === 'idle'}
    >
      <div className="space-y-4">
        {/* Progress Bar */}
        {status === 'running' && (
          <div className="space-y-2">
            <div className="flex justify-between text-sm">
              <span className="text-gray-600">Progress</span>
              <span className="font-medium">{Math.round(progress)}%</span>
            </div>
            <div className="w-full bg-gray-200 rounded-full h-2">
              <motion.div
                className="bg-blue-600 h-2 rounded-full"
                initial={{ width: 0 }}
                animate={{ width: `${progress}%` }}
                transition={{ duration: 0.3 }}
              />
            </div>
          </div>
        )}

        {/* Action Buttons */}
        <div className="flex space-x-2">
          {status === 'idle' && (
            <button
              onClick={onStart}
              disabled={!canExecute}
              className="flex-1 flex items-center justify-center space-x-2 bg-blue-600 text-white px-4 py-2 rounded-md hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
            >
              <PlayIcon className="w-4 h-4" />
              <span>Start Migration</span>
            </button>
          )}

          {status === 'running' && (
            <>
              <button
                onClick={onPause}
                className="flex-1 flex items-center justify-center space-x-2 bg-yellow-600 text-white px-4 py-2 rounded-md hover:bg-yellow-700 transition-colors"
              >
                <PauseIcon className="w-4 h-4" />
                <span>Pause</span>
              </button>
              <button
                onClick={onStop}
                className="flex-1 flex items-center justify-center space-x-2 bg-red-600 text-white px-4 py-2 rounded-md hover:bg-red-700 transition-colors"
              >
                <StopIcon className="w-4 h-4" />
                <span>Stop</span>
              </button>
            </>
          )}

          {status === 'paused' && (
            <button
              onClick={onStart}
              className="flex-1 flex items-center justify-center space-x-2 bg-green-600 text-white px-4 py-2 rounded-md hover:bg-green-700 transition-colors"
            >
              <PlayIcon className="w-4 h-4" />
              <span>Resume</span>
            </button>
          )}
        </div>

        {/* Status Details */}
        {(status === 'running' || status === 'completed' || status === 'error') && (
          <div className="text-xs text-gray-600 space-y-1">
            <div>Tables processed: 15/23</div>
            <div>Current: customer_data</div>
            <div>Elapsed: 00:05:32</div>
          </div>
        )}
      </div>
    </BaseCard>
  );
};

// src/components/cards/ConnectCard.tsx
import React, { useState } from 'react';
import { LinkIcon, CheckCircleIcon, XCircleIcon } from '@heroicons/react/24/outline';
import { BaseCard } from './BaseCard';

interface ConnectCardProps {
  leftClusterStatus: 'idle' | 'connected' | 'error' | 'validating';
  rightClusterStatus: 'idle' | 'connected' | 'error' | 'validating';
  onValidateConnections: () => void;
  onConfigureConnections: () => void;
}

export const ConnectCard: React.FC<ConnectCardProps> = ({
  leftClusterStatus,
  rightClusterStatus,
  onValidateConnections,
  onConfigureConnections
}) => {
  const [expanded, setExpanded] = useState(false);

  const getClusterStatusInfo = (status: string) => {
    switch (status) {
      case 'connected':
        return { text: 'Connected', color: 'text-green-600', icon: <CheckCircleIcon className="w-4 h-4" /> };
      case 'error':
        return { text: 'Connection Failed', color: 'text-red-600', icon: <XCircleIcon className="w-4 h-4" /> };
      case 'validating':
        return { text: 'Validating...', color: 'text-blue-600', icon: <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-blue-500"></div> };
      default:
        return { text: 'Not Connected', color: 'text-gray-600', icon: <div className="w-4 h-4 rounded-full bg-gray-300"></div> };
    }
  };

  const leftStatus = getClusterStatusInfo(leftClusterStatus);
  const rightStatus = getClusterStatusInfo(rightClusterStatus);
  const bothConnected = leftClusterStatus === 'connected' && rightClusterStatus === 'connected';

  return (
    <BaseCard
      title="Cluster Connections"
      description={bothConnected ? 'Both clusters connected' : 'Configure and validate cluster connectionDtos'}
      icon={<LinkIcon className="w-6 h-6" />}
      expandable
      expanded={expanded}
      onExpand={() => setExpanded(!expanded)}
      status={bothConnected ? 'success' : leftClusterStatus === 'error' || rightClusterStatus === 'error' ? 'error' : 'idle'}
      badge={bothConnected ? '✓' : undefined}
    >
      {expanded && (
        <div className="space-y-4">
          {/* Connection Status */}
          <div className="space-y-3">
            <h4 className="font-medium text-gray-900 mb-3">Connection Status</h4>
            
            <div className="flex items-center justify-between p-3 bg-gray-50 rounded-md">
              <span className="text-sm font-medium">Source (LEFT)</span>
              <div className={`flex items-center space-x-2 ${leftStatus.color}`}>
                {leftStatus.icon}
                <span className="text-sm">{leftStatus.text}</span>
              </div>
            </div>

            <div className="flex items-center justify-between p-3 bg-gray-50 rounded-md">
              <span className="text-sm font-medium">Target (RIGHT)</span>
              <div className={`flex items-center space-x-2 ${rightStatus.color}`}>
                {rightStatus.icon}
                <span className="text-sm">{rightStatus.text}</span>
              </div>
            </div>
          </div>

          {/* Action Buttons */}
          <div className="space-y-2">
            <button
              onClick={onConfigureConnections}
              className="w-full flex items-center justify-center space-x-2 bg-blue-600 text-white px-4 py-2 rounded-md hover:bg-blue-700 transition-colors"
            >
              <span>Configure Connections</span>
            </button>
            <button
              onClick={onValidateConnections}
              className="w-full flex items-center justify-center space-x-2 bg-green-600 text-white px-4 py-2 rounded-md hover:bg-green-700 transition-colors"
            >
              <span>Validate Connections</span>
            </button>
          </div>
        </div>
      )}
    </BaseCard>
  );
};

// src/components/cards/ReportCard.tsx
import React, { useState } from 'react';
import { DocumentTextIcon, ArrowDownTrayIcon, EyeIcon } from '@heroicons/react/24/outline';
import { BaseCard } from './BaseCard';

interface Report {
  id: string;
  name: string;
  date: string;
  status: 'completed' | 'failed';
  type: 'migration' | 'validation' | 'distcp';
}

interface ReportCardProps {
  reports: Report[];
  onViewReport: (reportId: string) => void;
  onDownloadReport: (reportId: string) => void;
  onGenerateNew: () => void;
}

export const ReportCard: React.FC<ReportCardProps> = ({
  reports,
  onViewReport,
  onDownloadReport,
  onGenerateNew
}) => {
  const [expanded, setExpanded] = useState(false);

  return (
    <BaseCard
      title="Reports"
      description={`${reports.length} reports available`}
      icon={<DocumentTextIcon className="w-6 h-6" />}
      expandable
      expanded={expanded}
      onExpand={() => setExpanded(!expanded)}
      status={reports.length > 0 ? 'success' : 'idle'}
      badge={reports.length > 0 ? reports.length : undefined}
    >
      {expanded && (
        <div className="space-y-4">
          {reports.length > 0 ? (
            <div className="space-y-2">
              <h4 className="font-medium text-gray-900 mb-3">Recent Reports</h4>
              {reports.slice(0, 3).map((report) => (
                <div key={report.id} className="flex items-center justify-between p-3 bg-gray-50 rounded-md">
                  <div className="flex-1">
                    <div className="text-sm font-medium text-gray-900">{report.name}</div>
                    <div className="text-xs text-gray-500">{report.date} • {report.type}</div>
                  </div>
                  <div className="flex items-center space-x-2">
                    <div className={`w-2 h-2 rounded-full ${report.status === 'completed' ? 'bg-green-500' : 'bg-red-500'}`}></div>
                    <button
                      onClick={() => onViewReport(report.id)}
                      className="text-blue-600 hover:text-blue-800"
                    >
                      <EyeIcon className="w-4 h-4" />
                    </button>
                    <button
                      onClick={() => onDownloadReport(report.id)}
                      className="text-gray-600 hover:text-gray-800"
                    >
                      <ArrowDownTrayIcon className="w-4 h-4" />
                    </button>
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <div className="text-center py-4">
              <p className="text-sm text-gray-500 mb-3">No reports available</p>
            </div>
          )}
          
          <button
            onClick={onGenerateNew}
            className="w-full flex items-center justify-center space-x-2 bg-blue-600 text-white px-4 py-2 rounded-md hover:bg-blue-700 transition-colors"
          >
            <span>Generate New Report</span>
          </button>
        </div>
      )}
    </BaseCard>
  );
};

// src/components/cards/DatabaseCard.tsx
import React, { useState } from 'react';
import { CircleStackIcon, FunnelIcon } from '@heroicons/react/24/outline';
import { BaseCard } from './BaseCard';

interface DatabaseInfo {
  name: string;
  tables: number;
  selected: boolean;
}

interface DatabaseCardProps {
  databases: DatabaseInfo[];
  totalSelected: number;
  onManageDatabases: () => void;
  onSetFilters: () => void;
}

export const DatabaseCard: React.FC<DatabaseCardProps> = ({
  databases,
  totalSelected,
  onManageDatabases,
  onSetFilters
}) => {
  const [expanded, setExpanded] = useState(false);

  return (
    <BaseCard
      title="Databases"
      description={`${totalSelected} of ${databases.length} databases selected`}
      icon={<CircleStackIcon className="w-6 h-6" />}
      expandable
      expanded={expanded}
      onExpand={() => setExpanded(!expanded)}
      status={totalSelected > 0 ? 'success' : 'idle'}
      badge={totalSelected > 0 ? totalSelected : undefined}
    >
      {expanded && (
        <div className="space-y-4">
          {databases.length > 0 ? (
            <div className="space-y-2">
              <h4 className="font-medium text-gray-900 mb-3">Selected Databases</h4>
              <div className="max-h-32 overflow-y-auto space-y-1">
                {databases.filter(db => db.selected).map((db) => (
                  <div key={db.name} className="flex items-center justify-between p-2 bg-green-50 rounded">
                    <span className="text-sm font-medium text-gray-900">{db.name}</span>
                    <span className="text-xs text-gray-500">{db.tables} tables</span>
                  </div>
                ))}
              </div>
              
              {databases.filter(db => !db.selected).length > 0 && (
                <>
                  <h5 className="text-sm font-medium text-gray-700 mt-3">Available Databases</h5>
                  <div className="max-h-24 overflow-y-auto space-y-1">
                    {databases.filter(db => !db.selected).slice(0, 3).map((db) => (
                      <div key={db.name} className="flex items-center justify-between p-2 bg-gray-50 rounded">
                        <span className="text-sm text-gray-600">{db.name}</span>
                        <span className="text-xs text-gray-400">{db.tables} tables</span>
                      </div>
                    ))}
                  </div>
                </>
              )}
            </div>
          ) : (
            <div className="text-center py-4">
              <p className="text-sm text-gray-500 mb-3">No databases discovered</p>
            </div>
          )}
          
          <div className="flex space-x-2">
            <button
              onClick={onManageDatabases}
              className="flex-1 flex items-center justify-center space-x-2 bg-blue-600 text-white px-3 py-2 rounded-md hover:bg-blue-700 transition-colors"
            >
              <span className="text-sm">Manage</span>
            </button>
            <button
              onClick={onSetFilters}
              className="flex-1 flex items-center justify-center space-x-2 bg-gray-600 text-white px-3 py-2 rounded-md hover:bg-gray-700 transition-colors"
            >
              <FunnelIcon className="w-4 h-4" />
              <span className="text-sm">Filters</span>
            </button>
          </div>
        </div>
      )}
    </BaseCard>
  );
};

// src/components/cards/CardGrid.tsx
import React from 'react';
import { motion } from 'framer-motion';

interface CardGridProps {
  children: React.ReactNode;
  columns?: number;
  gap?: number;
}

export const CardGrid: React.FC<CardGridProps> = ({ 
  children, 
  columns = 4, 
  gap = 6 
}) => {
  const containerVariants = {
    hidden: { opacity: 0 },
    visible: {
      opacity: 1,
      transition: {
        staggerChildren: 0.1,
        delayChildren: 0.2
      }
    }
  };

  const itemVariants = {
    hidden: { y: 20, opacity: 0 },
    visible: {
      y: 0,
      opacity: 1,
      transition: {
        type: "spring",
        damping: 20,
        stiffness: 300
      }
    }
  };

  return (
    <motion.div
      className={`grid gap-${gap} auto-rows-fr`}
      style={{
        gridTemplateColumns: `repeat(${columns}, minmax(0, 1fr))`
      }}
      variants={containerVariants}
      initial="hidden"
      animate="visible"
    >
      {React.Children.map(children, (child, index) => (
        <motion.div key={index} variants={itemVariants}>
          {child}
        </motion.div>
      ))}
    </motion.div>
  );
};

// src/components/dashboard/Dashboard.tsx
import React, { useState, useEffect } from 'react';
import { CardGrid } from '../cards/CardGrid';
import { ConfigCard } from '../cards/ConfigCard';
import { ConnectCard } from '../cards/ConnectCard';
import { DatabaseCard } from '../cards/DatabaseCard';
import { ExecuteCard } from '../cards/ExecuteCard';
import { ReportCard } from '../cards/ReportCard';

interface DashboardState {
  config: {
    hasConfig: boolean;
    configName?: string;
  };
  connectionDtos: {
    leftStatus: 'idle' | 'connected' | 'error' | 'validating';
    rightStatus: 'idle' | 'connected' | 'error' | 'validating';
  };
  databases: {
    list: Array<{
      name: string;
      tables: number;
      selected: boolean;
    }>;
    totalSelected: number;
  };
  execution: {
    status: 'idle' | 'running' | 'paused' | 'completed' | 'error';
    progress: number;
    canExecute: boolean;
  };
  reports: Array<{
    id: string;
    name: string;
    date: string;
    status: 'completed' | 'failed';
    type: 'migration' | 'validation' | 'distcp';
  }>;
}

export const Dashboard: React.FC = () => {
  const [state, setState] = useState<DashboardState>({
    config: { hasConfig: false },
    connectionDtos: { leftStatus: 'idle', rightStatus: 'idle' },
    databases: { list: [], totalSelected: 0 },
    execution: { status: 'idle', progress: 0, canExecute: false },
    reports: []
  });

  const handleNewConfig = () => {
    console.log('Creating new configuration...');
  };

  const handleLoadConfig = () => {
    console.log('Loading configuration...');
  };

  const handleTemplates = () => {
    console.log('Browsing templates...');
  };

  const handleValidateConnections = () => {
    setState(prev => ({
      ...prev,
      connectionDtos: {
        leftStatus: 'validating',
        rightStatus: 'validating'
      }
    }));

    setTimeout(() => {
      setState(prev => ({
        ...prev,
        connectionDtos: {
          leftStatus: 'connected',
          rightStatus: 'connected'
        }
      }));
    }, 2000);
  };

  const handleConfigureConnections = () => {
    console.log('Opening connection configuration...');
  };

  const handleManageDatabases = () => {
    console.log('Opening database management...');
  };

  const handleSetFilters = () => {
    console.log('Opening filter settings...');
  };

  const handleStart = () => {
    if (state.execution.status === 'idle') {
      setState(prev => ({
        ...prev,
        execution: { ...prev.execution, status: 'running' }
      }));
      simulateProgress();
    } else if (state.execution.status === 'paused') {
      setState(prev => ({
        ...prev,
        execution: { ...prev.execution, status: 'running' }
      }));
    }
  };

  const handlePause = () => {
    setState(prev => ({
      ...prev,
      execution: { ...prev.execution, status: 'paused' }
    }));
  };

  const handleStop = () => {
    setState(prev => ({
      ...prev,
      execution: { 
        ...prev.execution, 
        status: 'idle',
        progress: 0
      }
    }));
  };

  const simulateProgress = () => {
    let progress = 0;
    const interval = setInterval(() => {
      progress += Math.random() * 10;
      if (progress >= 100) {
        progress = 100;
        setState(prev => ({
          ...prev,
          execution: {
            ...prev.execution,
            status: 'completed',
            progress: 100
          }
        }));
        clearInterval(interval);
      } else {
        setState(prev => ({
          ...prev,
          execution: {
            ...prev.execution,
            progress
          }
        }));
      }
    }, 500);
  };

  const handleViewReport = (reportId: string) => {
    console.log(`Viewing report: ${reportId}`);
  };

  const handleDownloadReport = (reportId: string) => {
    console.log(`Downloading report: ${reportId}`);
  };

  const handleGenerateNew = () => {
    console.log('Generating new report...');
  };

  useEffect(() => {
    const canExecute = state.config.hasConfig && 
                      state.connectionDtos.leftStatus === 'connected' &&
                      state.connectionDtos.rightStatus === 'connected' &&
                      state.databases.totalSelected > 0;
    
    setState(prev => ({
      ...prev,
      execution: { ...prev.execution, canExecute }
    }));
  }, [state.config.hasConfig, state.connectionDtos, state.databases.totalSelected]);

  return (
    <div className="min-h-screen bg-gray-50 py-8">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="mb-8">
          <h1 className="text-3xl font-bold text-gray-900">HMS-Mirror Dashboard</h1>
          <p className="text-gray-600">Manage your Hive Metastore migration workflow</p>
        </div>

        <CardGrid columns={4} gap={6}>
          <ConfigCard
            onNewConfig={handleNewConfig}
            onLoadConfig={handleLoadConfig}
            onTemplates={handleTemplates}
            hasConfig={state.config.hasConfig}
            configName={state.config.configName}
          />

          <ConnectCard
            leftClusterStatus={state.connectionDtos.leftStatus}
            rightClusterStatus={state.connectionDtos.rightStatus}
            onValidateConnections={handleValidateConnections}
            onConfigureConnections={handleConfigureConnections}
          />

          <DatabaseCard
            databases={state.databases.list}
            totalSelected={state.databases.totalSelected}
            onManageDatabases={handleManageDatabases}
            onSetFilters={handleSetFilters}
          />

          <ExecuteCard
            status={state.execution.status}
            progress={state.execution.progress}
            onStart={handleStart}
            onPause={handlePause}
            onStop={handleStop}
            canExecute={state.execution.canExecute}
          />

          <ReportCard
            reports={state.reports}
            onViewReport={handleViewReport}
            onDownloadReport={handleDownloadReport}
            onGenerateNew={handleGenerateNew}
          />
        </CardGrid>
      </div>
    </div>
  );
};