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
              <button
                key={index}
                className="flex items-center space-x-3 p-3 rounded-md bg-gray-50 hover:bg-gray-100 transition-colors"
                onClick={action.onClick}
              >
                <div className="text-gray-600">{action.icon}</div>
                <span className="text-sm font-medium text-gray-900">{action.label}</span>
              </button>
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

export default ConfigCard;