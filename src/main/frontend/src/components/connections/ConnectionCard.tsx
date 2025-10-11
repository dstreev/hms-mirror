import React from 'react';
import { 
  PencilIcon, 
  BeakerIcon, 
  DocumentDuplicateIcon, 
  TrashIcon,
  StarIcon
} from '@heroicons/react/24/outline';
import { StarIcon as StarIconSolid } from '@heroicons/react/24/solid';
import { Connection, getConnectionStatusColor, getConnectionStatusIcon } from '../../types/Connection';

interface ConnectionCardProps {
  connection: Connection;
  selected: boolean;
  onSelect: (selected: boolean) => void;
  onEdit: () => void;
  onTest: () => void;
  onDuplicate: () => void;
  onDelete: () => void;
}

const ConnectionCard: React.FC<ConnectionCardProps> = ({
  connection,
  selected,
  onSelect,
  onEdit,
  onTest,
  onDuplicate,
  onDelete
}) => {
  const statusColor = getConnectionStatusColor(connection);
  const statusIcon = getConnectionStatusIcon(connection);
  
  const getTestStatusText = () => {
    if (!connection.testResults || connection.testResults.status === 'NEVER_TESTED') {
      return '⚠️ Never tested';
    }
    
    const lastTested = connection.testResults.lastTested;
    if (!lastTested) return '⚠️ Never tested';
    
    const testDate = new Date(lastTested).toLocaleDateString();
    
    if (connection.testResults.status === 'SUCCESS') {
      return `✅ Last tested: ${testDate}`;
    } else if (connection.testResults.status === 'FAILED') {
      return `❌ Last test failed`;
    } else {
      return `⚠️ Test status unknown`;
    }
  };

  const formatCreatedDate = (dateString: string) => {
    return new Date(dateString).toLocaleDateString();
  };

  return (
    <div className={`bg-white rounded-lg border-2 transition-all duration-200 hover:shadow-md ${
      selected ? 'border-blue-500 bg-blue-50' : 'border-gray-200 hover:border-gray-300'
    }`}>
      <div className="p-6">
        <div className="flex items-start justify-between">
          {/* Main Content */}
          <div className="flex items-start space-x-4 flex-1">
            {/* Selection Checkbox */}
            <div className="flex-shrink-0 mt-1">
              <input
                type="checkbox"
                checked={selected}
                onChange={(e) => onSelect(e.target.checked)}
                className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
              />
            </div>

            {/* Status Icon */}
            <div className="flex-shrink-0">
              <span className="text-2xl">{statusIcon}</span>
            </div>

            {/* Connection Info */}
            <div className="flex-1 min-w-0">
              <div className="flex items-center space-x-2 mb-1">
                <h3 className="text-lg font-semibold text-gray-900 truncate">
                  {connection.name}
                </h3>
                {connection.isDefault && (
                  <StarIconSolid className="h-5 w-5 text-yellow-400 flex-shrink-0" title="Default connection" />
                )}
              </div>
              
              <p className="text-gray-600 mb-2 break-all">{connection.config.hcfsNamespace}</p>
              
              {connection.description && (
                <p className="text-gray-500 text-sm mb-3">{connection.description}</p>
              )}

              <div className="flex flex-wrap gap-4 text-sm text-gray-600">
                <div className="flex items-center">
                  <span className="mr-1">📍</span>
                  Environment: {connection.environment || 'Not specified'}
                </div>
                <div className="flex items-center">
                  <span className="mr-1">📅</span>
                  Created: {formatCreatedDate(connection.created)}
                </div>
                <div className={`flex items-center ${statusColor}`}>
                  {getTestStatusText()}
                </div>
              </div>

              {/* Technical Details */}
              <div className="mt-3 flex flex-wrap gap-4 text-xs text-gray-500">
                <span>Pool: {connection.config.connectionPoolLib}</span>
                <span>Max Connections: {connection.config.connectionPool.max}</span>
                {connection.config.legacy && <span className="text-orange-600">Legacy Mode</span>}
              </div>
            </div>
          </div>

          {/* Action Buttons */}
          <div className="flex-shrink-0 ml-4">
            <div className="flex space-x-2">
              <button
                onClick={onEdit}
                className="inline-flex items-center px-3 py-1.5 border border-gray-300 text-xs font-medium rounded text-gray-700 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
                title="Edit connection"
              >
                <PencilIcon className="h-3 w-3 mr-1" />
                Edit
              </button>
              
              <button
                onClick={onTest}
                className="inline-flex items-center px-3 py-1.5 border border-gray-300 text-xs font-medium rounded text-gray-700 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-green-500"
                title="Test connection"
              >
                <BeakerIcon className="h-3 w-3 mr-1" />
                Test
              </button>
              
              <button
                onClick={onDuplicate}
                className="inline-flex items-center px-3 py-1.5 border border-gray-300 text-xs font-medium rounded text-gray-700 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
                title="Duplicate connection"
              >
                <DocumentDuplicateIcon className="h-3 w-3 mr-1" />
                Copy
              </button>
              
              <button
                onClick={onDelete}
                className="inline-flex items-center px-3 py-1.5 border border-gray-300 text-xs font-medium rounded text-red-700 bg-white hover:bg-red-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-red-500"
                title="Delete connection"
                disabled={connection.isDefault}
              >
                <TrashIcon className="h-3 w-3 mr-1" />
                Delete
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default ConnectionCard;