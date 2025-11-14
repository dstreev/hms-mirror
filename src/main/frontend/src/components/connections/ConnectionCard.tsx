import React from 'react';
import {
  PencilIcon,
  BeakerIcon,
  DocumentDuplicateIcon,
  TrashIcon,
  ArrowDownTrayIcon
} from '@heroicons/react/24/outline';
import { Connection, getConnectionStatusColor, getConnectionStatusIcon } from '../../types/Connection';

interface ConnectionCardProps {
  connection: Connection;
  selected: boolean;
  onSelect: (selected: boolean) => void;
  onEdit: () => void;
  onTest: () => void;
  onDuplicate: () => void;
  onDelete: () => void;
  onExport: () => void;
}

const ConnectionCard: React.FC<ConnectionCardProps> = ({
  connection,
  selected,
  onSelect,
  onEdit,
  onTest,
  onDuplicate,
  onDelete,
  onExport
}) => {
  const statusColor = getConnectionStatusColor(connection);
  const statusIcon = getConnectionStatusIcon(connection);

  const formatCreatedDate = (dateString: string) => {
    return new Date(dateString).toLocaleDateString();
  };

  return (
    <div className={`bg-white rounded-lg border-2 transition-all duration-200 hover:shadow-md flex flex-col ${
      selected ? 'border-blue-500 bg-blue-50' : 'border-gray-200 hover:border-gray-300'
    }`}>
      <div className="p-6 flex-1">
        {/* Header with Trash Icon */}
        <div className="flex items-start justify-between mb-4">
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

            {/* Connection Name */}
            <div className="flex-1 min-w-0">
              <h3 className="text-lg font-semibold text-gray-900 truncate">
                {connection.name}
              </h3>
            </div>
          </div>

          {/* Trash Icon */}
          <button
            onClick={onDelete}
            className="flex-shrink-0 p-2 text-red-600 hover:bg-red-50 rounded-md transition-colors"
            title="Delete connection"
          >
            <TrashIcon className="h-5 w-5" />
          </button>
        </div>

        {/* Connection Info */}
        <div className="ml-12">
          <p className="text-gray-600 mb-2 break-all">{connection.config.hcfsNamespace}</p>

          {connection.description && (
            <p className="text-gray-500 text-sm mb-3">{connection.description}</p>
          )}

          <div className="flex flex-wrap gap-4 text-sm text-gray-600">
            <div className="flex items-center">
              <span className="mr-1">🖥️</span>
              Platform: {connection.config.platformType || 'Not specified'}
            </div>
            <div className="flex items-center">
              <span className="mr-1">📍</span>
              Environment: {connection.environment || 'Not specified'}
            </div>
            <div className="flex items-center">
              <span className="mr-1">📅</span>
              Created: {formatCreatedDate(connection.created)}
            </div>
          </div>

          {/* Connection Endpoint Test Results - Always show all endpoints */}
          <div className="mt-3 space-y-2">
            <div className="text-xs font-semibold text-gray-700 mb-2">Connection Test Status:</div>

            {/* HCFS Namespace Test Result */}
            <div className="flex items-center text-xs">
              <span className="w-40 font-medium text-gray-700">HCFS Namespace:</span>
              {connection.hcfsTestResults ? (
                <span className={connection.hcfsTestResults.status === 'SUCCESS' ? 'text-green-600' : connection.hcfsTestResults.status === 'FAILED' ? 'text-red-600' : 'text-yellow-600'}>
                  {connection.hcfsTestResults.status === 'SUCCESS' ? '✅ Connected' : connection.hcfsTestResults.status === 'FAILED' ? '❌ Failed' : '⚠️ Never Tested'}
                  {connection.hcfsTestResults.lastTested && (
                    <span className="text-gray-400 ml-1">
                      ({new Date(connection.hcfsTestResults.lastTested).toLocaleDateString()})
                    </span>
                  )}
                </span>
              ) : (
                <span className="text-yellow-600">⚠️ Never Tested</span>
              )}
            </div>

            {/* HiveServer2 Test Result */}
            <div className="flex items-center text-xs">
              <span className="w-40 font-medium text-gray-700">HiveServer2:</span>
              {connection.hs2TestResults ? (
                <span className={connection.hs2TestResults.status === 'SUCCESS' ? 'text-green-600' : connection.hs2TestResults.status === 'FAILED' ? 'text-red-600' : 'text-yellow-600'}>
                  {connection.hs2TestResults.status === 'SUCCESS' ? '✅ Connected' : connection.hs2TestResults.status === 'FAILED' ? '❌ Failed' : '⚠️ Never Tested'}
                  {connection.hs2TestResults.lastTested && (
                    <span className="text-gray-400 ml-1">
                      ({new Date(connection.hs2TestResults.lastTested).toLocaleDateString()})
                    </span>
                  )}
                </span>
              ) : (
                <span className="text-yellow-600">⚠️ Never Tested</span>
              )}
            </div>

            {/* Metastore Direct Test Result */}
            <div className="flex items-center text-xs">
              <span className="w-40 font-medium text-gray-700">Metastore Direct:</span>
              {connection.metastoreDirectTestResults ? (
                <span className={connection.metastoreDirectTestResults.status === 'SUCCESS' ? 'text-green-600' : connection.metastoreDirectTestResults.status === 'FAILED' ? 'text-red-600' : 'text-yellow-600'}>
                  {connection.metastoreDirectTestResults.status === 'SUCCESS' ? '✅ Connected' : connection.metastoreDirectTestResults.status === 'FAILED' ? '❌ Failed' : '⚠️ Never Tested'}
                  {connection.metastoreDirectTestResults.lastTested && (
                    <span className="text-gray-400 ml-1">
                      ({new Date(connection.metastoreDirectTestResults.lastTested).toLocaleDateString()})
                    </span>
                  )}
                </span>
              ) : (
                <span className="text-yellow-600">⚠️ Never Tested</span>
              )}
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
      <div className="flex items-center justify-between border-t border-gray-200 px-6 py-4">
        <div className="flex items-center gap-2">
          <button
            onClick={onEdit}
            className="inline-flex items-center px-3 py-1.5 border border-gray-300 text-xs font-medium rounded text-gray-700 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
            title="Edit connection"
          >
            <PencilIcon className="h-3 w-3 mr-1" />
            Edit
          </button>

          <button
            onClick={onDuplicate}
            className="inline-flex items-center px-3 py-1.5 border border-gray-300 text-xs font-medium rounded text-gray-700 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
            title="Duplicate connection"
          >
            <DocumentDuplicateIcon className="h-3 w-3 mr-1" />
            Copy
          </button>
        </div>

        <div className="flex items-center gap-2">
          <button
            onClick={onTest}
            className="inline-flex items-center px-3 py-1.5 border border-gray-300 text-xs font-medium rounded text-gray-700 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-green-500"
            title="Test connection"
          >
            <BeakerIcon className="h-3 w-3 mr-1" />
            Test
          </button>

          <button
            onClick={onExport}
            className="inline-flex items-center px-3 py-1.5 border border-gray-300 text-xs font-medium rounded text-gray-700 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-purple-500"
            title="Export connection to JSON"
          >
            <ArrowDownTrayIcon className="h-3 w-3 mr-1" />
            Export
          </button>
        </div>
      </div>
    </div>
  );
};

export default ConnectionCard;