import React from 'react';
import { XMarkIcon } from '@heroicons/react/24/outline';
import { ConnectionListFilters, ENVIRONMENT_OPTIONS } from '../../types/Connection';

interface ConnectionFiltersProps {
  filters: ConnectionListFilters;
  onFiltersChange: (filters: ConnectionListFilters) => void;
  onClose: () => void;
}

const ConnectionFilters: React.FC<ConnectionFiltersProps> = ({
  filters,
  onFiltersChange,
  onClose
}) => {
  const handleFilterChange = (key: keyof ConnectionListFilters, value: string) => {
    onFiltersChange({
      ...filters,
      [key]: value
    });
  };

  const clearFilters = () => {
    onFiltersChange({
      search: '',
      environment: '',
      status: 'all'
    });
  };

  const hasActiveFilters = filters.environment !== '' || filters.status !== 'all';

  return (
    <div className="bg-gray-50 border border-gray-200 rounded-lg p-4">
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-sm font-medium text-gray-900">Filter Connections</h3>
        <button
          onClick={onClose}
          className="text-gray-400 hover:text-gray-600"
        >
          <XMarkIcon className="h-4 w-4" />
        </button>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
        {/* Environment Filter */}
        <div>
          <label className="block text-xs font-medium text-gray-700 mb-1">
            Environment
          </label>
          <select
            value={filters.environment}
            onChange={(e) => handleFilterChange('environment', e.target.value)}
            className="block w-full text-sm border-gray-300 rounded-md focus:ring-blue-500 focus:border-blue-500"
          >
            <option value="">All Environments</option>
            {ENVIRONMENT_OPTIONS.map((option) => (
              <option key={option.value} value={option.value}>
                {option.label}
              </option>
            ))}
          </select>
        </div>

        {/* Status Filter */}
        <div>
          <label className="block text-xs font-medium text-gray-700 mb-1">
            Test Status
          </label>
          <select
            value={filters.status}
            onChange={(e) => handleFilterChange('status', e.target.value)}
            className="block w-full text-sm border-gray-300 rounded-md focus:ring-blue-500 focus:border-blue-500"
          >
            <option value="all">All Statuses</option>
            <option value="success">✅ Tested Successfully</option>
            <option value="failed">❌ Test Failed</option>
            <option value="never_tested">⚠️ Never Tested</option>
          </select>
        </div>

        {/* Clear Filters */}
        <div className="flex items-end">
          {hasActiveFilters && (
            <button
              onClick={clearFilters}
              className="text-sm text-blue-600 hover:text-blue-700 underline"
            >
              Clear filters
            </button>
          )}
        </div>
      </div>

      {/* Active Filters Display */}
      {hasActiveFilters && (
        <div className="mt-3 flex flex-wrap gap-2">
          <span className="text-xs text-gray-500">Active filters:</span>
          {filters.environment && (
            <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-blue-100 text-blue-800">
              Environment: {ENVIRONMENT_OPTIONS.find(opt => opt.value === filters.environment)?.label}
              <button
                onClick={() => handleFilterChange('environment', '')}
                className="ml-1 text-blue-600 hover:text-blue-700"
              >
                <XMarkIcon className="h-3 w-3" />
              </button>
            </span>
          )}
          {filters.status !== 'all' && (
            <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-blue-100 text-blue-800">
              Status: {filters.status === 'success' ? 'Success' : filters.status === 'failed' ? 'Failed' : 'Never Tested'}
              <button
                onClick={() => handleFilterChange('status', 'all')}
                className="ml-1 text-blue-600 hover:text-blue-700"
              >
                <XMarkIcon className="h-3 w-3" />
              </button>
            </span>
          )}
        </div>
      )}
    </div>
  );
};

export default ConnectionFilters;