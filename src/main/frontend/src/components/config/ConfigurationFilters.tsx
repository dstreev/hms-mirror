import React from 'react';
import { XMarkIcon } from '@heroicons/react/24/outline';

export interface ConfigurationListFilters {
  search: string;
  strategy: string;
}

interface ConfigurationFiltersProps {
  filters: ConfigurationListFilters;
  onFiltersChange: (filters: ConfigurationListFilters) => void;
  onClose: () => void;
}

const ConfigurationFilters: React.FC<ConfigurationFiltersProps> = ({
  filters,
  onFiltersChange,
  onClose
}) => {
  const handleFilterChange = (key: keyof ConfigurationListFilters, value: string) => {
    onFiltersChange({
      ...filters,
      [key]: value
    });
  };

  const clearFilters = () => {
    onFiltersChange({
      search: filters.search,
      strategy: ''
    });
  };

  const hasActiveFilters = filters.strategy !== '';

  return (
    <div className="bg-gray-50 border border-gray-200 rounded-lg p-4">
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-sm font-medium text-gray-900">Filter Configurations</h3>
        <button
          onClick={onClose}
          className="text-gray-400 hover:text-gray-600"
        >
          <XMarkIcon className="h-4 w-4" />
        </button>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
        {/* Strategy Filter */}
        <div>
          <label className="block text-xs font-medium text-gray-700 mb-1">
            Data Strategy
          </label>
          <select
            value={filters.strategy}
            onChange={(e) => handleFilterChange('strategy', e.target.value)}
            className="block w-full text-sm border-gray-300 rounded-md focus:ring-blue-500 focus:border-blue-500"
          >
            <option value="">All Strategies</option>
            <option value="SQL">🔄 SQL</option>
            <option value="HYBRID">🔀 Hybrid</option>
            <option value="EXPORT_IMPORT">📊 Export/Import</option>
            <option value="SCHEMA_ONLY">📋 Schema Only</option>
            <option value="STORAGE_MIGRATION">📦 Storage Migration</option>
            <option value="LINKED">🔗 Linked</option>
            <option value="COMMON">🤝 Common</option>
            <option value="DUMP">💾 Dump</option>
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
          {filters.strategy && (
            <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-blue-100 text-blue-800">
              Strategy: {filters.strategy}
              <button
                onClick={() => handleFilterChange('strategy', '')}
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

export default ConfigurationFilters;
