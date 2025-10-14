import React from 'react';
import { XMarkIcon } from '@heroicons/react/24/outline';

export interface DatasetListFilters {
  search: string;
  databaseCount: string;
  tableCount: string;
}

interface DatasetFiltersProps {
  filters: DatasetListFilters;
  onFiltersChange: (filters: DatasetListFilters) => void;
  onClose: () => void;
}

const DatasetFilters: React.FC<DatasetFiltersProps> = ({
  filters,
  onFiltersChange,
  onClose
}) => {
  const handleFilterChange = (key: keyof DatasetListFilters, value: string) => {
    onFiltersChange({
      ...filters,
      [key]: value
    });
  };

  const clearFilters = () => {
    onFiltersChange({
      search: filters.search,
      databaseCount: '',
      tableCount: ''
    });
  };

  const hasActiveFilters = filters.databaseCount !== '' || filters.tableCount !== '';

  return (
    <div className="bg-gray-50 border border-gray-200 rounded-lg p-4">
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-sm font-medium text-gray-900">Filter Datasets</h3>
        <button
          onClick={onClose}
          className="text-gray-400 hover:text-gray-600"
        >
          <XMarkIcon className="h-4 w-4" />
        </button>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
        {/* Database Count Filter */}
        <div>
          <label className="block text-xs font-medium text-gray-700 mb-1">
            Database Count
          </label>
          <select
            value={filters.databaseCount}
            onChange={(e) => handleFilterChange('databaseCount', e.target.value)}
            className="block w-full text-sm border-gray-300 rounded-md focus:ring-blue-500 focus:border-blue-500"
          >
            <option value="">All Counts</option>
            <option value="1-5">1-5 databases</option>
            <option value="6-10">6-10 databases</option>
            <option value="11-20">11-20 databases</option>
            <option value="21+">21+ databases</option>
          </select>
        </div>

        {/* Table Count Filter */}
        <div>
          <label className="block text-xs font-medium text-gray-700 mb-1">
            Table Count
          </label>
          <select
            value={filters.tableCount}
            onChange={(e) => handleFilterChange('tableCount', e.target.value)}
            className="block w-full text-sm border-gray-300 rounded-md focus:ring-blue-500 focus:border-blue-500"
          >
            <option value="">All Counts</option>
            <option value="1-10">1-10 tables</option>
            <option value="11-50">11-50 tables</option>
            <option value="51-100">51-100 tables</option>
            <option value="101+">101+ tables</option>
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
          {filters.databaseCount && (
            <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-blue-100 text-blue-800">
              Databases: {filters.databaseCount}
              <button
                onClick={() => handleFilterChange('databaseCount', '')}
                className="ml-1 text-blue-600 hover:text-blue-700"
              >
                <XMarkIcon className="h-3 w-3" />
              </button>
            </span>
          )}
          {filters.tableCount && (
            <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-blue-100 text-blue-800">
              Tables: {filters.tableCount}
              <button
                onClick={() => handleFilterChange('tableCount', '')}
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

export default DatasetFilters;
