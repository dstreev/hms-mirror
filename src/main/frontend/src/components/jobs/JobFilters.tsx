import React from 'react';
import { XMarkIcon } from '@heroicons/react/24/outline';
import { Job } from '../../types/Job';

export interface JobListFilters {
  search: string;
  strategy: string;
  dataset: string;
  config: string;
  leftConnection: string;
  rightConnection: string;
}

interface JobFiltersProps {
  filters: JobListFilters;
  onFiltersChange: (filters: JobListFilters) => void;
  onClose: () => void;
  jobs: Job[];
}

const JobFilters: React.FC<JobFiltersProps> = ({
  filters,
  onFiltersChange,
  onClose,
  jobs
}) => {
  // Extract unique values for each filter
  const uniqueStrategies = Array.from(new Set(jobs.map(j => j.strategy).filter(Boolean))).sort();
  const uniqueDatasets = Array.from(new Set(jobs.map(j => j.datasetName).filter(Boolean))).sort();
  const uniqueConfigs = Array.from(new Set(jobs.map(j => j.configurationName).filter(Boolean))).sort();
  const uniqueLeftConnections = Array.from(new Set(jobs.map(j => j.leftConnectionName).filter(Boolean))).sort();
  const uniqueRightConnections = Array.from(new Set(jobs.map(j => j.rightConnectionName).filter(Boolean))).sort();

  const handleFilterChange = (key: keyof JobListFilters, value: string) => {
    onFiltersChange({
      ...filters,
      [key]: value
    });
  };

  const clearFilters = () => {
    onFiltersChange({
      search: filters.search,
      strategy: '',
      dataset: '',
      config: '',
      leftConnection: '',
      rightConnection: ''
    });
  };

  const hasActiveFilters = filters.strategy !== '' || filters.dataset !== '' || filters.config !== '' ||
                           filters.leftConnection !== '' || filters.rightConnection !== '';

  return (
    <div className="bg-gray-50 border border-gray-200 rounded-lg p-4">
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-sm font-medium text-gray-900">Filter Jobs</h3>
        <button
          onClick={onClose}
          className="text-gray-400 hover:text-gray-600"
        >
          <XMarkIcon className="h-4 w-4" />
        </button>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
        {/* Data Strategy Filter */}
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
            {uniqueStrategies.map(strategy => (
              <option key={strategy} value={strategy}>{strategy}</option>
            ))}
          </select>
        </div>

        {/* Dataset Filter */}
        <div>
          <label className="block text-xs font-medium text-gray-700 mb-1">
            Dataset
          </label>
          <select
            value={filters.dataset}
            onChange={(e) => handleFilterChange('dataset', e.target.value)}
            className="block w-full text-sm border-gray-300 rounded-md focus:ring-blue-500 focus:border-blue-500"
          >
            <option value="">All Datasets</option>
            {uniqueDatasets.map(dataset => (
              <option key={dataset} value={dataset}>{dataset}</option>
            ))}
          </select>
        </div>

        {/* Config Filter */}
        <div>
          <label className="block text-xs font-medium text-gray-700 mb-1">
            Configuration
          </label>
          <select
            value={filters.config}
            onChange={(e) => handleFilterChange('config', e.target.value)}
            className="block w-full text-sm border-gray-300 rounded-md focus:ring-blue-500 focus:border-blue-500"
          >
            <option value="">All Configurations</option>
            {uniqueConfigs.map(config => (
              <option key={config} value={config}>{config}</option>
            ))}
          </select>
        </div>

        {/* Left Connection Filter */}
        <div>
          <label className="block text-xs font-medium text-gray-700 mb-1">
            Left Connection
          </label>
          <select
            value={filters.leftConnection}
            onChange={(e) => handleFilterChange('leftConnection', e.target.value)}
            className="block w-full text-sm border-gray-300 rounded-md focus:ring-blue-500 focus:border-blue-500"
          >
            <option value="">All Left Connections</option>
            {uniqueLeftConnections.map(conn => (
              <option key={conn} value={conn}>{conn}</option>
            ))}
          </select>
        </div>

        {/* Right Connection Filter */}
        <div>
          <label className="block text-xs font-medium text-gray-700 mb-1">
            Right Connection
          </label>
          <select
            value={filters.rightConnection}
            onChange={(e) => handleFilterChange('rightConnection', e.target.value)}
            className="block w-full text-sm border-gray-300 rounded-md focus:ring-blue-500 focus:border-blue-500"
          >
            <option value="">All Right Connections</option>
            {uniqueRightConnections.map(conn => (
              <option key={conn} value={conn}>{conn}</option>
            ))}
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
          {filters.dataset && (
            <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-blue-100 text-blue-800">
              Dataset: {filters.dataset}
              <button
                onClick={() => handleFilterChange('dataset', '')}
                className="ml-1 text-blue-600 hover:text-blue-700"
              >
                <XMarkIcon className="h-3 w-3" />
              </button>
            </span>
          )}
          {filters.config && (
            <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-blue-100 text-blue-800">
              Config: {filters.config}
              <button
                onClick={() => handleFilterChange('config', '')}
                className="ml-1 text-blue-600 hover:text-blue-700"
              >
                <XMarkIcon className="h-3 w-3" />
              </button>
            </span>
          )}
          {filters.leftConnection && (
            <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-blue-100 text-blue-800">
              Left: {filters.leftConnection}
              <button
                onClick={() => handleFilterChange('leftConnection', '')}
                className="ml-1 text-blue-600 hover:text-blue-700"
              >
                <XMarkIcon className="h-3 w-3" />
              </button>
            </span>
          )}
          {filters.rightConnection && (
            <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-blue-100 text-blue-800">
              Right: {filters.rightConnection}
              <button
                onClick={() => handleFilterChange('rightConnection', '')}
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

export default JobFilters;
