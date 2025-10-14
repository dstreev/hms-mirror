import React, { useState, useEffect } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import {
  PlusIcon,
  ArrowPathIcon,
  PencilSquareIcon,
  TrashIcon,
  DocumentDuplicateIcon,
  MagnifyingGlassIcon,
  DocumentArrowUpIcon,
  ExclamationTriangleIcon,
  FunnelIcon
} from '@heroicons/react/24/outline';
import { datasetApi, DatasetListResponse } from '../../services/api/datasetApi';
import { DatasetFormData } from '../../types/Dataset';
import DatasetFilters, { DatasetListFilters } from './DatasetFilters';
import ConfirmationDialog from '../common/ConfirmationDialog';

interface DatasetSummary {
  name: string;
  description: string;
  databaseCount: number;
  totalTables: number;
  createdDate?: string;
  modifiedDate?: string;
}

const ListDatasetsPage: React.FC = () => {
  const navigate = useNavigate();
  const location = useLocation();
  const [datasets, setDatasets] = useState<DatasetSummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [filters, setFilters] = useState<DatasetListFilters>({
    search: '',
    databaseCount: '',
    tableCount: ''
  });
  const [showFilters, setShowFilters] = useState(false);
  const [selectedDatasets, setSelectedDatasets] = useState<string[]>([]);
  const [actionLoading, setActionLoading] = useState<string | null>(null);
  const [deleteDialog, setDeleteDialog] = useState<{
    isOpen: boolean;
    dataset: DatasetSummary | null;
  }>({ isOpen: false, dataset: null });

  useEffect(() => {
    loadDatasets();
  }, [location.key]);

  const loadDatasets = async () => {
    try {
      setLoading(true);
      setError(null);
      const response = await datasetApi.getDatasets();
      
      if (response && response.status === 'SUCCESS' && response.data) {
        const datasetSummaries: DatasetSummary[] = Object.entries(response.data).map(([name, dataset]) => ({
          name,
          description: dataset.description || '',
          databaseCount: dataset.databases?.length || 0,
          totalTables: dataset.databases?.reduce((total, db) => total + (db.tables?.length || 0), 0) || 0,
          createdDate: dataset.createdDate,
          modifiedDate: dataset.modifiedDate
        }));
        setDatasets(datasetSummaries);
      } else {
        setDatasets([]);
      }
    } catch (err: any) {
      console.error('Failed to load datasets:', err);
      setError(err.message || 'Failed to load datasets');
      setDatasets([]);
    } finally {
      setLoading(false);
    }
  };

  const handleEditDataset = async (datasetName: string) => {
    try {
      setActionLoading(datasetName);
      setError(null);

      const dataset = await datasetApi.getDataset(datasetName);
      if (dataset) {
        // Navigate to dataset wizard with the loaded data
        navigate('/datasets/edit', { state: { dataset, mode: 'edit' } });
      } else {
        setError(`Failed to load dataset: ${datasetName}. The dataset may not exist or the backend service may be unavailable.`);
      }
    } catch (err: any) {
      console.error(`Failed to load dataset ${datasetName}:`, err);
      setError(err.message || `Failed to load dataset: ${datasetName}. Check browser console for details.`);
    } finally {
      setActionLoading(null);
    }
  };

  const handleDeleteClick = (dataset: DatasetSummary) => {
    setDeleteDialog({ isOpen: true, dataset });
  };

  const handleDeleteConfirm = async () => {
    if (!deleteDialog.dataset) return;

    try {
      setError(null);
      setActionLoading(deleteDialog.dataset.name);

      const result = await datasetApi.deleteDataset(deleteDialog.dataset.name);
      if (result.success) {
        // Close dialog and refresh list
        setDeleteDialog({ isOpen: false, dataset: null });
        await loadDatasets();
        setSelectedDatasets(prev => prev.filter(name => name !== deleteDialog.dataset!.name));
      } else {
        setError(result.message || `Failed to delete dataset: ${deleteDialog.dataset.name}`);
        setDeleteDialog({ isOpen: false, dataset: null });
      }
    } catch (err: any) {
      console.error(`Failed to delete dataset ${deleteDialog.dataset.name}:`, err);
      setError(err.message || `Failed to delete dataset: ${deleteDialog.dataset.name}`);
      setDeleteDialog({ isOpen: false, dataset: null });
    } finally {
      setActionLoading(null);
    }
  };

  const handleDeleteCancel = () => {
    setDeleteDialog({ isOpen: false, dataset: null });
  };

  const handleCopyDataset = async (datasetName: string) => {
    try {
      setActionLoading(datasetName);
      // Load the full dataset data from the backend
      const dataset = await datasetApi.getDataset(datasetName);

      if (dataset) {
        // Navigate to Dataset Wizard with copy mode - clear the name for user to provide new name
        navigate('/datasets/edit', {
          state: {
            dataset: {
              ...dataset,
              name: '' // Clear name for copy mode
            },
            mode: 'copy'
          }
        });
      } else {
        setError(`Failed to load dataset: ${datasetName}`);
      }
    } catch (err: any) {
      console.error(`Failed to load dataset ${datasetName} for copying:`, err);
      setError(err.message || `Failed to load dataset for copying: ${datasetName}`);
    } finally {
      setActionLoading(null);
    }
  };

  const handleRefresh = () => {
    loadDatasets();
  };

  const handleCreateNew = () => {
    navigate('/datasets/new');
  };

  const handleImport = () => {
    navigate('/datasets/import');
  };

  const matchesDatabaseCountFilter = (count: number, filter: string): boolean => {
    if (!filter) return true;

    switch (filter) {
      case '1-5': return count >= 1 && count <= 5;
      case '6-10': return count >= 6 && count <= 10;
      case '11-20': return count >= 11 && count <= 20;
      case '21+': return count >= 21;
      default: return true;
    }
  };

  const matchesTableCountFilter = (count: number, filter: string): boolean => {
    if (!filter) return true;

    switch (filter) {
      case '1-10': return count >= 1 && count <= 10;
      case '11-50': return count >= 11 && count <= 50;
      case '51-100': return count >= 51 && count <= 100;
      case '101+': return count >= 101;
      default: return true;
    }
  };

  const filteredDatasets = datasets.filter(dataset => {
    if (filters.search &&
        !dataset.name.toLowerCase().includes(filters.search.toLowerCase()) &&
        !dataset.description.toLowerCase().includes(filters.search.toLowerCase())) {
      return false;
    }

    if (!matchesDatabaseCountFilter(dataset.databaseCount, filters.databaseCount)) {
      return false;
    }

    if (!matchesTableCountFilter(dataset.totalTables, filters.tableCount)) {
      return false;
    }

    return true;
  });

  const formatDate = (dateStr?: string) => {
    if (!dateStr) return 'N/A';
    try {
      return new Date(dateStr).toLocaleDateString();
    } catch {
      return dateStr;
    }
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600"></div>
        <span className="ml-3 text-gray-600">Loading datasets...</span>
      </div>
    );
  }

  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      {/* Page Header */}
      <div className="mb-8">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold text-gray-900 flex items-center">
              📊 Dataset Management
            </h1>
            <p className="text-gray-600 mt-2">Manage dataset collections for HMS Mirror processing</p>
          </div>

          <div className="flex space-x-3">
            <button
              onClick={() => setShowFilters(!showFilters)}
              className="inline-flex items-center px-4 py-2 border border-gray-300 rounded-md shadow-sm text-sm font-medium text-gray-700 bg-white hover:bg-gray-50"
            >
              <FunnelIcon className="h-4 w-4 mr-2" />
              Filters
            </button>
            <button
              onClick={handleRefresh}
              className="inline-flex items-center px-4 py-2 border border-gray-300 rounded-md shadow-sm text-sm font-medium text-gray-700 bg-white hover:bg-gray-50"
            >
              <ArrowPathIcon className="h-4 w-4 mr-2" />
              Refresh
            </button>
            <button
              onClick={handleImport}
              className="inline-flex items-center px-4 py-2 border border-gray-300 rounded-md shadow-sm text-sm font-medium text-gray-700 bg-white hover:bg-gray-50"
            >
              <DocumentArrowUpIcon className="h-4 w-4 mr-2" />
              Import
            </button>
            <button
              onClick={handleCreateNew}
              className="inline-flex items-center px-4 py-2 border border-transparent rounded-md shadow-sm text-sm font-medium text-white bg-blue-600 hover:bg-blue-700"
            >
              <PlusIcon className="h-4 w-4 mr-2" />
              New Dataset
            </button>
          </div>
        </div>

        {/* Search Bar */}
        <div className="mt-6 max-w-md">
          <div className="relative">
            <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
              <MagnifyingGlassIcon className="h-5 w-5 text-gray-400" />
            </div>
            <input
              type="text"
              placeholder="Search datasets..."
              value={filters.search}
              onChange={(e) => setFilters({ ...filters, search: e.target.value })}
              className="block w-full pl-10 pr-3 py-2 border border-gray-300 rounded-md leading-5 bg-white placeholder-gray-500 focus:outline-none focus:placeholder-gray-400 focus:ring-1 focus:ring-blue-500 focus:border-blue-500"
            />
          </div>
        </div>
      </div>

      {/* Filters */}
      {showFilters && (
        <div className="mb-6">
          <DatasetFilters
            filters={filters}
            onFiltersChange={setFilters}
            onClose={() => setShowFilters(false)}
          />
        </div>
      )}

      {/* Error State */}
      {error && (
        <div className="mb-6 bg-red-50 border border-red-200 rounded-md p-4">
          <div className="flex">
            <div className="ml-3">
              <h3 className="text-sm font-medium text-red-800">Error loading datasets</h3>
              <div className="mt-2 text-sm text-red-700">{error}</div>
              <div className="mt-4">
                <button
                  onClick={handleRefresh}
                  className="text-sm bg-red-100 text-red-800 rounded-md px-2 py-1 hover:bg-red-200"
                >
                  Try again
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Empty State */}
      {filteredDatasets.length === 0 && !loading && (
        <div className="text-center py-12">
          <div className="text-6xl mb-4">📊</div>
          <h3 className="text-lg font-medium text-gray-900 mb-2">
            {datasets.length === 0 ? 'No datasets found' : 'No datasets match your filters'}
          </h3>
          <p className="text-gray-600 mb-6">
            {datasets.length === 0
              ? 'Create your first dataset to get started with HMS-Mirror'
              : 'Try adjusting your search or filter criteria'
            }
          </p>
          {datasets.length === 0 && (
            <button
              onClick={handleCreateNew}
              className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700"
            >
              <PlusIcon className="h-4 w-4 mr-2" />
              Create Dataset
            </button>
          )}
          <div className="mt-4 text-sm text-gray-500">
            💡 Tip: Datasets define collections of databases and tables for migration workflows
          </div>
        </div>
      )}

      {/* Dataset List */}
      {filteredDatasets.length > 0 && (
        <>
          <div className="mb-4 flex items-center justify-between">
            <div className="flex items-center">
              <span className="text-sm text-gray-700">
                📊 Showing {filteredDatasets.length} dataset{filteredDatasets.length !== 1 ? 's' : ''}
                {datasets.length !== filteredDatasets.length && ` of ${datasets.length} total`}
              </span>
            </div>
          </div>
          <div className="space-y-4">
            {filteredDatasets.map((dataset) => (
              <div
                key={dataset.name}
                className="bg-white border border-gray-200 rounded-lg p-6 hover:shadow-md transition-shadow"
              >
                <div className="flex items-center justify-between">
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center">
                      <h3 className="text-lg font-medium text-gray-900 truncate">
                        {dataset.name}
                      </h3>
                      {actionLoading === dataset.name && (
                        <div className="ml-2 animate-spin rounded-full h-4 w-4 border-b-2 border-blue-600"></div>
                      )}
                    </div>
                    <p className="text-sm text-gray-500 mt-1">{dataset.description || 'No description'}</p>
                    <div className="flex items-center mt-2 text-xs text-gray-400 space-x-4">
                      <span>📁 {dataset.databaseCount} database{dataset.databaseCount !== 1 ? 's' : ''}</span>
                      <span>📄 {dataset.totalTables} table{dataset.totalTables !== 1 ? 's' : ''}</span>
                      {dataset.modifiedDate && (
                        <span>🕒 Modified: {formatDate(dataset.modifiedDate)}</span>
                      )}
                    </div>
                  </div>

                  <div className="flex items-center space-x-2 ml-4">
                    <button
                      onClick={() => handleEditDataset(dataset.name)}
                      disabled={actionLoading === dataset.name}
                      className="inline-flex items-center px-3 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 bg-white hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
                      title="Edit dataset"
                    >
                      <PencilSquareIcon className="h-4 w-4 mr-1" />
                      Edit
                    </button>

                    <button
                      onClick={() => handleCopyDataset(dataset.name)}
                      disabled={actionLoading === dataset.name}
                      className="inline-flex items-center px-3 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 bg-white hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
                      title="Copy dataset"
                    >
                      <DocumentDuplicateIcon className="h-4 w-4 mr-1" />
                      Copy
                    </button>

                    <button
                      onClick={() => handleDeleteClick(dataset)}
                      disabled={actionLoading === dataset.name}
                      className="inline-flex items-center px-3 py-2 border border-red-300 rounded-md text-sm font-medium text-red-700 bg-white hover:bg-red-50 disabled:opacity-50 disabled:cursor-not-allowed"
                      title="Delete dataset"
                    >
                      <TrashIcon className="h-4 w-4 mr-1" />
                      Delete
                    </button>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </>
      )}

      {/* Delete Confirmation Dialog */}
      <ConfirmationDialog
        isOpen={deleteDialog.isOpen}
        title="Delete Dataset"
        message={`Are you sure you want to delete the dataset "${deleteDialog.dataset?.name}"? This action cannot be undone.`}
        confirmText="Delete"
        cancelText="Cancel"
        variant="danger"
        onConfirm={handleDeleteConfirm}
        onCancel={handleDeleteCancel}
      />
    </div>
  );
};

export default ListDatasetsPage;