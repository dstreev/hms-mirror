import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { 
  ArrowLeftIcon, 
  PencilSquareIcon, 
  TrashIcon, 
  DocumentDuplicateIcon,
  MagnifyingGlassIcon,
  DocumentArrowDownIcon,
  ExclamationTriangleIcon
} from '@heroicons/react/24/outline';
import { datasetApi, DatasetListResponse } from '../../services/api/datasetApi';
import { DatasetFormData } from '../../types/Dataset';

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
  const [datasets, setDatasets] = useState<DatasetSummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [searchTerm, setSearchTerm] = useState('');
  const [selectedDatasets, setSelectedDatasets] = useState<string[]>([]);
  const [actionLoading, setActionLoading] = useState<string | null>(null);

  useEffect(() => {
    loadDatasets();
  }, []);

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
      const dataset = await datasetApi.getDataset(datasetName);
      if (dataset) {
        // Navigate to dataset wizard with the loaded data
        navigate('/datasets/edit', { state: { dataset, mode: 'edit' } });
      } else {
        setError(`Failed to load dataset: ${datasetName}`);
      }
    } catch (err: any) {
      console.error(`Failed to load dataset ${datasetName}:`, err);
      setError(err.message || `Failed to load dataset: ${datasetName}`);
    } finally {
      setActionLoading(null);
    }
  };

  const handleDeleteDataset = async (datasetName: string) => {
    if (!window.confirm(`Are you sure you want to delete the dataset "${datasetName}"? This action cannot be undone.`)) {
      return;
    }

    try {
      setActionLoading(datasetName);
      const result = await datasetApi.deleteDataset(datasetName);
      if (result.success) {
        await loadDatasets(); // Refresh the list
        setSelectedDatasets(prev => prev.filter(name => name !== datasetName));
      } else {
        setError(result.message || `Failed to delete dataset: ${datasetName}`);
      }
    } catch (err: any) {
      console.error(`Failed to delete dataset ${datasetName}:`, err);
      setError(err.message || `Failed to delete dataset: ${datasetName}`);
    } finally {
      setActionLoading(null);
    }
  };

  const handleCopyDataset = async (datasetName: string) => {
    const newName = prompt(`Enter a name for the copy of "${datasetName}":`, `${datasetName}_copy`);
    if (!newName || newName.trim() === '') return;

    try {
      setActionLoading(datasetName);
      const result = await datasetApi.copyDataset(datasetName, newName.trim());
      if (result.success) {
        await loadDatasets(); // Refresh the list
      } else {
        setError(result.message || `Failed to copy dataset: ${datasetName}`);
      }
    } catch (err: any) {
      console.error(`Failed to copy dataset ${datasetName}:`, err);
      setError(err.message || `Failed to copy dataset: ${datasetName}`);
    } finally {
      setActionLoading(null);
    }
  };

  const filteredDatasets = datasets.filter(dataset =>
    dataset.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
    dataset.description.toLowerCase().includes(searchTerm.toLowerCase())
  );

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
      <div className="container mx-auto px-4 py-8">
        <div className="flex items-center justify-center py-12">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
          <span className="ml-2 text-gray-600">Loading datasets...</span>
        </div>
      </div>
    );
  }

  return (
    <div className="container mx-auto px-4 py-8">
      {/* Header */}
      <div className="mb-8">
        <button
          onClick={() => navigate('/datasets')}
          className="flex items-center text-sm text-gray-500 hover:text-gray-700 mb-4"
        >
          <ArrowLeftIcon className="h-4 w-4 mr-1" />
          Back to Dataset Management
        </button>
        
        <h1 className="text-3xl font-bold text-gray-900 mb-2">Dataset List</h1>
        <p className="text-gray-600">
          Manage your existing datasets. Click on a dataset to edit it, or use the action buttons for other operations.
        </p>
      </div>

      {/* Search and Actions */}
      <div className="mb-6 flex flex-col sm:flex-row gap-4 items-start sm:items-center justify-between">
        <div className="relative flex-1 max-w-md">
          <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
            <MagnifyingGlassIcon className="h-5 w-5 text-gray-400" />
          </div>
          <input
            type="text"
            placeholder="Search datasets..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            className="block w-full pl-10 pr-3 py-2 border border-gray-300 rounded-md leading-5 bg-white placeholder-gray-500 focus:outline-none focus:placeholder-gray-400 focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
          />
        </div>
        
        <div className="flex items-center gap-2">
          <span className="text-sm text-gray-500">
            {filteredDatasets.length} of {datasets.length} datasets
          </span>
        </div>
      </div>

      {/* Error Message */}
      {error && (
        <div className="mb-6 bg-red-50 border border-red-200 rounded-md p-4">
          <div className="flex">
            <ExclamationTriangleIcon className="h-5 w-5 text-red-400 mr-2" />
            <div className="text-sm text-red-600">{error}</div>
            <button
              onClick={() => setError(null)}
              className="ml-auto text-red-400 hover:text-red-600"
            >
              ×
            </button>
          </div>
        </div>
      )}

      {/* Datasets List */}
      {filteredDatasets.length === 0 ? (
        <div className="text-center py-12 bg-gray-50 rounded-lg border-2 border-dashed border-gray-300">
          <svg className="mx-auto h-12 w-12 text-gray-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M20 13V6a2 2 0 00-2-2H6a2 2 0 00-2 2v7m16 0v5a2 2 0 01-2 2H6a2 2 0 01-2 2v-5m16 0h-2M4 13h2m13-8v.01M6 8v.01" />
          </svg>
          <h3 className="mt-2 text-sm font-medium text-gray-900">
            {searchTerm ? 'No datasets found' : 'No datasets available'}
          </h3>
          <p className="mt-1 text-sm text-gray-500">
            {searchTerm 
              ? `No datasets match your search "${searchTerm}". Try a different search term.`
              : 'Get started by creating your first dataset.'
            }
          </p>
          {!searchTerm && (
            <div className="mt-6">
              <button
                onClick={() => navigate('/datasets/new')}
                className="inline-flex items-center px-4 py-2 border border-transparent shadow-sm text-sm font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
              >
                Create Dataset
              </button>
            </div>
          )}
        </div>
      ) : (
        <div className="bg-white shadow overflow-hidden rounded-md">
          <ul className="divide-y divide-gray-200">
            {filteredDatasets.map((dataset) => (
              <li key={dataset.name} className="px-6 py-4 hover:bg-gray-50">
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
                      <span>{dataset.databaseCount} database{dataset.databaseCount !== 1 ? 's' : ''}</span>
                      <span>{dataset.totalTables} table{dataset.totalTables !== 1 ? 's' : ''}</span>
                      {dataset.modifiedDate && (
                        <span>Modified: {formatDate(dataset.modifiedDate)}</span>
                      )}
                    </div>
                  </div>
                  
                  <div className="flex items-center space-x-2">
                    <button
                      onClick={() => handleEditDataset(dataset.name)}
                      disabled={actionLoading === dataset.name}
                      className="p-2 text-blue-600 hover:text-blue-800 hover:bg-blue-50 rounded-md disabled:opacity-50 disabled:cursor-not-allowed"
                      title="Edit dataset"
                    >
                      <PencilSquareIcon className="h-5 w-5" />
                    </button>
                    
                    <button
                      onClick={() => handleCopyDataset(dataset.name)}
                      disabled={actionLoading === dataset.name}
                      className="p-2 text-green-600 hover:text-green-800 hover:bg-green-50 rounded-md disabled:opacity-50 disabled:cursor-not-allowed"
                      title="Copy dataset"
                    >
                      <DocumentDuplicateIcon className="h-5 w-5" />
                    </button>
                    
                    <button
                      onClick={() => handleDeleteDataset(dataset.name)}
                      disabled={actionLoading === dataset.name}
                      className="p-2 text-red-600 hover:text-red-800 hover:bg-red-50 rounded-md disabled:opacity-50 disabled:cursor-not-allowed"
                      title="Delete dataset"
                    >
                      <TrashIcon className="h-5 w-5" />
                    </button>
                  </div>
                </div>
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
};

export default ListDatasetsPage;