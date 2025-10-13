import React from 'react';
import { useNavigate } from 'react-router-dom';
import BaseCard from '../cards/BaseCard';
import { PlusIcon, DocumentArrowUpIcon, ListBulletIcon } from '@heroicons/react/24/outline';

const DatasetsPage: React.FC = () => {
  const navigate = useNavigate();

  return (
    <div className="container mx-auto px-4 py-8">
      <div className="mb-8">
        <h1 className="text-3xl font-bold text-gray-900 mb-2">Dataset Management</h1>
        <p className="text-lg text-gray-600">
          Create, manage, and import datasets for HMS Mirror processing. 
          Define collections of databases and tables for your migration workflows.
        </p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        {/* New Dataset Card */}
        <BaseCard
          title="New Dataset"
          description="Create a new dataset using the Dataset Wizard. Define databases, tables, and filters for processing."
          icon={<PlusIcon className="w-6 h-6" />}
          onClick={() => navigate('/datasets/new')}
          className="hover:border-blue-300"
        >
          <div className="mt-4 pt-4 border-t border-gray-100">
            <ul className="text-sm text-gray-600 space-y-2">
              <li className="flex items-center">
                <div className="w-1.5 h-1.5 bg-blue-500 rounded-full mr-2"></div>
                Step-by-step wizard interface
              </li>
              <li className="flex items-center">
                <div className="w-1.5 h-1.5 bg-blue-500 rounded-full mr-2"></div>
                Database and table configuration
              </li>
              <li className="flex items-center">
                <div className="w-1.5 h-1.5 bg-blue-500 rounded-full mr-2"></div>
                Advanced filtering options
              </li>
              <li className="flex items-center">
                <div className="w-1.5 h-1.5 bg-blue-500 rounded-full mr-2"></div>
                Warehouse path settings
              </li>
            </ul>
          </div>
        </BaseCard>

        {/* Import Dataset Card */}
        <BaseCard
          title="Import Dataset"
          description="Import an existing dataset from a YAML configuration file or another HMS Mirror instance."
          icon={<DocumentArrowUpIcon className="w-6 h-6" />}
          onClick={() => navigate('/datasets/import')}
          className="hover:border-green-300"
        >
          <div className="mt-4 pt-4 border-t border-gray-100">
            <ul className="text-sm text-gray-600 space-y-2">
              <li className="flex items-center">
                <div className="w-1.5 h-1.5 bg-green-500 rounded-full mr-2"></div>
                YAML file upload
              </li>
              <li className="flex items-center">
                <div className="w-1.5 h-1.5 bg-green-500 rounded-full mr-2"></div>
                Configuration validation
              </li>
              <li className="flex items-center">
                <div className="w-1.5 h-1.5 bg-green-500 rounded-full mr-2"></div>
                Import from RocksDB backup
              </li>
              <li className="flex items-center">
                <div className="w-1.5 h-1.5 bg-green-500 rounded-full mr-2"></div>
                Duplicate detection
              </li>
            </ul>
          </div>
        </BaseCard>

        {/* List Datasets Card */}
        <BaseCard
          title="List Datasets"
          description="View, edit, and manage all existing datasets. Export, duplicate, or delete datasets as needed."
          icon={<ListBulletIcon className="w-6 h-6" />}
          onClick={() => navigate('/datasets/list')}
          className="hover:border-purple-300"
        >
          <div className="mt-4 pt-4 border-t border-gray-100">
            <ul className="text-sm text-gray-600 space-y-2">
              <li className="flex items-center">
                <div className="w-1.5 h-1.5 bg-purple-500 rounded-full mr-2"></div>
                Search and filter datasets
              </li>
              <li className="flex items-center">
                <div className="w-1.5 h-1.5 bg-purple-500 rounded-full mr-2"></div>
                Edit existing configurations
              </li>
              <li className="flex items-center">
                <div className="w-1.5 h-1.5 bg-purple-500 rounded-full mr-2"></div>
                Export to YAML
              </li>
              <li className="flex items-center">
                <div className="w-1.5 h-1.5 bg-purple-500 rounded-full mr-2"></div>
                Delete and duplicate
              </li>
            </ul>
          </div>
        </BaseCard>
      </div>

      {/* Quick Stats Section */}
      <div className="mt-12 bg-gray-50 rounded-lg p-6">
        <h2 className="text-xl font-semibold text-gray-900 mb-4">Dataset Overview</h2>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <div className="text-center">
            <div className="text-2xl font-bold text-blue-600">-</div>
            <div className="text-sm text-gray-600">Total Datasets</div>
          </div>
          <div className="text-center">
            <div className="text-2xl font-bold text-green-600">-</div>
            <div className="text-sm text-gray-600">Active Datasets</div>
          </div>
          <div className="text-center">
            <div className="text-2xl font-bold text-purple-600">-</div>
            <div className="text-sm text-gray-600">Databases Configured</div>
          </div>
        </div>
        <div className="mt-4 text-xs text-gray-500 text-center">
          Statistics will be populated when datasets are loaded from RocksDB
        </div>
      </div>
    </div>
  );
};

export default DatasetsPage;