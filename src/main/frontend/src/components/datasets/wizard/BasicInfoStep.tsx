import React from 'react';
import { DatasetWizardStepProps } from '../../../types/Dataset';

const BasicInfoStep: React.FC<DatasetWizardStepProps> = ({ formData, errors, updateFormData }) => {
  return (
    <div className="space-y-6">
      <div className="grid grid-cols-1 gap-6">
        <div>
          <label htmlFor="name" className="block text-sm font-medium text-gray-700 mb-2">
            Dataset Name *
          </label>
          <input
            type="text"
            id="name"
            name="name"
            value={formData.name}
            onChange={(e) => updateFormData({ name: e.target.value })}
            className={`block w-full px-3 py-2 border rounded-md shadow-sm placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 ${
              errors.name ? 'border-red-300 text-red-900 placeholder-red-300' : 'border-gray-300'
            }`}
            placeholder="Enter a unique name for your dataset"
          />
          {errors.name && (
            <p className="mt-2 text-sm text-red-600">{errors.name}</p>
          )}
          <p className="mt-2 text-sm text-gray-500">
            The dataset name will be used as the key when saving to RocksDB. Choose a descriptive, unique name.
          </p>
        </div>

        <div>
          <label htmlFor="description" className="block text-sm font-medium text-gray-700 mb-2">
            Description *
          </label>
          <textarea
            id="description"
            name="description"
            rows={4}
            value={formData.description}
            onChange={(e) => updateFormData({ description: e.target.value })}
            className={`block w-full px-3 py-2 border rounded-md shadow-sm placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 ${
              errors.description ? 'border-red-300 text-red-900 placeholder-red-300' : 'border-gray-300'
            }`}
            placeholder="Describe what this dataset contains and its purpose"
          />
          {errors.description && (
            <p className="mt-2 text-sm text-red-600">{errors.description}</p>
          )}
          <p className="mt-2 text-sm text-gray-500">
            Provide a clear description of what this dataset contains and how it will be used in your HMS Mirror processing.
          </p>
        </div>
      </div>

      {/* Info Box */}
      <div className="bg-blue-50 border border-blue-200 rounded-md p-4">
        <div className="flex">
          <div className="flex-shrink-0">
            <svg className="h-5 w-5 text-blue-400" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor">
              <path fillRule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clipRule="evenodd" />
            </svg>
          </div>
          <div className="ml-3">
            <h3 className="text-sm font-medium text-blue-800">
              About Datasets
            </h3>
            <div className="mt-2 text-sm text-blue-700">
              <p>
                A dataset defines a collection of databases and tables for HMS Mirror processing. 
                Each dataset can contain multiple databases, and each database can specify either:
              </p>
              <ul className="list-disc list-inside mt-2 space-y-1">
                <li>A specific list of tables to include</li>
                <li>Filter criteria to automatically select tables based on patterns, size, or partition limits</li>
              </ul>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default BasicInfoStep;