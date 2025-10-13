import React from 'react';
import { DatasetWizardStepProps } from '../../../types/Dataset';

const ReviewAndSaveStep: React.FC<DatasetWizardStepProps> = ({ formData }) => {
  return (
    <div className="space-y-6">
      <div className="bg-gray-50 rounded-lg p-6">
        <h3 className="text-lg font-medium text-gray-900 mb-4">Dataset Summary</h3>
        
        {/* Basic Information */}
        <div className="mb-6">
          <h4 className="text-sm font-semibold text-gray-700 mb-2">Basic Information</h4>
          <div className="bg-white rounded-md p-4 border border-gray-200">
            <div className="grid grid-cols-1 gap-4">
              <div>
                <span className="text-sm font-medium text-gray-600">Name:</span>
                <span className="ml-2 text-sm text-gray-900">{formData.name}</span>
              </div>
              <div>
                <span className="text-sm font-medium text-gray-600">Description:</span>
                <p className="ml-2 text-sm text-gray-900 mt-1">{formData.description}</p>
              </div>
            </div>
          </div>
        </div>

        {/* Database Configuration */}
        <div>
          <h4 className="text-sm font-semibold text-gray-700 mb-2">
            Database Configuration ({formData.databases.length} database{formData.databases.length !== 1 ? 's' : ''})
          </h4>
          <div className="space-y-3">
            {formData.databases.map((database, index) => (
              <div key={index} className="bg-white rounded-md p-4 border border-gray-200">
                <div className="flex items-center justify-between mb-3">
                  <h5 className="text-sm font-medium text-gray-900">{database.databaseName}</h5>
                  <span className="text-xs text-gray-500 bg-gray-100 px-2 py-1 rounded">
                    Database {index + 1}
                  </span>
                </div>
                
                {/* Tables */}
                {database.tables.length > 0 && (
                  <div className="mb-3">
                    <span className="text-sm font-medium text-gray-600">Tables ({database.tables.length}):</span>
                    <div className="mt-1">
                      {database.tables.slice(0, 5).map((table, tableIndex) => (
                        <span
                          key={tableIndex}
                          className="inline-block bg-blue-100 text-blue-800 text-xs px-2 py-1 rounded mr-2 mb-1"
                        >
                          {table}
                        </span>
                      ))}
                      {database.tables.length > 5 && (
                        <span className="text-xs text-gray-500">
                          +{database.tables.length - 5} more
                        </span>
                      )}
                    </div>
                  </div>
                )}

                {/* Filter */}
                {database.filter && (
                  <div className="mb-3">
                    <span className="text-sm font-medium text-gray-600">Filter:</span>
                    <div className="mt-1 text-xs text-gray-600 space-y-1">
                      {database.filter.tblRegEx && (
                        <div>Include: <code className="bg-gray-100 px-1 rounded">{database.filter.tblRegEx}</code></div>
                      )}
                      {database.filter.tblExcludeRegEx && (
                        <div>Exclude: <code className="bg-gray-100 px-1 rounded">{database.filter.tblExcludeRegEx}</code></div>
                      )}
                      {database.filter.tblSizeLimit && database.filter.tblSizeLimit > 0 && (
                        <div>Size limit: {database.filter.tblSizeLimit} bytes</div>
                      )}
                      {database.filter.tblPartitionLimit && database.filter.tblPartitionLimit > 0 && (
                        <div>Partition limit: {database.filter.tblPartitionLimit}</div>
                      )}
                    </div>
                  </div>
                )}

                {/* Warehouse */}
                {(database.warehouse.managedDirectory || database.warehouse.externalDirectory) && (
                  <div>
                    <span className="text-sm font-medium text-gray-600">Warehouse:</span>
                    <div className="mt-1 text-xs text-gray-600 space-y-1">
                      {database.warehouse.managedDirectory && (
                        <div>Managed: <code className="bg-gray-100 px-1 rounded">{database.warehouse.managedDirectory}</code></div>
                      )}
                      {database.warehouse.externalDirectory && (
                        <div>External: <code className="bg-gray-100 px-1 rounded">{database.warehouse.externalDirectory}</code></div>
                      )}
                    </div>
                  </div>
                )}

                {/* No configuration indicator */}
                {database.tables.length === 0 && !database.filter && !database.warehouse.managedDirectory && !database.warehouse.externalDirectory && (
                  <div className="text-xs text-gray-500 italic">
                    No tables or filters configured
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* YAML Preview */}
      <div className="bg-gray-50 rounded-lg p-6">
        <h3 className="text-lg font-medium text-gray-900 mb-4">YAML Preview</h3>
        <p className="text-sm text-gray-600 mb-3">
          This is how your dataset will be stored in RocksDB:
        </p>
        <div className="bg-gray-900 rounded-md p-4 overflow-x-auto">
          <pre className="text-sm text-green-400 font-mono">
            <code>
{`name: ${formData.name}
description: ${formData.description}
databases:`}
{formData.databases.map((database, index) => (
`
  - databaseName: ${database.databaseName}
    tables:${database.tables.length > 0 ? database.tables.map(table => `
      - ${table}`).join('') : ' []'}${database.filter ? `
    filter:${database.filter.tblRegEx ? `
      tblRegEx: "${database.filter.tblRegEx}"` : ''}${database.filter.tblExcludeRegEx ? `
      tblExcludeRegEx: "${database.filter.tblExcludeRegEx}"` : ''}${database.filter.tblSizeLimit ? `
      tblSizeLimit: ${database.filter.tblSizeLimit}` : ''}${database.filter.tblPartitionLimit ? `
      tblPartitionLimit: ${database.filter.tblPartitionLimit}` : ''}` : ''}
    warehouse:
      warehouseSource: PLAN${database.warehouse.managedDirectory ? `
      managedDirectory: "${database.warehouse.managedDirectory}"` : ''}${database.warehouse.externalDirectory ? `
      externalDirectory: "${database.warehouse.externalDirectory}"` : ''}`
)).join('')}
            </code>
          </pre>
        </div>
      </div>

      {/* Save Instructions */}
      <div className="bg-blue-50 border border-blue-200 rounded-md p-4">
        <div className="flex">
          <div className="flex-shrink-0">
            <svg className="h-5 w-5 text-blue-400" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor">
              <path fillRule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clipRule="evenodd" />
            </svg>
          </div>
          <div className="ml-3">
            <h3 className="text-sm font-medium text-blue-800">
              Ready to Save
            </h3>
            <div className="mt-2 text-sm text-blue-700">
              <p>
                When you click "Save Dataset", this configuration will be saved to RocksDB 
                in the datasets column family using "<strong>{formData.name}</strong>" as the key.
              </p>
              <p className="mt-2">
                You can then use this dataset in your HMS Mirror processing workflows.
              </p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default ReviewAndSaveStep;