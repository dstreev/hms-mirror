import React, { useState } from 'react';
import { XMarkIcon } from '@heroicons/react/24/outline';

export interface DataStrategy {
  key: string;
  name: string;
  description: string;
  visible: boolean;
}

interface DataStrategySelectionModalProps {
  isOpen: boolean;
  onClose: () => void;
  onSelect: (strategy: string) => void;
  title: string;
  selectedStrategy?: string;
}

const DATA_STRATEGIES: DataStrategy[] = [
  {
    key: 'SCHEMA_ONLY',
    name: 'Schema Only',
    description: 'Transfer schema only, location replaced with RIGHT cluster namespace. Data moved externally (e.g., distcp).',
    visible: true
  },
  {
    key: 'LINKED',
    name: 'Linked',
    description: 'Clusters are linked. Transfer schema leaving location unchanged. Test RIGHT cluster using LEFT storage.',
    visible: true
  },
  {
    key: 'SQL',
    name: 'SQL',
    description: 'Clusters are linked. Use SQL to migrate data between clusters.',
    visible: true
  },
  {
    key: 'EXPORT_IMPORT',
    name: 'Export/Import',
    description: 'Use EXPORT/IMPORT to migrate data. EXPORT from LEFT where RIGHT can IMPORT.',
    visible: true
  },
  {
    key: 'HYBRID',
    name: 'Hybrid',
    description: 'Automatically select SQL or EXPORT_IMPORT based on table criteria.',
    visible: true
  },
  {
    key: 'CONVERT_LINKED',
    name: 'Convert Linked',
    description: 'Convert existing LINKED schemas to SCHEMA_ONLY without recreating.',
    visible: true
  },
  {
    key: 'STORAGE_MIGRATION',
    name: 'Storage Migration',
    description: 'Migrate tables from current storage to default warehouse locations.',
    visible: true
  },
  {
    key: 'COMMON',
    name: 'Common',
    description: 'Shared storage between clusters. No data migration required.',
    visible: true
  },
  {
    key: 'ICEBERG_CONVERSION',
    name: 'Iceberg Conversion',
    description: 'Convert tables to Iceberg format during migration.',
    visible: true
  },
  {
    key: 'DUMP',
    name: 'Dump',
    description: 'Generate scripts based on LEFT cluster configuration without migration.',
    visible: true
  }
];

const DataStrategySelectionModal: React.FC<DataStrategySelectionModalProps> = ({
  isOpen,
  onClose,
  onSelect,
  title,
  selectedStrategy
}) => {
  const [selected, setSelected] = useState(selectedStrategy || '');

  if (!isOpen) return null;

  const handleSelect = () => {
    if (selected) {
      onSelect(selected);
      onClose();
    }
  };

  const visibleStrategies = DATA_STRATEGIES.filter(strategy => strategy.visible);

  return (
    <div className="fixed inset-0 z-50 overflow-y-auto">
      <div className="flex min-h-full items-end justify-center p-4 text-center sm:items-center sm:p-0">
        <div className="fixed inset-0 bg-gray-500 bg-opacity-75 transition-opacity" onClick={onClose}></div>
        
        <div className="relative transform overflow-hidden rounded-lg bg-white text-left shadow-xl transition-all sm:my-8 sm:w-full sm:max-w-2xl">
          <div className="bg-white px-4 pt-5 pb-4 sm:p-6 sm:pb-4">
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-lg font-medium leading-6 text-gray-900">
                {title}
              </h3>
              <button
                onClick={onClose}
                className="text-gray-400 hover:text-gray-600"
              >
                <XMarkIcon className="h-6 w-6" />
              </button>
            </div>
            
            <div className="mt-3">
              <p className="text-sm text-gray-500 mb-4">
                Select a data migration strategy. This choice cannot be changed after the configuration is created.
              </p>
              
              <div className="space-y-3 max-h-96 overflow-y-auto">
                {visibleStrategies.map((strategy) => (
                  <div
                    key={strategy.key}
                    className={`relative cursor-pointer rounded-lg border p-4 hover:bg-gray-50 ${
                      selected === strategy.key
                        ? 'border-blue-500 bg-blue-50'
                        : 'border-gray-300'
                    }`}
                    onClick={() => setSelected(strategy.key)}
                  >
                    <div className="flex items-start">
                      <div className="flex h-5 items-center">
                        <input
                          type="radio"
                          name="data-strategy"
                          value={strategy.key}
                          checked={selected === strategy.key}
                          onChange={() => setSelected(strategy.key)}
                          className="h-4 w-4 border-gray-300 text-blue-600 focus:ring-blue-500"
                        />
                      </div>
                      <div className="ml-3 text-sm">
                        <label className="font-medium text-gray-900 cursor-pointer">
                          {strategy.name}
                        </label>
                        <p className="text-gray-500 mt-1">
                          {strategy.description}
                        </p>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </div>
          
          <div className="bg-gray-50 px-4 py-3 sm:flex sm:flex-row-reverse sm:px-6">
            <button
              type="button"
              onClick={handleSelect}
              disabled={!selected}
              className="inline-flex w-full justify-center rounded-md border border-transparent bg-blue-600 px-4 py-2 text-base font-medium text-white shadow-sm hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 disabled:bg-gray-300 disabled:cursor-not-allowed sm:ml-3 sm:w-auto sm:text-sm"
            >
              Select Strategy
            </button>
            <button
              type="button"
              onClick={onClose}
              className="mt-3 inline-flex w-full justify-center rounded-md border border-gray-300 bg-white px-4 py-2 text-base font-medium text-gray-700 shadow-sm hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:ring-offset-2 sm:mt-0 sm:ml-3 sm:w-auto sm:text-sm"
            >
              Cancel
            </button>
          </div>
        </div>
      </div>
    </div>
  );
};

export default DataStrategySelectionModal;