import React from 'react';
import { XMarkIcon } from '@heroicons/react/24/outline';
import StrategySelector from './StrategySelector';
import { StrategySelectionResult } from '../../types/StrategySelection';

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

const DataStrategySelectionModal: React.FC<DataStrategySelectionModalProps> = ({
  isOpen,
  onClose,
  onSelect,
  title,
  selectedStrategy
}) => {
  if (!isOpen) return null;

  const handleStrategySelected = (result: StrategySelectionResult) => {
    onSelect(result.strategy);
    onClose();
  };

  return (
    <div className="fixed inset-0 z-50 overflow-y-auto">
      <div className="flex min-h-full items-end justify-center p-4 text-center sm:items-center sm:p-0">
        <div className="fixed inset-0 bg-gray-500 bg-opacity-75 transition-opacity" onClick={onClose}></div>
        
        <div className="relative transform overflow-hidden rounded-lg bg-white text-left shadow-xl transition-all sm:my-8 sm:w-full sm:max-w-4xl max-h-[90vh]">
          <div className="bg-white">
            <div className="flex items-center justify-between p-4 border-b border-gray-200">
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
            
            <div className="overflow-y-auto max-h-[calc(90vh-120px)]">
              <StrategySelector
                onStrategySelected={handleStrategySelected}
                onCancel={onClose}
              />
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default DataStrategySelectionModal;