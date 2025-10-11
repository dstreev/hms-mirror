import React from 'react';
import { 
  BeakerIcon, 
  ArrowDownTrayIcon, 
  TrashIcon,
  XMarkIcon
} from '@heroicons/react/24/outline';

interface BulkActionsProps {
  selectedCount: number;
  onAction: (action: string) => void;
  onClear: () => void;
}

const BulkActions: React.FC<BulkActionsProps> = ({
  selectedCount,
  onAction,
  onClear
}) => {
  return (
    <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
      <div className="flex items-center justify-between">
        <div className="flex items-center space-x-4">
          <div className="flex items-center space-x-2">
            <span className="text-sm font-medium text-blue-900">
              {selectedCount} connection{selectedCount !== 1 ? 's' : ''} selected
            </span>
            <button
              onClick={onClear}
              className="text-blue-600 hover:text-blue-700"
              title="Clear selection"
            >
              <XMarkIcon className="h-4 w-4" />
            </button>
          </div>

          <div className="h-4 border-l border-blue-300"></div>

          <div className="flex space-x-2">
            <button
              onClick={() => onAction('test')}
              className="inline-flex items-center px-3 py-1.5 border border-blue-300 text-sm font-medium rounded text-blue-700 bg-white hover:bg-blue-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
            >
              <BeakerIcon className="h-4 w-4 mr-1.5" />
              Test Selected
            </button>

            <button
              onClick={() => onAction('export')}
              className="inline-flex items-center px-3 py-1.5 border border-blue-300 text-sm font-medium rounded text-blue-700 bg-white hover:bg-blue-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
            >
              <ArrowDownTrayIcon className="h-4 w-4 mr-1.5" />
              Export
            </button>

            <button
              onClick={() => onAction('delete')}
              className="inline-flex items-center px-3 py-1.5 border border-red-300 text-sm font-medium rounded text-red-700 bg-white hover:bg-red-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-red-500"
            >
              <TrashIcon className="h-4 w-4 mr-1.5" />
              Delete Selected
            </button>
          </div>
        </div>

        <div className="text-xs text-blue-600">
          Tip: Hold Ctrl/Cmd while clicking to select multiple connections
        </div>
      </div>
    </div>
  );
};

export default BulkActions;