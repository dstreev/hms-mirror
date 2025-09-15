import React, { useState } from 'react';
import { XMarkIcon } from '@heroicons/react/24/outline';

interface SaveAsDialogProps {
  isOpen: boolean;
  onClose: () => void;
  onSave: (filename: string) => void;
  currentFilename?: string;
}

const SaveAsDialog: React.FC<SaveAsDialogProps> = ({ isOpen, onClose, onSave, currentFilename }) => {
  const [filename, setFilename] = useState('');
  const [error, setError] = useState<string | null>(null);

  const handleSave = () => {
    const trimmedFilename = filename.trim();
    
    if (!trimmedFilename) {
      setError('Please enter a filename');
      return;
    }

    // Validate filename (no special characters that could cause issues)
    if (!/^[a-zA-Z0-9_\-]+$/.test(trimmedFilename)) {
      setError('Filename can only contain letters, numbers, underscores, and hyphens');
      return;
    }

    // Add .yaml extension if not present
    const finalFilename = trimmedFilename.endsWith('.yaml') || trimmedFilename.endsWith('.yml') 
      ? trimmedFilename 
      : `${trimmedFilename}.yaml`;

    onSave(finalFilename);
    handleClose();
  };

  const handleClose = () => {
    setFilename('');
    setError(null);
    onClose();
  };

  const handleKeyPress = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      handleSave();
    }
  };

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-50 overflow-y-auto">
      <div className="flex items-center justify-center min-h-screen px-4">
        {/* Backdrop */}
        <div className="fixed inset-0 bg-gray-500 bg-opacity-75 transition-opacity" onClick={handleClose}></div>

        {/* Modal */}
        <div className="relative bg-white rounded-lg shadow-xl max-w-md w-full">
          <div className="absolute top-0 right-0 pt-4 pr-4">
            <button
              onClick={handleClose}
              className="text-gray-400 hover:text-gray-500 focus:outline-none"
            >
              <XMarkIcon className="h-6 w-6" />
            </button>
          </div>

          <div className="p-6">
            <h3 className="text-lg font-medium text-gray-900 mb-4">Save Configuration As</h3>
            
            {currentFilename && (
              <div className="mb-4 text-sm text-gray-600">
                Current file: <span className="font-medium">{currentFilename}</span>
              </div>
            )}

            <div className="mb-4">
              <label htmlFor="filename" className="block text-sm font-medium text-gray-700 mb-2">
                New Filename
              </label>
              <input
                id="filename"
                type="text"
                value={filename}
                onChange={(e) => {
                  setFilename(e.target.value);
                  setError(null);
                }}
                onKeyPress={handleKeyPress}
                placeholder="my-config"
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                autoFocus
              />
              <p className="mt-1 text-xs text-gray-500">
                The .yaml extension will be added automatically if not provided
              </p>
              {error && (
                <p className="mt-2 text-sm text-red-600">{error}</p>
              )}
            </div>

            <div className="flex justify-end space-x-3">
              <button
                onClick={handleClose}
                className="px-4 py-2 border border-gray-300 rounded-md text-gray-700 hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-gray-500 focus:ring-offset-2"
              >
                Cancel
              </button>
              <button
                onClick={handleSave}
                className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2"
              >
                Save As
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default SaveAsDialog;