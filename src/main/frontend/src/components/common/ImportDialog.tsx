import React, { useState, useRef } from 'react';
import { XMarkIcon, ArrowUpTrayIcon, ExclamationTriangleIcon } from '@heroicons/react/24/outline';
import { importFromJson } from '../../utils/importExport';

interface ImportDialogProps<T> {
  isOpen: boolean;
  onClose: () => void;
  onImport: (data: T, newName?: string) => Promise<{ success: boolean; message?: string; status?: number }>;
  title: string;
  itemType: string; // "Connection", "Dataset", "Configuration"
  getNameFromData: (data: T) => string;
  setNameInData: (data: T, newName: string) => T;
}

export default function ImportDialog<T>({
  isOpen,
  onClose,
  onImport,
  title,
  itemType,
  getNameFromData,
  setNameInData
}: ImportDialogProps<T>) {
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [parsedData, setParsedData] = useState<T | null>(null);
  const [importing, setImporting] = useState(false);
  const [error, setError] = useState<string>('');
  const [nameConflict, setNameConflict] = useState(false);
  const [newName, setNewName] = useState<string>('');
  const fileInputRef = useRef<HTMLInputElement>(null);

  const handleFileSelect = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) return;

    setError('');
    setSelectedFile(file);
    setNameConflict(false);

    try {
      const data = await importFromJson<T>(file);
      setParsedData(data);
      setNewName(getNameFromData(data));
    } catch (err: any) {
      setError(err.message || 'Failed to parse JSON file');
      setParsedData(null);
    }
  };

  const handleImport = async () => {
    if (!parsedData) return;

    setImporting(true);
    setError('');

    try {
      // If there's a name conflict being resolved, use the new name
      const dataToImport = nameConflict
        ? setNameInData(parsedData, newName)
        : parsedData;

      const result = await onImport(dataToImport, nameConflict ? newName : undefined);

      if (result.success) {
        // Success - close dialog
        handleClose();
      } else if (result.status === 409) {
        // Name conflict - show rename option
        setNameConflict(true);
        setError(result.message || `A ${itemType.toLowerCase()} with this name already exists.`);
      } else {
        // Other error
        setError(result.message || `Failed to import ${itemType.toLowerCase()}`);
      }
    } catch (err: any) {
      setError(err.message || `Error importing ${itemType.toLowerCase()}`);
    } finally {
      setImporting(false);
    }
  };

  const handleClose = () => {
    setSelectedFile(null);
    setParsedData(null);
    setError('');
    setNameConflict(false);
    setNewName('');
    if (fileInputRef.current) {
      fileInputRef.current.value = '';
    }
    onClose();
  };

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
      <div className="bg-white rounded-lg shadow-xl max-w-lg w-full mx-4">
        {/* Header */}
        <div className="flex items-center justify-between p-6 border-b border-gray-200">
          <h2 className="text-xl font-semibold text-gray-900">{title}</h2>
          <button
            onClick={handleClose}
            className="text-gray-400 hover:text-gray-600"
          >
            <XMarkIcon className="w-6 h-6" />
          </button>
        </div>

        {/* Body */}
        <div className="p-6 space-y-4">
          {/* File Input */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Select JSON File
            </label>
            <div className="flex items-center space-x-2">
              <input
                ref={fileInputRef}
                type="file"
                accept=".json"
                onChange={handleFileSelect}
                className="block w-full text-sm text-gray-900 border border-gray-300 rounded-lg cursor-pointer bg-gray-50 focus:outline-none"
              />
            </div>
            {selectedFile && (
              <p className="mt-2 text-sm text-gray-600">
                Selected: {selectedFile.name}
              </p>
            )}
          </div>

          {/* Preview - Show name */}
          {parsedData && !nameConflict && (
            <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
              <h3 className="text-sm font-medium text-blue-900 mb-1">Preview</h3>
              <p className="text-sm text-blue-700">
                <span className="font-medium">Name:</span> {getNameFromData(parsedData)}
              </p>
            </div>
          )}

          {/* Name Conflict - Rename Input */}
          {nameConflict && (
            <div className="bg-amber-50 border border-amber-200 rounded-lg p-4">
              <div className="flex items-start space-x-2 mb-3">
                <ExclamationTriangleIcon className="w-5 h-5 text-amber-600 flex-shrink-0 mt-0.5" />
                <div>
                  <h3 className="text-sm font-medium text-amber-900">Name Conflict</h3>
                  <p className="text-sm text-amber-700 mt-1">
                    A {itemType.toLowerCase()} with this name already exists. Please enter a different name.
                  </p>
                </div>
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  New Name
                </label>
                <input
                  type="text"
                  value={newName}
                  onChange={(e) => setNewName(e.target.value)}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-blue-500 focus:border-blue-500"
                  placeholder={`Enter a unique name for this ${itemType.toLowerCase()}`}
                />
              </div>
            </div>
          )}

          {/* Error Message */}
          {error && !nameConflict && (
            <div className="bg-red-50 border border-red-200 rounded-lg p-4">
              <p className="text-sm text-red-700">{error}</p>
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="flex justify-end space-x-3 p-6 border-t border-gray-200">
          <button
            onClick={handleClose}
            className="px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-md hover:bg-gray-50"
          >
            Cancel
          </button>
          <button
            onClick={handleImport}
            disabled={!parsedData || importing || (nameConflict && !newName.trim())}
            className="px-4 py-2 text-sm font-medium text-white bg-blue-600 rounded-md hover:bg-blue-700 disabled:bg-gray-300 disabled:cursor-not-allowed flex items-center space-x-2"
          >
            {importing ? (
              <>
                <svg className="animate-spin h-4 w-4 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                </svg>
                <span>Importing...</span>
              </>
            ) : (
              <>
                <ArrowUpTrayIcon className="w-4 h-4" />
                <span>Import</span>
              </>
            )}
          </button>
        </div>
      </div>
    </div>
  );
}
