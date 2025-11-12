import React, { useState } from 'react';
import {
  ArrowPathIcon,
  FolderArrowDownIcon,
  CheckCircleIcon,
  ExclamationCircleIcon,
  InformationCircleIcon
} from '@heroicons/react/24/outline';

interface ImportResult {
  status: string;
  scanned: number;
  imported: number;
  skipped: number;
  failed: number;
  importedPaths: string[];
  skippedPaths: string[];
  errors: string[];
}

const CliReportsPage: React.FC = () => {
  const [importing, setImporting] = useState(false);
  const [result, setResult] = useState<ImportResult | null>(null);
  const [error, setError] = useState<string | null>(null);

  const handleImportAll = async () => {
    try {
      setImporting(true);
      setError(null);
      setResult(null);

      const response = await fetch('/hms-mirror/api/v1/reports/import/all', {
        method: 'POST',
      });

      if (!response.ok) {
        throw new Error(`Import failed: ${response.statusText}`);
      }

      const data = await response.json();
      setResult(data);

    } catch (err: any) {
      console.error('Failed to import reports:', err);
      setError(err.message || 'Failed to import reports');
    } finally {
      setImporting(false);
    }
  };

  return (
    <div className="container mx-auto px-4 py-6 max-w-7xl">
      {/* Header */}
      <div className="mb-6">
        <h1 className="text-3xl font-bold text-gray-900">CLI Reports Import</h1>
        <p className="mt-1 text-sm text-gray-600">
          Import filesystem-based reports from CLI executions into RocksDB
        </p>
      </div>

      {/* Info Card */}
      <div className="mb-6 bg-blue-50 border border-blue-200 rounded-lg p-4">
        <div className="flex">
          <InformationCircleIcon className="h-5 w-5 text-blue-400 mr-3 flex-shrink-0 mt-0.5" />
          <div className="text-sm text-blue-700">
            <p className="font-medium mb-2">About CLI Report Import</p>
            <p className="mb-2">
              This utility scans the <code className="bg-blue-100 px-1 rounded">$HOME/.hms-mirror/reports</code> directory
              for report directories containing <code className="bg-blue-100 px-1 rounded">session-config.yaml</code> and{' '}
              <code className="bg-blue-100 px-1 rounded">run-status.yaml</code> files.
            </p>
            <ul className="list-disc list-inside space-y-1">
              <li>Reports are identified by their relative path from the reports directory</li>
              <li>Previously imported reports will be skipped (based on their key)</li>
              <li>After import, reports will be visible in the Runtime Reports page</li>
            </ul>
          </div>
        </div>
      </div>

      {/* Import Button */}
      <div className="bg-white shadow rounded-lg border border-gray-200 p-6">
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-lg font-medium text-gray-900">Import All Reports</h2>
            <p className="mt-1 text-sm text-gray-500">
              Scan the reports directory and import all discovered reports
            </p>
          </div>
          <button
            onClick={handleImportAll}
            disabled={importing}
            className="inline-flex items-center px-4 py-2 border border-transparent rounded-md shadow-sm text-sm font-medium text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {importing ? (
              <>
                <ArrowPathIcon className="h-5 w-5 mr-2 animate-spin" />
                Importing...
              </>
            ) : (
              <>
                <FolderArrowDownIcon className="h-5 w-5 mr-2" />
                Import Reports
              </>
            )}
          </button>
        </div>
      </div>

      {/* Error Message */}
      {error && (
        <div className="mt-6 rounded-md bg-red-50 p-4 border border-red-200">
          <div className="flex">
            <ExclamationCircleIcon className="h-5 w-5 text-red-400 mr-3" />
            <div>
              <h3 className="text-sm font-medium text-red-800">Import Error</h3>
              <p className="mt-1 text-sm text-red-700">{error}</p>
            </div>
          </div>
        </div>
      )}

      {/* Import Results */}
      {result && (
        <div className="mt-6 space-y-4">
          {/* Summary Stats */}
          <div className="bg-white shadow rounded-lg border border-gray-200 p-6">
            <h2 className="text-lg font-medium text-gray-900 mb-4">Import Summary</h2>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
              <div className="bg-gray-50 rounded-lg p-4">
                <div className="text-2xl font-bold text-gray-900">{result.scanned}</div>
                <div className="text-sm text-gray-600">Scanned</div>
              </div>
              <div className="bg-green-50 rounded-lg p-4">
                <div className="text-2xl font-bold text-green-700">{result.imported}</div>
                <div className="text-sm text-green-600">Imported</div>
              </div>
              <div className="bg-yellow-50 rounded-lg p-4">
                <div className="text-2xl font-bold text-yellow-700">{result.skipped}</div>
                <div className="text-sm text-yellow-600">Skipped</div>
              </div>
              <div className="bg-red-50 rounded-lg p-4">
                <div className="text-2xl font-bold text-red-700">{result.failed}</div>
                <div className="text-sm text-red-600">Failed</div>
              </div>
            </div>
          </div>

          {/* Imported Paths */}
          {result.importedPaths.length > 0 && (
            <div className="bg-white shadow rounded-lg border border-gray-200 p-6">
              <div className="flex items-center mb-4">
                <CheckCircleIcon className="h-5 w-5 text-green-500 mr-2" />
                <h2 className="text-lg font-medium text-gray-900">
                  Successfully Imported ({result.importedPaths.length})
                </h2>
              </div>
              <div className="max-h-60 overflow-y-auto">
                <ul className="space-y-1">
                  {result.importedPaths.map((path, index) => (
                    <li key={index} className="text-sm text-gray-700 font-mono bg-gray-50 px-3 py-2 rounded">
                      {path}
                    </li>
                  ))}
                </ul>
              </div>
            </div>
          )}

          {/* Skipped Paths */}
          {result.skippedPaths.length > 0 && (
            <div className="bg-white shadow rounded-lg border border-gray-200 p-6">
              <div className="flex items-center mb-4">
                <InformationCircleIcon className="h-5 w-5 text-yellow-500 mr-2" />
                <h2 className="text-lg font-medium text-gray-900">
                  Skipped (Already Imported) ({result.skippedPaths.length})
                </h2>
              </div>
              <div className="max-h-60 overflow-y-auto">
                <ul className="space-y-1">
                  {result.skippedPaths.map((path, index) => (
                    <li key={index} className="text-sm text-gray-600 font-mono bg-gray-50 px-3 py-2 rounded">
                      {path}
                    </li>
                  ))}
                </ul>
              </div>
            </div>
          )}

          {/* Errors */}
          {result.errors.length > 0 && (
            <div className="bg-white shadow rounded-lg border border-red-200 p-6">
              <div className="flex items-center mb-4">
                <ExclamationCircleIcon className="h-5 w-5 text-red-500 mr-2" />
                <h2 className="text-lg font-medium text-gray-900">
                  Errors ({result.errors.length})
                </h2>
              </div>
              <div className="max-h-60 overflow-y-auto">
                <ul className="space-y-2">
                  {result.errors.map((error, index) => (
                    <li key={index} className="text-sm text-red-700 bg-red-50 px-3 py-2 rounded">
                      {error}
                    </li>
                  ))}
                </ul>
              </div>
            </div>
          )}

          {/* Success Message */}
          {result.imported > 0 && (
            <div className="rounded-md bg-green-50 p-4 border border-green-200">
              <div className="flex">
                <CheckCircleIcon className="h-5 w-5 text-green-400 mr-3" />
                <div>
                  <p className="text-sm text-green-700">
                    Successfully imported {result.imported} report{result.imported !== 1 ? 's' : ''}.
                    You can now view them in the <a href="/runtime/reports" className="font-medium underline">Runtime Reports</a> page.
                  </p>
                </div>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
};

export default CliReportsPage;
