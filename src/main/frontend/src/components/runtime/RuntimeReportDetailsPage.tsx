import React, { useState, useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { ArrowLeftIcon, ArrowPathIcon } from '@heroicons/react/24/outline';
import { runtimeReportsApi, ConversionResult } from '../../services/api/runtimeReportsApi';

const RuntimeReportDetailsPage: React.FC = () => {
  const { key } = useParams<{ key: string }>();
  const navigate = useNavigate();
  const [report, setReport] = useState<ConversionResult | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (key) {
      loadReport();
    }
  }, [key]);

  const loadReport = async () => {
    if (!key) return;

    try {
      setLoading(true);
      setError(null);
      const result = await runtimeReportsApi.get(key);

      if (result) {
        setReport(result);
      } else {
        setError('Report not found');
      }
    } catch (err: any) {
      console.error('Failed to load report:', err);
      setError(err.message || 'Failed to load report');
    } finally {
      setLoading(false);
    }
  };

  const handleBack = () => {
    navigate('/runtime/reports');
  };

  if (loading) {
    return (
      <div className="container mx-auto px-4 py-6 max-w-7xl">
        <div className="text-center py-12">
          <ArrowPathIcon className="h-12 w-12 text-gray-400 animate-spin mx-auto" />
          <p className="mt-2 text-sm text-gray-600">Loading report details...</p>
        </div>
      </div>
    );
  }

  if (error || !report) {
    return (
      <div className="container mx-auto px-4 py-6 max-w-7xl">
        <div className="mb-4">
          <button
            onClick={handleBack}
            className="inline-flex items-center text-sm text-blue-600 hover:text-blue-800"
          >
            <ArrowLeftIcon className="h-4 w-4 mr-1" />
            Back to Reports
          </button>
        </div>
        <div className="rounded-md bg-red-50 p-4">
          <h3 className="text-sm font-medium text-red-800">Error</h3>
          <p className="mt-1 text-sm text-red-700">{error || 'Report not found'}</p>
        </div>
      </div>
    );
  }

  return (
    <div className="container mx-auto px-4 py-6 max-w-7xl">
      {/* Header */}
      <div className="mb-6">
        <button
          onClick={handleBack}
          className="inline-flex items-center text-sm text-blue-600 hover:text-blue-800 mb-4"
        >
          <ArrowLeftIcon className="h-4 w-4 mr-1" />
          Back to Reports
        </button>

        <h1 className="text-3xl font-bold text-gray-900">Report Details</h1>
        <p className="mt-1 text-sm text-gray-600">
          Conversion result: {report.key}
        </p>
      </div>

      {/* Content - Placeholder */}
      <div className="bg-white shadow rounded-lg border border-gray-200 p-6">
        <div className="space-y-6">
          <div>
            <h2 className="text-lg font-medium text-gray-900 mb-4">Summary</h2>
            <dl className="grid grid-cols-1 gap-x-4 gap-y-6 sm:grid-cols-2">
              <div>
                <dt className="text-sm font-medium text-gray-500">Key</dt>
                <dd className="mt-1 text-sm text-gray-900">{report.key}</dd>
              </div>
              <div>
                <dt className="text-sm font-medium text-gray-500">Created</dt>
                <dd className="mt-1 text-sm text-gray-900">
                  {runtimeReportsApi.formatDate(report.created)}
                </dd>
              </div>
              {report.config && (
                <>
                  <div>
                    <dt className="text-sm font-medium text-gray-500">Configuration</dt>
                    <dd className="mt-1 text-sm text-gray-900">{report.config.name}</dd>
                  </div>
                  {report.config.dataStrategy && (
                    <div>
                      <dt className="text-sm font-medium text-gray-500">Data Strategy</dt>
                      <dd className="mt-1 text-sm text-gray-900">{report.config.dataStrategy}</dd>
                    </div>
                  )}
                </>
              )}
              {report.dataset && (
                <div>
                  <dt className="text-sm font-medium text-gray-500">Dataset</dt>
                  <dd className="mt-1 text-sm text-gray-900">{report.dataset.name}</dd>
                </div>
              )}
            </dl>
          </div>

          {/* Placeholder message */}
          <div className="border-t border-gray-200 pt-6">
            <div className="rounded-md bg-blue-50 p-4">
              <p className="text-sm text-blue-700">
                <strong>Note:</strong> Full report details rendering will be implemented in a future update.
                This page will display comprehensive conversion results, table migration status,
                SQL scripts, and detailed statistics.
              </p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default RuntimeReportDetailsPage;
