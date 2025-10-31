import React, { useEffect, useState } from 'react';
import { ConnectionFormData, ENVIRONMENT_OPTIONS } from '../../../types/Connection';

interface BasicInfoStepProps {
  formData: ConnectionFormData;
  errors: Record<string, string>;
  onChange: (updates: Partial<ConnectionFormData>) => void;
  onNext: () => void;
  onBack: () => void;
  isEditMode?: boolean;
}

const BasicInfoStep: React.FC<BasicInfoStepProps> = ({
  formData,
  errors,
  onChange,
  onNext,
  onBack,
  isEditMode = false
}) => {
  const [platformTypes, setPlatformTypes] = useState<string[]>([]);
  const [loadingPlatformTypes, setLoadingPlatformTypes] = useState(true);

  useEffect(() => {
    console.log('🎨 [BasicInfoStep] Component mounted/updated');
    console.log('🎨 [BasicInfoStep] isEditMode:', isEditMode);
    console.log('🎨 [BasicInfoStep] formData:', formData);
    console.log('🎨 [BasicInfoStep] formData.name:', formData.name);
    console.log('🎨 [BasicInfoStep] formData.description:', formData.description);
  }, [formData, isEditMode]);

  useEffect(() => {
    // Fetch platform types from the API
    const fetchPlatformTypes = async () => {
      try {
        const response = await fetch('/hms-mirror/api/v1/app/platform-types');
        if (response.ok) {
          const types = await response.json();
          setPlatformTypes(types);
        } else {
          console.error('Failed to fetch platform types:', response.statusText);
          // Fallback to empty array if API fails
          setPlatformTypes([]);
        }
      } catch (error) {
        console.error('Error fetching platform types:', error);
        setPlatformTypes([]);
      } finally {
        setLoadingPlatformTypes(false);
      }
    };

    fetchPlatformTypes();
  }, []);
  return (
    <div className="space-y-6">
      {/* Connection Name */}
      <div>
        <label htmlFor="name" className="block text-sm font-medium text-gray-700 mb-2">
          Connection Name *
        </label>
        <input
          type="text"
          id="name"
          value={formData.name}
          onChange={(e) => onChange({ name: e.target.value })}
          className={`block w-full px-3 py-2 border rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500 ${
            errors.name ? 'border-red-300' : 'border-gray-300'
          } ${isEditMode ? 'bg-gray-100 cursor-not-allowed' : ''}`}
          placeholder="e.g., Production CDP Cluster"
          required
          readOnly={isEditMode}
          disabled={isEditMode}
        />
        {errors.name && (
          <p className="mt-1 text-sm text-red-600">{errors.name}</p>
        )}
        <p className="mt-1 text-xs text-gray-500">
          {isEditMode ? '🔒 Connection name cannot be changed after creation' : '⚠️ Must be unique across all connections'}
        </p>
      </div>

      {/* Description */}
      <div>
        <label htmlFor="description" className="block text-sm font-medium text-gray-700 mb-2">
          Description
        </label>
        <textarea
          id="description"
          rows={3}
          value={formData.description}
          onChange={(e) => onChange({ description: e.target.value })}
          className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500"
          placeholder="Optional description of this cluster environment"
        />
      </div>

      {/* Platform Type */}
      <div>
        <label htmlFor="platformType" className="block text-sm font-medium text-gray-700 mb-2">
          Platform Type *
        </label>
        <select
          id="platformType"
          value={formData.platformType}
          onChange={(e) => onChange({ platformType: e.target.value as any })}
          className={`block w-full px-3 py-2 border rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500 ${
            errors.platformType ? 'border-red-300' : 'border-gray-300'
          }`}
          required
          disabled={loadingPlatformTypes}
        >
          <option value="">
            {loadingPlatformTypes ? 'Loading platform types...' : 'Select platform type...'}
          </option>
          {platformTypes.map((platformType) => (
            <option key={platformType} value={platformType}>
              {platformType}
            </option>
          ))}
        </select>
        {errors.platformType && (
          <p className="mt-1 text-sm text-red-600">{errors.platformType}</p>
        )}
        <p className="mt-1 text-xs text-gray-500">
          Platform type determines Hive version compatibility and connection behavior
        </p>
      </div>

      {/* Environment Type */}
      <div>
        <label htmlFor="environment" className="block text-sm font-medium text-gray-700 mb-2">
          Environment Type
        </label>
        <select
          id="environment"
          value={formData.environment}
          onChange={(e) => onChange({ environment: e.target.value as any })}
          className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500"
        >
          <option value="">Select environment type...</option>
          {ENVIRONMENT_OPTIONS.map((option) => (
            <option key={option.value} value={option.value}>
              {option.label}
            </option>
          ))}
        </select>
        <p className="mt-1 text-xs text-gray-500">
          Environment classification helps organize and identify connections
        </p>
      </div>

      {/* Help Section */}
      <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
        <h4 className="text-sm font-medium text-blue-900 mb-2">💡 Tips</h4>
        <ul className="text-sm text-blue-700 space-y-1">
          <li>• Use descriptive names that identify the cluster purpose</li>
          <li>• Environment types help organize connections by lifecycle stage</li>
          <li>• Connection profiles can be reused across multiple migrations</li>
        </ul>
      </div>
    </div>
  );
};

export default BasicInfoStep;