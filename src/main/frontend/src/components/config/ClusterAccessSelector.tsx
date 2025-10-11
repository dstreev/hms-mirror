import React, { useState } from 'react';
import { clusterAccessOptions, CLUSTER_ACCESS } from '../../types/StrategySelection';

interface ClusterAccessSelectorProps {
  selection: CLUSTER_ACCESS | null;
  onSelection: (access: CLUSTER_ACCESS) => void;
  onBack: () => void;
}

const ClusterAccessSelector: React.FC<ClusterAccessSelectorProps> = ({ selection, onSelection, onBack }) => {
  const [selectedAccess, setSelectedAccess] = useState<CLUSTER_ACCESS | null>(selection);

  const handleSelection = (accessValue: CLUSTER_ACCESS) => {
    setSelectedAccess(accessValue);
  };

  const handleContinue = () => {
    if (selectedAccess && onSelection) {
      onSelection(selectedAccess);
    }
  };

  const getAccessDetails = (accessType: CLUSTER_ACCESS) => {
    switch (accessType) {
      case CLUSTER_ACCESS.DIRECT_ACCESS:
        return {
          details: "Both clusters can read and write to each other's storage locations",
          examples: ["Shared HDFS namespace", "Both clusters have access to same S3 buckets", "Cross-cluster storage permissions configured"]
        };
      case CLUSTER_ACCESS.INTERMEDIATE_STORAGE:
        return {
          details: "A shared storage location that both clusters can access for data transit",
          examples: ["Shared S3 bucket for temporary data", "Common HDFS location accessible by both", "Cloud storage accessible from both environments"]
        };
      case CLUSTER_ACCESS.NO_ACCESS:
        return {
          details: "Clusters cannot access each other's storage directly",
          examples: ["Isolated networks", "Different cloud providers", "Security restrictions prevent cross-access"],
          warning: "This will limit migration options significantly"
        };
      default:
        return { details: "", examples: [] };
    }
  };

  return (
    <div className="p-6">
      <div className="text-center mb-8">
        <h3 className="text-xl font-semibold text-gray-900 mb-2">Can your clusters access each other's storage?</h3>
        <p className="text-gray-600">
          This determines which data movement strategies are available for your migration.
        </p>
      </div>

      <div className="space-y-4 mb-8">
        {clusterAccessOptions.map((option) => {
          const details = getAccessDetails(option.value);
          return (
            <div 
              key={option.value} 
              className={`cursor-pointer rounded-lg border-2 p-4 transition-all hover:bg-gray-50 ${
                selectedAccess === option.value
                  ? 'border-blue-500 bg-blue-50'
                  : 'border-gray-200'
              } ${option.value === CLUSTER_ACCESS.NO_ACCESS ? 'border-yellow-300' : ''}`}
              onClick={() => handleSelection(option.value)}
            >
              <div className="flex items-start space-x-4">
                <div className="text-2xl flex-shrink-0">{option.icon}</div>
                <div className="flex-1 min-w-0">
                  <h4 className="text-lg font-medium text-gray-900">{option.label}</h4>
                  <p className="text-gray-600 mt-1">{details.details}</p>
                  {details.warning && (
                    <p className="text-yellow-700 mt-2 font-medium">⚠️ {details.warning}</p>
                  )}
                </div>
                <div className="flex-shrink-0">
                  <input
                    type="radio"
                    name="clusterAccess"
                    value={option.value}
                    checked={selectedAccess === option.value}
                    onChange={() => handleSelection(option.value)}
                    className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300"
                  />
                </div>
              </div>
              
              {selectedAccess === option.value && details.examples && details.examples.length > 0 && (
                <div className="mt-4 pl-12 border-t border-gray-200 pt-3">
                  <strong className="text-sm text-gray-700">Examples:</strong>
                  <ul className="mt-1 text-sm text-gray-600 space-y-1">
                    {details.examples.map((example, index) => (
                      <li key={index}>• {example}</li>
                    ))}
                  </ul>
                </div>
              )}
            </div>
          );
        })}
      </div>

      <div className="flex justify-between">
        <button 
          onClick={onBack}
          className="px-4 py-2 border border-gray-300 text-gray-700 rounded-lg hover:bg-gray-50"
        >
          ← Back
        </button>
        <button 
          onClick={handleContinue}
          disabled={!selectedAccess}
          className="px-6 py-3 bg-blue-600 text-white font-medium rounded-lg hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 disabled:bg-gray-300 disabled:cursor-not-allowed"
        >
          Continue
        </button>
      </div>
    </div>
  );
};

export default ClusterAccessSelector;