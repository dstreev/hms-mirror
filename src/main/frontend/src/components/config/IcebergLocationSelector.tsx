import React, { useState } from 'react';
import { icebergLocationOptions, ICEBERG_LOCATION } from '../../types/StrategySelection';

interface IcebergLocationSelectorProps {
  selection: ICEBERG_LOCATION | null;
  onSelection: (location: ICEBERG_LOCATION) => void;
  onBack: () => void;
}

const IcebergLocationSelector: React.FC<IcebergLocationSelectorProps> = ({ selection, onSelection, onBack }) => {
  const [selectedLocation, setSelectedLocation] = useState<ICEBERG_LOCATION | null>(selection);

  const handleSelection = (locationValue: ICEBERG_LOCATION) => {
    setSelectedLocation(locationValue);
  };

  const handleContinue = () => {
    if (selectedLocation && onSelection) {
      onSelection(selectedLocation);
    }
  };

  return (
    <div className="p-6">
      <div className="text-center mb-8">
        <h3 className="text-xl font-semibold text-gray-900 mb-2">Where do you want the Iceberg tables?</h3>
        <p className="text-gray-600">
          Choose whether to convert tables in-place or during migration to a different cluster.
        </p>
      </div>

      <div className="space-y-4 mb-8">
        {icebergLocationOptions.map((option) => (
          <div 
            key={option.value} 
            className={`cursor-pointer rounded-lg border-2 p-4 transition-all hover:bg-gray-50 ${
              selectedLocation === option.value
                ? 'border-blue-500 bg-blue-50'
                : 'border-gray-200'
            }`}
            onClick={() => handleSelection(option.value)}
          >
            <div className="flex items-start space-x-4">
              <div className="text-2xl flex-shrink-0">{option.icon}</div>
              <div className="flex-1 min-w-0">
                <h4 className="text-lg font-medium text-gray-900">{option.label}</h4>
                <p className="text-blue-600 mt-2 font-medium">→ Uses: {option.strategy} strategy</p>
              </div>
              <div className="flex-shrink-0">
                <input
                  type="radio"
                  name="icebergLocation"
                  value={option.value}
                  checked={selectedLocation === option.value}
                  onChange={() => handleSelection(option.value)}
                  className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300"
                />
              </div>
            </div>
          </div>
        ))}
      </div>

      <div className="bg-blue-50 rounded-lg p-4 mb-6">
        <h4 className="text-lg font-medium text-blue-900 mb-3">🧊 Iceberg Requirements</h4>
        <p className="text-blue-800 mb-2"><strong>Your target cluster must support Iceberg:</strong></p>
        <ul className="text-blue-700 space-y-1 ml-4 list-disc">
          <li>CDP 7.3.1+ or CDW 1.5.1+</li>
          <li>Public Cloud environments (August 2023+)</li>
          <li>Proper Iceberg catalog configuration</li>
          <li>Compatible Hive/Spark versions</li>
        </ul>
        <p className="text-blue-800 mt-3 text-sm italic">Note: Iceberg conversion is typically one-way. Review CDP Iceberg limitations before proceeding.</p>
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
          disabled={!selectedLocation}
          className="px-6 py-3 bg-blue-600 text-white font-medium rounded-lg hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 disabled:bg-gray-300 disabled:cursor-not-allowed"
        >
          Continue
        </button>
      </div>
    </div>
  );
};

export default IcebergLocationSelector;