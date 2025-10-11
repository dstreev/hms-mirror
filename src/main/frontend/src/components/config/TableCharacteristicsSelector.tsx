import React, { useState } from 'react';
import { tableCharacteristicsOptions, TABLE_CHARACTERISTICS } from '../../types/StrategySelection';

interface TableCharacteristicsSelectorProps {
  selection: TABLE_CHARACTERISTICS | null;
  onSelection: (characteristics: TABLE_CHARACTERISTICS) => void;
  onBack: () => void;
}

const TableCharacteristicsSelector: React.FC<TableCharacteristicsSelectorProps> = ({ selection, onSelection, onBack }) => {
  const [selectedCharacteristics, setSelectedCharacteristics] = useState<TABLE_CHARACTERISTICS | null>(selection);

  const handleSelection = (characteristicsValue: TABLE_CHARACTERISTICS) => {
    setSelectedCharacteristics(characteristicsValue);
  };

  const handleContinue = () => {
    if (selectedCharacteristics && onSelection) {
      onSelection(selectedCharacteristics);
    }
  };

  return (
    <div className="p-6">
      <div className="text-center mb-8">
        <h3 className="text-xl font-semibold text-gray-900 mb-2">What describes your table characteristics?</h3>
        <p className="text-gray-600">
          The number of partitions in your tables affects which migration strategy works best.
        </p>
      </div>

      <div className="space-y-4 mb-8">
        {tableCharacteristicsOptions.map((option) => (
          <div 
            key={option.value} 
            className={`cursor-pointer rounded-lg border-2 p-4 transition-all hover:bg-gray-50 ${
              selectedCharacteristics === option.value
                ? 'border-blue-500 bg-blue-50'
                : 'border-gray-200'
            }`}
            onClick={() => handleSelection(option.value)}
          >
            <div className="flex items-start space-x-4">
              <div className="text-2xl flex-shrink-0">{option.icon}</div>
              <div className="flex-1 min-w-0">
                <h4 className="text-lg font-medium text-gray-900">{option.label}</h4>
                <p className="text-gray-600 mt-1">{option.description}</p>
                <p className="text-blue-600 mt-2 font-medium">→ Recommends: {option.strategy} strategy</p>
              </div>
              <div className="flex-shrink-0">
                <input
                  type="radio"
                  name="tableCharacteristics"
                  value={option.value}
                  checked={selectedCharacteristics === option.value}
                  onChange={() => handleSelection(option.value)}
                  className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300"
                />
              </div>
            </div>
          </div>
        ))}
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
          disabled={!selectedCharacteristics}
          className="px-6 py-3 bg-blue-600 text-white font-medium rounded-lg hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 disabled:bg-gray-300 disabled:cursor-not-allowed"
        >
          Continue
        </button>
      </div>
    </div>
  );
};

export default TableCharacteristicsSelector;