import React, { useState } from 'react';
import { migrationGoalOptions, MIGRATION_GOALS } from '../../types/StrategySelection';

interface MigrationGoalSelectorProps {
  selection: MIGRATION_GOALS | null;
  onSelection: (goal: MIGRATION_GOALS) => void;
}

const MigrationGoalSelector: React.FC<MigrationGoalSelectorProps> = ({ selection, onSelection }) => {
  const [selectedGoal, setSelectedGoal] = useState<MIGRATION_GOALS | null>(selection);

  const handleSelection = (goalValue: MIGRATION_GOALS) => {
    setSelectedGoal(goalValue);
  };

  const handleContinue = () => {
    if (selectedGoal && onSelection) {
      onSelection(selectedGoal);
    }
  };

  return (
    <div className="p-6">
      <div className="text-center mb-8">
        <h3 className="text-xl font-semibold text-gray-900 mb-2">What is your primary migration goal?</h3>
        <p className="text-gray-600">
          Choose the option that best describes what you want to accomplish with HMS-Mirror.
        </p>
      </div>

      <div className="space-y-4 mb-8">
        {migrationGoalOptions.map((option) => (
          <div 
            key={option.value} 
            className={`relative cursor-pointer rounded-lg border-2 p-4 transition-all hover:bg-gray-50 ${
              selectedGoal === option.value
                ? 'border-blue-500 bg-blue-50'
                : 'border-gray-200'
            } ${option.isCommon ? 'ring-2 ring-green-200' : ''}`}
            onClick={() => handleSelection(option.value)}
          >
            <div className="flex items-start space-x-4">
              <div className="text-2xl flex-shrink-0">{option.icon}</div>
              <div className="flex-1 min-w-0">
                <div className="flex items-center space-x-2">
                  <h4 className="text-lg font-medium text-gray-900">{option.label}</h4>
                  {option.isCommon && (
                    <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800">
                      Most Common
                    </span>
                  )}
                </div>
                <p className="text-gray-600 mt-1">{option.description}</p>
              </div>
              <div className="flex-shrink-0">
                <input
                  type="radio"
                  name="migrationGoal"
                  value={option.value}
                  checked={selectedGoal === option.value}
                  onChange={() => handleSelection(option.value)}
                  className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300"
                />
              </div>
            </div>
          </div>
        ))}
      </div>

      <div className="flex justify-center">
        <button 
          onClick={handleContinue}
          disabled={!selectedGoal}
          className="px-6 py-3 bg-blue-600 text-white font-medium rounded-lg hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 disabled:bg-gray-300 disabled:cursor-not-allowed"
        >
          Continue
        </button>
      </div>

      <div className="mt-8 bg-gray-50 rounded-lg p-4">
        <details>
          <summary className="cursor-pointer text-blue-600 font-medium">💡 Need help choosing?</summary>
          <div className="mt-3 text-sm text-gray-700">
            <p className="font-medium mb-2">Most users start with "Move schemas + data between clusters" - this covers the majority of migration scenarios.</p>
            <ul className="space-y-1 ml-4 list-disc">
              <li><strong>Schemas + data:</strong> Full migration to a new cluster</li>
              <li><strong>Schemas only:</strong> When you want to handle data movement separately with distcp</li>
              <li><strong>Iceberg conversion:</strong> Modernizing table formats during migration</li>
              <li><strong>Within cluster:</strong> Moving to different storage (e.g., HDFS to S3)</li>
            </ul>
          </div>
        </details>
      </div>
    </div>
  );
};

export default MigrationGoalSelector;