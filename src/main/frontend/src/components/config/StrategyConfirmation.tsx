import React from 'react';
import { strategyInfo, STRATEGIES, StrategyRecommendation } from '../../types/StrategySelection';

interface StrategyConfirmationProps {
  strategy: StrategyRecommendation | null;
  onConfirm: () => void;
  onBack: () => void;
  onCancel: () => void;
}

const StrategyConfirmation: React.FC<StrategyConfirmationProps> = ({ strategy, onConfirm, onBack, onCancel }) => {
  if (!strategy) {
    return (
      <div className="p-6 text-center">
        <div className="text-red-600 mb-4">
          <svg className="w-16 h-16 mx-auto mb-4" fill="currentColor" viewBox="0 0 20 20">
            <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z" clipRule="evenodd" />
          </svg>
          <h3 className="text-xl font-semibold mb-2">No Strategy Available</h3>
          <p className="text-gray-600">{strategy?.error || 'Unable to determine a suitable strategy based on your selections.'}</p>
          {strategy?.suggestion && (
            <div className="mt-4 p-4 bg-yellow-50 rounded-lg">
              <p className="text-yellow-800"><strong>Suggestion:</strong> {strategy.suggestion}</p>
            </div>
          )}
        </div>
        
        <div className="flex justify-center space-x-4">
          <button onClick={onBack} className="px-4 py-2 border border-gray-300 text-gray-700 rounded-lg hover:bg-gray-50">
            ← Back to Revise
          </button>
          <button onClick={onCancel} className="px-4 py-2 border border-red-300 text-red-700 rounded-lg hover:bg-red-50">
            Cancel
          </button>
        </div>
      </div>
    );
  }

  const info = strategyInfo[strategy.strategy];
  
  if (!info) {
    return (
      <div className="p-6 text-center">
        <h3 className="text-xl font-semibold text-red-600 mb-4">Unknown strategy: {strategy.strategy}</h3>
        <button onClick={onBack} className="px-4 py-2 border border-gray-300 text-gray-700 rounded-lg hover:bg-gray-50">
          ← Back
        </button>
      </div>
    );
  }

  const getAlternativeStrategies = (currentStrategy: STRATEGIES) => {
    switch (currentStrategy) {
      case STRATEGIES.SQL:
        return [
          { 
            strategy: STRATEGIES.HYBRID, 
            reason: 'Auto-selects method per table - good if you\'re unsure about partition sizes' 
          },
          { 
            strategy: STRATEGIES.EXPORT_IMPORT, 
            reason: 'Better for smaller partitioned tables with complex structures' 
          }
        ];
      case STRATEGIES.HYBRID:
        return [
          { 
            strategy: STRATEGIES.SQL, 
            reason: 'If you know most tables have large partition counts' 
          },
          { 
            strategy: STRATEGIES.EXPORT_IMPORT, 
            reason: 'If you know most tables have small partition counts' 
          }
        ];
      case STRATEGIES.EXPORT_IMPORT:
        return [
          { 
            strategy: STRATEGIES.HYBRID, 
            reason: 'Mix of SQL and EXPORT_IMPORT based on table characteristics' 
          },
          { 
            strategy: STRATEGIES.SQL, 
            reason: 'If you have many large partitioned tables' 
          }
        ];
      default:
        return [];
    }
  };

  const alternatives = getAlternativeStrategies(strategy.strategy);

  return (
    <div className="p-6">
      <div className="text-center mb-8">
        <div className="inline-flex items-center justify-center w-12 h-12 bg-green-100 rounded-full mb-4">
          <svg className="w-6 h-6 text-green-600" fill="currentColor" viewBox="0 0 20 20">
            <path fillRule="evenodd" d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z" clipRule="evenodd" />
          </svg>
        </div>
        <h3 className="text-2xl font-bold text-gray-900 mb-2">🎯 Recommended Strategy: {info.name}</h3>
      </div>

      {/* Main Strategy Card */}
      <div className="bg-green-50 border-2 border-green-200 rounded-lg p-6 mb-6">
        <div className="flex items-center justify-between mb-4">
          <h4 className="text-lg font-semibold text-green-900">{info.name} Strategy</h4>
          <span className="bg-green-100 text-green-800 text-sm font-medium px-2.5 py-0.5 rounded-full">Recommended</span>
        </div>
        
        <div className="space-y-4">
          <div>
            <h5 className="font-medium text-gray-900 mb-2">Why this strategy?</h5>
            <p className="text-gray-700">{strategy.reason}</p>
            {strategy.path && strategy.path.length > 0 && (
              <p className="text-sm text-gray-600 italic mt-2">
                <strong>Your selections:</strong> {strategy.path.join(' → ')}
              </p>
            )}
          </div>

          <div>
            <h5 className="font-medium text-gray-900 mb-2">Key Features:</h5>
            <ul className="space-y-1">
              {info.features.map((feature, index) => (
                <li key={index} className="flex items-center text-gray-700">
                  <svg className="w-4 h-4 text-green-500 mr-2 flex-shrink-0" fill="currentColor" viewBox="0 0 20 20">
                    <path fillRule="evenodd" d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z" clipRule="evenodd" />
                  </svg>
                  {feature}
                </li>
              ))}
            </ul>
          </div>

          <div>
            <h5 className="font-medium text-gray-900 mb-2">Requirements:</h5>
            <ul className="space-y-1">
              {info.requirements.map((requirement, index) => (
                <li key={index} className="flex items-center text-gray-700">
                  <span className="w-2 h-2 bg-gray-400 rounded-full mr-2 flex-shrink-0"></span>
                  {requirement}
                </li>
              ))}
            </ul>
          </div>

          {strategy.intermediateStorage && (
            <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
              <p className="text-blue-800">
                <strong>📦 Special Configuration:</strong> This strategy will use intermediate storage for data transit.
              </p>
            </div>
          )}
        </div>
      </div>

      {/* Alternative Strategies */}
      {alternatives.length > 0 && (
        <div className="mb-6">
          <h4 className="text-lg font-medium text-gray-900 mb-4">Other Compatible Strategies:</h4>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {alternatives.map((alt, index) => {
              const altInfo = strategyInfo[alt.strategy];
              return (
                <div key={index} className="bg-gray-50 border border-gray-200 rounded-lg p-4">
                  <h5 className="font-medium text-gray-900 mb-2">{alt.strategy}</h5>
                  <p className="text-sm text-gray-600 mb-2">{altInfo.description}</p>
                  <p className="text-sm text-blue-600">• {alt.reason}</p>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* Next Steps */}
      <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 mb-6">
        <h5 className="font-medium text-blue-900 mb-2">📋 Next Steps</h5>
        <p className="text-blue-800 mb-2">After confirming your strategy, you'll configure:</p>
        <ul className="text-blue-700 space-y-1 ml-4 list-disc text-sm">
          <li>Source and target cluster connections</li>
          <li>Database and table selections</li>
          <li>Strategy-specific settings</li>
          <li>Migration execution options</li>
        </ul>
      </div>

      {/* Action Buttons */}
      <div className="flex justify-between items-center">
        <button 
          onClick={onBack}
          className="px-4 py-2 border border-gray-300 text-gray-700 rounded-lg hover:bg-gray-50"
        >
          ← Change Strategy
        </button>
        <button 
          onClick={onConfirm}
          className="px-6 py-3 bg-green-600 text-white font-medium rounded-lg hover:bg-green-700 focus:outline-none focus:ring-2 focus:ring-green-500 focus:ring-offset-2"
        >
          Continue with {info.name} Strategy →
        </button>
      </div>

      <div className="text-center mt-4">
        <button 
          onClick={onCancel}
          className="text-gray-500 hover:text-gray-700 text-sm underline"
        >
          Cancel and return to main menu
        </button>
      </div>
    </div>
  );
};

export default StrategyConfirmation;