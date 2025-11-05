import React, { useState } from 'react';
import { ChevronLeftIcon, ChevronRightIcon, ArrowPathIcon } from '@heroicons/react/24/outline';

interface StrategySelectionWizardProps {
  initialStrategy?: string;
  onStrategySelected: (strategy: string, reasoning: string[]) => void;
  onCancel?: () => void;
}

interface WizardState {
  step: number;
  migrationGoal?: string;
  clusterAccess?: string;
  tableCharacteristics?: string;
  icebergLocation?: string;
  selectedStrategy?: string;
  reasoning: string[];
}

const StrategySelectionWizard: React.FC<StrategySelectionWizardProps> = ({
  initialStrategy,
  onStrategySelected,
  onCancel
}) => {
  const [wizardState, setWizardState] = useState<WizardState>({
    step: 0,
    reasoning: []
  });

  const updateState = (updates: Partial<WizardState>) => {
    setWizardState(prev => ({ ...prev, ...updates }));
  };

  const handleMigrationGoalSelect = (goal: string) => {
    updateState({ migrationGoal: goal, step: 1 });

    // Direct strategy assignments for certain goals
    switch (goal) {
      case 'schemas-only':
        finalizeStrategy('SCHEMA_ONLY', [
          'You want to move schemas only',
          'Handles data separately via distcp',
          'Generates distcp plans for data movement'
        ]);
        break;
      case 'storage-migration':
        finalizeStrategy('STORAGE_MIGRATION', [
          'You want to move data within the same cluster',
          'Changes storage location (e.g., HDFS→Ozone, HDFS→S3)',
          'Can use SQL or DISTCP for data movement'
        ]);
        break;
      case 'read-only-test':
        finalizeStrategy('LINKED', [
          'You want to test new cluster with old data (read-only)',
          'Creates read-only access to existing data',
          'Automatically sets readOnly=true and noPurge=true'
        ]);
        break;
      case 'shared-storage':
        finalizeStrategy('COMMON', [
          'Clusters share the same physical storage',
          'Only metadata needs to move',
          'No data movement needed'
        ]);
        break;
      case 'extract-schemas':
        finalizeStrategy('DUMP', [
          'You want to extract schemas only',
          'No target cluster required',
          'Generates SQL files for later manual execution'
        ]);
        break;
    }
  };

  const finalizeStrategy = (strategy: string, reasons: string[]) => {
    updateState({
      selectedStrategy: strategy,
      reasoning: reasons,
      step: 99 // Confirmation screen
    });
  };

  const handleClusterAccessSelect = (access: string) => {
    updateState({ clusterAccess: access });

    if (wizardState.migrationGoal === 'schemas-data') {
      if (access === 'yes') {
        updateState({ step: 2 });
      } else if (access === 'intermediate') {
        finalizeStrategy('SQL', [
          'You want to move schemas + data between clusters',
          'Clusters need intermediate storage for data transfer',
          'SQL strategy recommended with intermediateStorage configured'
        ]);
      } else if (access === 'no') {
        // Show error state
        updateState({ step: 98 }); // Error screen
      }
    } else if (wizardState.migrationGoal === 'iceberg-conversion') {
      if (access === 'same-cluster') {
        finalizeStrategy('STORAGE_MIGRATION', [
          'You want to convert to Iceberg format in-place',
          'Same cluster conversion',
          'STORAGE_MIGRATION with SQL recommended'
        ]);
      } else if (access === 'different-cluster') {
        updateState({ step: 2, clusterAccess: 'yes' });
      }
    }
  };

  const handleTableCharacteristicsSelect = (characteristics: string) => {
    updateState({ tableCharacteristics: characteristics });

    const baseReasoning = [
      'You want to move schemas + data between clusters',
      'Clusters can access each other\'s storage'
    ];

    if (characteristics === 'mixed') {
      finalizeStrategy('HYBRID', [
        ...baseReasoning,
        'You have a mix of small and large partitioned tables',
        'Auto-selects best method per table based on partition count'
      ]);
    } else if (characteristics === 'small-partitions') {
      finalizeStrategy('EXPORT_IMPORT', [
        ...baseReasoning,
        'You have mostly tables with < 100 partitions',
        'EXPORT_IMPORT is good for small partitioned tables'
      ]);
    } else if (characteristics === 'large-partitions') {
      finalizeStrategy('SQL', [
        ...baseReasoning,
        'You have mostly tables with > 100 partitions',
        'SQL strategy is better for large partitioned tables'
      ]);
    }
  };

  const handleBack = () => {
    if (wizardState.step > 0) {
      updateState({ step: wizardState.step - 1 });
    }
  };

  const handleConfirm = () => {
    if (wizardState.selectedStrategy) {
      onStrategySelected(wizardState.selectedStrategy, wizardState.reasoning);
    }
  };

  const handleRestart = () => {
    setWizardState({
      step: 0,
      reasoning: []
    });
  };

  const renderBreadcrumb = () => {
    const steps: string[] = [];
    if (wizardState.migrationGoal) steps.push('Migration Goal');
    if (wizardState.step >= 1 && wizardState.step < 99) steps.push('Details');
    if (wizardState.step === 99) steps.push('Strategy Selected');

    return (
      <div className="flex items-center space-x-2 text-sm text-gray-600 mb-6">
        {steps.map((step, index) => (
          <React.Fragment key={step}>
            {index > 0 && <span>›</span>}
            <span className={index === steps.length - 1 ? 'font-medium text-blue-600' : ''}>
              {step}
            </span>
          </React.Fragment>
        ))}
      </div>
    );
  };

  const renderStep0 = () => (
    <div className="space-y-4">
      <h3 className="text-lg font-medium text-gray-900 mb-4">
        What is your primary migration goal?
      </h3>

      <div className="space-y-3">
        <button
          onClick={() => handleMigrationGoalSelect('schemas-data')}
          className="w-full text-left p-4 border border-gray-300 rounded-lg hover:border-blue-500 hover:bg-blue-50 transition-colors"
        >
          <div className="font-medium text-gray-900">🔄 Move schemas + data between clusters</div>
          <div className="text-sm text-gray-600 mt-1">Migrate both metadata and data to a different cluster</div>
        </button>

        <button
          onClick={() => handleMigrationGoalSelect('schemas-only')}
          className="w-full text-left p-4 border border-gray-300 rounded-lg hover:border-blue-500 hover:bg-blue-50 transition-colors"
        >
          <div className="font-medium text-gray-900">📋 Move schemas only, handle data separately</div>
          <div className="text-sm text-gray-600 mt-1">Migrate metadata only, use distcp for data movement</div>
        </button>

        <button
          onClick={() => handleMigrationGoalSelect('iceberg-conversion')}
          className="w-full text-left p-4 border border-gray-300 rounded-lg hover:border-blue-500 hover:bg-blue-50 transition-colors"
        >
          <div className="font-medium text-gray-900">🧊 Convert to Iceberg format</div>
          <div className="text-sm text-gray-600 mt-1">Convert existing tables to Iceberg table format</div>
        </button>

        <button
          onClick={() => handleMigrationGoalSelect('storage-migration')}
          className="w-full text-left p-4 border border-gray-300 rounded-lg hover:border-blue-500 hover:bg-blue-50 transition-colors"
        >
          <div className="font-medium text-gray-900">📦 Move data within cluster to new storage</div>
          <div className="text-sm text-gray-600 mt-1">Change storage location within same cluster (HDFS→Ozone, HDFS→S3)</div>
        </button>

        <button
          onClick={() => handleMigrationGoalSelect('read-only-test')}
          className="w-full text-left p-4 border border-gray-300 rounded-lg hover:border-blue-500 hover:bg-blue-50 transition-colors"
        >
          <div className="font-medium text-gray-900">🔗 Test new cluster with old data (read-only)</div>
          <div className="text-sm text-gray-600 mt-1">Create read-only access to existing data for testing</div>
        </button>

        <button
          onClick={() => handleMigrationGoalSelect('shared-storage')}
          className="w-full text-left p-4 border border-gray-300 rounded-lg hover:border-blue-500 hover:bg-blue-50 transition-colors"
        >
          <div className="font-medium text-gray-900">🤝 Clusters share same physical storage</div>
          <div className="text-sm text-gray-600 mt-1">Only metadata needs to move, data is already accessible</div>
        </button>

        <button
          onClick={() => handleMigrationGoalSelect('extract-schemas')}
          className="w-full text-left p-4 border border-gray-300 rounded-lg hover:border-blue-500 hover:bg-blue-50 transition-colors"
        >
          <div className="font-medium text-gray-900">💾 Extract schemas only (no target yet)</div>
          <div className="text-sm text-gray-600 mt-1">Generate SQL files for later manual execution</div>
        </button>
      </div>
    </div>
  );

  const renderStep1 = () => {
    if (wizardState.migrationGoal === 'schemas-data') {
      return (
        <div className="space-y-4">
          <h3 className="text-lg font-medium text-gray-900 mb-4">
            Can your clusters access each other's storage?
          </h3>

          <div className="space-y-3">
            <button
              onClick={() => handleClusterAccessSelect('yes')}
              className="w-full text-left p-4 border border-gray-300 rounded-lg hover:border-blue-500 hover:bg-blue-50 transition-colors"
            >
              <div className="font-medium text-gray-900">✅ Yes, clusters can access each other's storage</div>
            </button>

            <button
              onClick={() => handleClusterAccessSelect('intermediate')}
              className="w-full text-left p-4 border border-gray-300 rounded-lg hover:border-blue-500 hover:bg-blue-50 transition-colors"
            >
              <div className="font-medium text-gray-900">❌ No, but we have intermediate storage both can access</div>
            </button>

            <button
              onClick={() => handleClusterAccessSelect('no')}
              className="w-full text-left p-4 border border-gray-300 rounded-lg hover:border-blue-500 hover:bg-blue-50 transition-colors"
            >
              <div className="font-medium text-gray-900">🚫 No shared storage access at all</div>
            </button>
          </div>
        </div>
      );
    } else if (wizardState.migrationGoal === 'iceberg-conversion') {
      return (
        <div className="space-y-4">
          <h3 className="text-lg font-medium text-gray-900 mb-4">
            Where do you want the Iceberg tables?
          </h3>

          <div className="space-y-3">
            <button
              onClick={() => handleClusterAccessSelect('same-cluster')}
              className="w-full text-left p-4 border border-gray-300 rounded-lg hover:border-blue-500 hover:bg-blue-50 transition-colors"
            >
              <div className="font-medium text-gray-900">🎯 Same cluster (in-place conversion)</div>
              <div className="text-sm text-gray-600 mt-1">STORAGE_MIGRATION with SQL recommended</div>
            </button>

            <button
              onClick={() => handleClusterAccessSelect('different-cluster')}
              className="w-full text-left p-4 border border-gray-300 rounded-lg hover:border-blue-500 hover:bg-blue-50 transition-colors"
            >
              <div className="font-medium text-gray-900">🔄 Different cluster (conversion during migration)</div>
              <div className="text-sm text-gray-600 mt-1">SQL strategy with Iceberg conversion recommended</div>
            </button>
          </div>
        </div>
      );
    }
    return null;
  };

  const renderStep2 = () => (
    <div className="space-y-4">
      <h3 className="text-lg font-medium text-gray-900 mb-4">
        What describes your table characteristics?
      </h3>

      <div className="space-y-3">
        <button
          onClick={() => handleTableCharacteristicsSelect('mixed')}
          className="w-full text-left p-4 border border-gray-300 rounded-lg hover:border-blue-500 hover:bg-blue-50 transition-colors"
        >
          <div className="font-medium text-gray-900">🔀 Mix of small and large partitioned tables</div>
          <div className="text-sm text-gray-600 mt-1">HYBRID strategy recommended - Auto-selects best method per table</div>
        </button>

        <button
          onClick={() => handleTableCharacteristicsSelect('small-partitions')}
          className="w-full text-left p-4 border border-gray-300 rounded-lg hover:border-blue-500 hover:bg-blue-50 transition-colors"
        >
          <div className="font-medium text-gray-900">📊 Mostly tables with &lt; 100 partitions</div>
          <div className="text-sm text-gray-600 mt-1">EXPORT_IMPORT strategy recommended - Good for small partitioned tables</div>
        </button>

        <button
          onClick={() => handleTableCharacteristicsSelect('large-partitions')}
          className="w-full text-left p-4 border border-gray-300 rounded-lg hover:border-blue-500 hover:bg-blue-50 transition-colors"
        >
          <div className="font-medium text-gray-900">📈 Mostly tables with &gt; 100 partitions</div>
          <div className="text-sm text-gray-600 mt-1">SQL strategy recommended - Better for large partitioned tables</div>
        </button>
      </div>
    </div>
  );

  const renderError = () => (
    <div className="space-y-4">
      <div className="bg-red-50 border border-red-200 rounded-lg p-6">
        <h3 className="text-lg font-medium text-red-800 mb-2">
          ❌ Direct data movement not possible
        </h3>
        <p className="text-sm text-red-700 mb-4">
          Without shared storage access, direct data movement between clusters is not possible.
        </p>
        <div className="text-sm text-red-700">
          <p className="font-medium mb-2">Consider these options:</p>
          <ul className="list-disc list-inside space-y-1">
            <li>Setting up intermediate storage that both clusters can access</li>
            <li>Using SCHEMA_ONLY strategy + manual data transfer via distcp</li>
          </ul>
        </div>
      </div>

      <div className="flex justify-between">
        <button
          onClick={handleRestart}
          className="inline-flex items-center px-4 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 bg-white hover:bg-gray-50"
        >
          <ArrowPathIcon className="h-4 w-4 mr-2" />
          Start Over
        </button>
        {onCancel && (
          <button
            onClick={onCancel}
            className="px-4 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 bg-white hover:bg-gray-50"
          >
            Cancel
          </button>
        )}
      </div>
    </div>
  );

  const getStrategyDetails = (strategy: string) => {
    const details: Record<string, { emoji: string; features: string[]; requirements: string[] }> = {
      'SQL': {
        emoji: '🔄',
        features: [
          'Supports Iceberg conversion',
          'Better for large partitioned tables',
          'Uses SQL INSERT statements for data movement'
        ],
        requirements: [
          'Target cluster must be accessible',
          'Sufficient processing capacity for SQL operations'
        ]
      },
      'EXPORT_IMPORT': {
        emoji: '📦',
        features: [
          'Better for smaller partitioned tables',
          'More robust for complex table structures',
          'Uses Hive EXPORT/IMPORT mechanism'
        ],
        requirements: [
          'Target cluster must be accessible',
          'Sufficient storage for export staging'
        ]
      },
      'HYBRID': {
        emoji: '🔀',
        features: [
          'Auto-selects method per table',
          'Good if you\'re unsure about partition sizes',
          'Optimizes data movement based on table characteristics'
        ],
        requirements: [
          'Target cluster must be accessible',
          'Works best with mixed workloads'
        ]
      },
      'SCHEMA_ONLY': {
        emoji: '📋',
        features: [
          'Generates distcp plans for data movement',
          'Fastest metadata migration',
          'Allows separate data movement timeline'
        ],
        requirements: [
          'Manual data movement using distcp',
          'Access to both cluster storage systems'
        ]
      },
      'STORAGE_MIGRATION': {
        emoji: '📦',
        features: [
          'Changes storage location within cluster',
          'Supports SQL or DISTCP methods',
          'Can handle HDFS→Ozone, HDFS→S3 migrations'
        ],
        requirements: [
          'Access to both source and target storage',
          'Sufficient capacity in target storage'
        ]
      },
      'LINKED': {
        emoji: '🔗',
        features: [
          'Read-only access for testing',
          'No data movement',
          'Automatically sets safety flags'
        ],
        requirements: [
          'Original cluster must remain accessible',
          'Testing/validation purposes only'
        ]
      },
      'COMMON': {
        emoji: '🤝',
        features: [
          'No data movement needed',
          'Fast migration',
          'Shared storage access'
        ],
        requirements: [
          'Clusters must share physical storage',
          'Metadata-only migration'
        ]
      },
      'DUMP': {
        emoji: '💾',
        features: [
          'Generates SQL files',
          'No target cluster required',
          'Manual execution control'
        ],
        requirements: [
          'Manual SQL execution later',
          'Storage for generated scripts'
        ]
      }
    };

    return details[strategy] || { emoji: '❓', features: [], requirements: [] };
  };

  const renderConfirmation = () => {
    if (!wizardState.selectedStrategy) return null;

    const details = getStrategyDetails(wizardState.selectedStrategy);

    return (
      <div className="space-y-6">
        <div className="bg-blue-50 border border-blue-200 rounded-lg p-6">
          <h3 className="text-xl font-medium text-blue-900 mb-4">
            {details.emoji} Recommended Strategy: {wizardState.selectedStrategy}
          </h3>

          <div className="space-y-4">
            <div>
              <h4 className="font-medium text-blue-900 mb-2">Why this strategy?</h4>
              <ul className="text-sm text-blue-800 space-y-1">
                {wizardState.reasoning.map((reason, index) => (
                  <li key={index}>• {reason}</li>
                ))}
              </ul>
            </div>

            {details.features.length > 0 && (
              <div>
                <h4 className="font-medium text-blue-900 mb-2">Key Features:</h4>
                <ul className="text-sm text-blue-800 space-y-1">
                  {details.features.map((feature, index) => (
                    <li key={index}>✅ {feature}</li>
                  ))}
                </ul>
              </div>
            )}

            {details.requirements.length > 0 && (
              <div>
                <h4 className="font-medium text-blue-900 mb-2">Requirements:</h4>
                <ul className="text-sm text-blue-800 space-y-1">
                  {details.requirements.map((req, index) => (
                    <li key={index}>• {req}</li>
                  ))}
                </ul>
              </div>
            )}
          </div>
        </div>

        <div className="flex justify-between">
          <button
            onClick={handleRestart}
            className="inline-flex items-center px-4 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 bg-white hover:bg-gray-50"
          >
            <ChevronLeftIcon className="h-4 w-4 mr-2" />
            Change Strategy
          </button>
          <button
            onClick={handleConfirm}
            className="inline-flex items-center px-4 py-2 border border-transparent rounded-md text-sm font-medium text-white bg-blue-600 hover:bg-blue-700"
          >
            Continue with {wizardState.selectedStrategy} Strategy
            <ChevronRightIcon className="h-4 w-4 ml-2" />
          </button>
        </div>
      </div>
    );
  };

  return (
    <div className="space-y-6">
      {renderBreadcrumb()}

      {wizardState.step === 0 && renderStep0()}
      {wizardState.step === 1 && renderStep1()}
      {wizardState.step === 2 && renderStep2()}
      {wizardState.step === 98 && renderError()}
      {wizardState.step === 99 && renderConfirmation()}

      {wizardState.step > 0 && wizardState.step < 98 && (
        <div className="flex justify-start">
          <button
            onClick={handleBack}
            className="inline-flex items-center px-4 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 bg-white hover:bg-gray-50"
          >
            <ChevronLeftIcon className="h-4 w-4 mr-2" />
            Back
          </button>
        </div>
      )}
    </div>
  );
};

export default StrategySelectionWizard;