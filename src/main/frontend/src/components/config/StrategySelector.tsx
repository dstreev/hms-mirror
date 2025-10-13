import React, { useState } from 'react';
import { 
  MIGRATION_GOALS, 
  CLUSTER_ACCESS, 
  TABLE_CHARACTERISTICS,
  ICEBERG_LOCATION,
  STRATEGIES,
  StrategyRecommendation,
  StrategySelectionResult,
  strategyInfo
} from '../../types/StrategySelection';
import MigrationGoalSelector from './MigrationGoalSelector';
import ClusterAccessSelector from './ClusterAccessSelector';
import TableCharacteristicsSelector from './TableCharacteristicsSelector';
import IcebergLocationSelector from './IcebergLocationSelector';
import StrategyConfirmation from './StrategyConfirmation';

interface StrategySelectorProps {
  onStrategySelected: (result: StrategySelectionResult) => void;
  onCancel: () => void;
}

interface Selections {
  migrationGoal: MIGRATION_GOALS | null;
  clusterAccess: CLUSTER_ACCESS | null;
  tableCharacteristics: TABLE_CHARACTERISTICS | null;
  icebergLocation: ICEBERG_LOCATION | null;
}

const StrategySelector: React.FC<StrategySelectorProps> = ({ onStrategySelected, onCancel }) => {
  const [currentStep, setCurrentStep] = useState<string>('goal');
  const [selections, setSelections] = useState<Selections>({
    migrationGoal: null,
    clusterAccess: null,
    tableCharacteristics: null,
    icebergLocation: null
  });
  const [recommendedStrategy, setRecommendedStrategy] = useState<StrategyRecommendation | null>(null);
  const [decisionPath, setDecisionPath] = useState<string[]>(['Migration Goal']);

  const updateSelection = (key: keyof Selections, value: any) => {
    const newSelections = { ...selections, [key]: value };
    setSelections(newSelections);
    
    // Determine recommended strategy based on current selections
    const strategy = determineStrategy(newSelections);
    if (strategy) {
      setRecommendedStrategy(strategy);
    }
  };

  const determineStrategy = (selections: Selections): StrategyRecommendation | null => {
    const { migrationGoal, clusterAccess, tableCharacteristics, icebergLocation } = selections;

    // Direct strategy mappings for single-answer goals
    switch (migrationGoal) {
      case MIGRATION_GOALS.SCHEMAS_ONLY:
        return { 
          strategy: STRATEGIES.SCHEMA_ONLY, 
          reason: 'You want to move schemas only and handle data separately',
          path: ['Schemas only migration']
        };
      
      case MIGRATION_GOALS.MOVE_WITHIN_CLUSTER:
        return { 
          strategy: STRATEGIES.STORAGE_MIGRATION, 
          reason: 'You want to move data within the same cluster to new storage',
          path: ['In-cluster storage migration']
        };
      
      case MIGRATION_GOALS.TEST_READ_ONLY:
        return { 
          strategy: STRATEGIES.LINKED, 
          reason: 'You want read-only testing access to existing data',
          path: ['Read-only testing setup']
        };
      
      case MIGRATION_GOALS.SHARED_STORAGE:
        return { 
          strategy: STRATEGIES.COMMON, 
          reason: 'Your clusters share the same physical storage',
          path: ['Shared storage metadata migration']
        };
      
      case MIGRATION_GOALS.EXTRACT_SCHEMAS:
        return { 
          strategy: STRATEGIES.DUMP, 
          reason: 'You want to extract schemas without a target cluster',
          path: ['Schema extraction only']
        };
    }

    // Complex decision trees for goals requiring multiple questions
    if (migrationGoal === MIGRATION_GOALS.CONVERT_ICEBERG) {
      if (icebergLocation === ICEBERG_LOCATION.SAME_CLUSTER) {
        return { 
          strategy: STRATEGIES.STORAGE_MIGRATION, 
          reason: 'You want in-place Iceberg conversion on the same cluster',
          path: ['Iceberg conversion', 'Same cluster (in-place)']
        };
      } else if (icebergLocation === ICEBERG_LOCATION.DIFFERENT_CLUSTER) {
        // Continue to cluster access questions
        if (clusterAccess && tableCharacteristics) {
          return determineDataMigrationStrategy(clusterAccess, tableCharacteristics, true);
        }
      }
    }

    if (migrationGoal === MIGRATION_GOALS.MOVE_SCHEMAS_DATA) {
      if (clusterAccess === CLUSTER_ACCESS.NO_ACCESS) {
        return {
          strategy: STRATEGIES.SCHEMA_ONLY, // Fallback to schema only
          error: 'Direct data movement not possible without shared storage',
          suggestion: 'Consider: Setting up intermediate storage, or using SCHEMA_ONLY + manual data transfer',
          path: ['Move schemas + data', 'No shared storage access'],
          reason: 'Fallback to schema-only migration due to storage constraints'
        };
      }
      
      if (clusterAccess && tableCharacteristics) {
        return determineDataMigrationStrategy(clusterAccess, tableCharacteristics, false);
      }
    }

    return null;
  };

  const determineDataMigrationStrategy = (clusterAccess: CLUSTER_ACCESS, tableCharacteristics: TABLE_CHARACTERISTICS, isIceberg: boolean): StrategyRecommendation => {
    const basePath = isIceberg 
      ? ['Iceberg conversion', 'Different cluster', 'Clusters can access storage']
      : ['Move schemas + data', 'Clusters can access storage'];

    if (clusterAccess === CLUSTER_ACCESS.DIRECT_ACCESS) {
      switch (tableCharacteristics) {
        case TABLE_CHARACTERISTICS.MIXED_PARTITIONS:
          return { 
            strategy: STRATEGIES.HYBRID, 
            reason: `You have mixed partition sizes and clusters can access each other's storage`,
            path: [...basePath, 'Mixed partition sizes']
          };
        case TABLE_CHARACTERISTICS.SMALL_PARTITIONS:
          return { 
            strategy: STRATEGIES.EXPORT_IMPORT, 
            reason: `You have mostly small partitioned tables and clusters can access each other's storage`,
            path: [...basePath, 'Small partitioned tables']
          };
        case TABLE_CHARACTERISTICS.LARGE_PARTITIONS:
          return { 
            strategy: STRATEGIES.SQL, 
            reason: `You have mostly large partitioned tables and clusters can access each other's storage`,
            path: [...basePath, 'Large partitioned tables']
          };
      }
    } else if (clusterAccess === CLUSTER_ACCESS.INTERMEDIATE_STORAGE) {
      return { 
        strategy: STRATEGIES.SQL, 
        reason: `SQL with intermediate storage is recommended for your setup`,
        path: [...basePath.slice(0, -1), 'Intermediate storage available'],
        intermediateStorage: true
      };
    }

    return {
      strategy: STRATEGIES.SQL,
      reason: 'Default SQL strategy for data migration',
      path: basePath
    };
  };

  const handleNext = (nextStep: string, breadcrumbLabel?: string) => {
    setCurrentStep(nextStep);
    if (breadcrumbLabel && !decisionPath.includes(breadcrumbLabel)) {
      setDecisionPath([...decisionPath, breadcrumbLabel]);
    }
  };

  const handleBack = () => {
    const newPath = [...decisionPath];
    newPath.pop();
    setDecisionPath(newPath);

    // Determine previous step based on current step and selections
    if (currentStep === 'confirmation') {
      if (selections.migrationGoal === MIGRATION_GOALS.CONVERT_ICEBERG) {
        setCurrentStep('icebergLocation');
      } else if (selections.migrationGoal === MIGRATION_GOALS.MOVE_SCHEMAS_DATA) {
        setCurrentStep('tableCharacteristics');
      } else {
        setCurrentStep('goal');
      }
    } else if (currentStep === 'tableCharacteristics') {
      setCurrentStep('clusterAccess');
    } else if (currentStep === 'clusterAccess') {
      if (selections.migrationGoal === MIGRATION_GOALS.CONVERT_ICEBERG && selections.icebergLocation === ICEBERG_LOCATION.DIFFERENT_CLUSTER) {
        setCurrentStep('icebergLocation');
      } else {
        setCurrentStep('goal');
      }
    } else if (currentStep === 'icebergLocation') {
      setCurrentStep('goal');
    } else {
      setCurrentStep('goal');
    }
  };

  const handleStrategyConfirm = () => {
    if (recommendedStrategy && onStrategySelected) {
      onStrategySelected({
        strategy: recommendedStrategy.strategy,
        reason: recommendedStrategy.reason,
        path: recommendedStrategy.path,
        intermediateStorage: recommendedStrategy.intermediateStorage || false
      });
    }
  };

  const handleDirectStrategySelection = (strategy: STRATEGIES) => {
    onStrategySelected({
      strategy: strategy,
      reason: `Directly selected ${strategyInfo[strategy].name} strategy`,
      path: ['Direct Selection'],
      intermediateStorage: false
    });
  };

  const renderBreadcrumb = () => (
    <nav className="mb-6">
      <div className="bg-gray-50 px-4 py-2 rounded-lg">
        <span className="text-sm text-gray-600">
          {decisionPath.map((crumb, index) => (
            <span key={index}>
              <span className={index === decisionPath.length - 1 ? 'text-blue-600 font-medium' : ''}>
                {crumb}
              </span>
              {index < decisionPath.length - 1 && ' > '}
            </span>
          ))}
        </span>
      </div>
    </nav>
  );

  const renderCurrentStep = () => {
    switch (currentStep) {
      case 'goal':
        return (
          <MigrationGoalSelector
            selection={selections.migrationGoal}
            onSelection={(goal: MIGRATION_GOALS) => {
              updateSelection('migrationGoal', goal);
              
              // Determine next step based on goal
              switch (goal) {
                case MIGRATION_GOALS.MOVE_SCHEMAS_DATA:
                  handleNext('clusterAccess', 'Cluster Access');
                  break;
                case MIGRATION_GOALS.CONVERT_ICEBERG:
                  handleNext('icebergLocation', 'Iceberg Location');
                  break;
                default:
                  handleNext('confirmation', 'Strategy Selected');
                  break;
              }
            }}
          />
        );

      case 'clusterAccess':
        return (
          <ClusterAccessSelector
            selection={selections.clusterAccess}
            onSelection={(access: CLUSTER_ACCESS) => {
              updateSelection('clusterAccess', access);
              
              if (access === CLUSTER_ACCESS.NO_ACCESS) {
                handleNext('confirmation', 'Strategy Selected');
              } else {
                handleNext('tableCharacteristics', 'Table Characteristics');
              }
            }}
            onBack={handleBack}
          />
        );

      case 'tableCharacteristics':
        return (
          <TableCharacteristicsSelector
            selection={selections.tableCharacteristics}
            onSelection={(characteristics: TABLE_CHARACTERISTICS) => {
              updateSelection('tableCharacteristics', characteristics);
              handleNext('confirmation', 'Strategy Selected');
            }}
            onBack={handleBack}
          />
        );

      case 'icebergLocation':
        return (
          <IcebergLocationSelector
            selection={selections.icebergLocation}
            onSelection={(location: ICEBERG_LOCATION) => {
              updateSelection('icebergLocation', location);
              
              if (location === ICEBERG_LOCATION.SAME_CLUSTER) {
                handleNext('confirmation', 'Strategy Selected');
              } else {
                handleNext('clusterAccess', 'Cluster Access');
              }
            }}
            onBack={handleBack}
          />
        );

      case 'confirmation':
        return (
          <StrategyConfirmation
            strategy={recommendedStrategy}
            onConfirm={handleStrategyConfirm}
            onBack={handleBack}
            onCancel={onCancel}
          />
        );

      default:
        return <div>Unknown step: {currentStep}</div>;
    }
  };

  return (
    <div className="max-w-4xl mx-auto">
      <div className="text-center mb-8">
        <h2 className="text-2xl font-bold text-gray-900 mb-2">Choose Your Migration Strategy</h2>
        <p className="text-gray-600">Answer a few questions to get the best strategy for your needs</p>
      </div>
      
      {/* Direct Strategy Selection */}
      <div className="bg-blue-50 border border-blue-200 rounded-lg p-6 mb-8">
        <h3 className="text-lg font-semibold text-gray-900 mb-3">Skip the wizard - Select strategy directly</h3>
        <p className="text-sm text-gray-600 mb-4">If you already know which strategy you need, click below to skip the decision wizard:</p>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          {Object.entries(strategyInfo).map(([strategyKey, info]) => (
            <button
              key={strategyKey}
              onClick={() => handleDirectStrategySelection(strategyKey as STRATEGIES)}
              className="p-3 bg-white border border-gray-300 rounded-lg hover:border-blue-500 hover:bg-blue-50 transition-colors text-left"
            >
              <div className="font-medium text-sm text-gray-900">{info.name}</div>
              <div className="text-xs text-gray-600 mt-1">{info.description}</div>
            </button>
          ))}
        </div>
        <div className="mt-4 pt-4 border-t border-blue-200">
          <p className="text-xs text-gray-500">Not sure which strategy to choose? Use the guided wizard below.</p>
        </div>
      </div>
      
      {renderBreadcrumb()}
      
      <div className="bg-white">
        {renderCurrentStep()}
      </div>
    </div>
  );
};

export default StrategySelector;