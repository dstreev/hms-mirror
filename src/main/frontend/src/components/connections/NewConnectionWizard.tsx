import React, { useState, useEffect } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { ArrowLeftIcon } from '@heroicons/react/24/outline';
import { 
  ConnectionFormData, 
  DEFAULT_CONNECTION_FORM,
  validateBasicInfoStep,
  validateConnectionSettingsStep,
  validateConnectionPoolStep,
  validateConnectionForm
} from '../../types/Connection';
import BasicInfoStep from './wizard/BasicInfoStep';
import HiveServer2Step from './wizard/HiveServer2Step';
import MetastoreDirectStep from './wizard/MetastoreDirectStep';
import ConnectionPoolStep from './wizard/ConnectionPoolStep';
import TestAndSaveStep from './wizard/TestAndSaveStep';
import WizardProgress from './wizard/WizardProgress';
import ErrorDialog from '../common/ErrorDialog';

const NewConnectionWizard: React.FC = () => {
  const navigate = useNavigate();
  const { id } = useParams<{ id: string }>();
  const isEditMode = Boolean(id);
  const [currentStep, setCurrentStep] = useState(0);
  const [formData, setFormData] = useState<ConnectionFormData>(DEFAULT_CONNECTION_FORM);
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [loading, setLoading] = useState(isEditMode);
  const [saving, setSaving] = useState(false);
  const [saveSuccess, setSaveSuccess] = useState(false);
  const [showErrorDialog, setShowErrorDialog] = useState(false);

  const steps = [
    { 
      id: 'basic', 
      title: 'Basic Information',
      component: BasicInfoStep
    },
    { 
      id: 'hiveserver2', 
      title: 'HiveServer2 Connection',
      component: HiveServer2Step
    },
    { 
      id: 'metastore', 
      title: 'Metastore Direct (Optional)',
      component: MetastoreDirectStep
    },
    { 
      id: 'test', 
      title: 'Test & Save',
      component: TestAndSaveStep
    }
  ];

  // Load existing connection data in edit mode
  useEffect(() => {
    if (isEditMode && id) {
      const loadConnection = async () => {
        try {
          setLoading(true);
          const response = await fetch(`/hms-mirror/api/v1/connections/${id}`);
          
          if (!response.ok) {
            throw new Error('Failed to load connection');
          }
          
          const connectionData = await response.json();
          
          // Convert the flattened connection data back to form format
          const convertedFormData: ConnectionFormData = {
            name: connectionData.name || '',
            description: connectionData.description || '',
            environment: connectionData.environment || 'DEV',
            platformType: connectionData.config?.platformType || '',
            hcfsNamespace: connectionData.config?.hcfsNamespace || '',
            hs2Uri: connectionData.config?.hiveServer2?.uri || '',
            hs2Username: connectionData.config?.hiveServer2?.connectionProperties?.user || '',
            hs2Password: connectionData.config?.hiveServer2?.connectionProperties?.password || '',
            hs2DriverClassName: connectionData.config?.hiveServer2?.driverClassName || '',
            hs2JarFile: connectionData.config?.hiveServer2?.jarFile || '',
            hs2Disconnected: connectionData.config?.hiveServer2?.disconnected || false,
            hs2ConnectionProperties: connectionData.config?.hiveServer2?.connectionProperties || {},
            metastoreDirectEnabled: Boolean(connectionData.config?.metastoreDirect?.uri),
            metastoreDirectUri: connectionData.config?.metastoreDirect?.uri || '',
            metastoreDirectType: connectionData.config?.metastoreDirect?.type || '',
            metastoreDirectUsername: connectionData.config?.metastoreDirect?.connectionProperties?.user || '',
            metastoreDirectPassword: connectionData.config?.metastoreDirect?.connectionProperties?.password || '',
            metastoreDirectMinConnections: connectionData.config?.metastoreDirect?.connectionPool?.min || 2,
            metastoreDirectMaxConnections: connectionData.config?.metastoreDirect?.connectionPool?.max || 10,
            connectionPoolLib: connectionData.config?.connectionPoolLib || 'HYBRID',
            partitionDiscoveryAuto: connectionData.config?.partitionDiscovery?.auto ?? true,
            partitionDiscoveryInitMSCK: connectionData.config?.partitionDiscovery?.initMSCK ?? true,
            partitionBucketLimit: connectionData.config?.partitionDiscovery?.partitionBucketLimit || 100,
            createIfNotExists: connectionData.config?.createIfNotExists || false,
            enableAutoTableStats: connectionData.config?.enableAutoTableStats || false,
            enableAutoColumnStats: connectionData.config?.enableAutoColumnStats || false
          };
          
          setFormData(convertedFormData);
        } catch (error) {
          console.error('Error loading connection:', error);
          setErrors({ general: 'Failed to load connection data' });
        } finally {
          setLoading(false);
        }
      };
      
      loadConnection();
    }
  }, [isEditMode, id]);

  const handleBack = () => {
    if (currentStep === 0) {
      navigate('/connections');
    } else {
      setCurrentStep(currentStep - 1);
    }
  };

  const handleNext = () => {
    // Validate current step based on step index
    let validation;
    switch (currentStep) {
      case 0: // Basic Info
        validation = validateBasicInfoStep(formData);
        break;
      case 1: // HiveServer2 Connection
        validation = validateConnectionSettingsStep(formData);
        break;
      case 2: // Metastore Direct (Optional)
        validation = { isValid: true, errors: {} }; // Optional step
        break;
      default:
        validation = { isValid: true, errors: {} };
    }
    
    if (!validation.isValid) {
      setErrors(validation.errors);
      setShowErrorDialog(true);
      return;
    }
    
    setErrors({});
    
    if (currentStep < steps.length - 1) {
      setCurrentStep(currentStep + 1);
    }
  };

  // Auto-determine connection pool type based on platform
  const getConnectionPoolType = (platformType: string): 'DBCP2' | 'HIKARI' | 'HYBRID' => {
    switch (platformType) {
      case 'CDP7_1':
      case 'CDP7_2':
        return 'HIKARI'; // CDP prefers HikariCP
      case 'HDP2':
      case 'HDP3':
      case 'CDH5':
      case 'CDH6':
        return 'DBCP2'; // Legacy platforms use DBCP2
      case 'APACHE':
      case 'EMR':
      case 'GENERIC':
      default:
        return 'HYBRID'; // Generic/unknown platforms use hybrid
    }
  };

  const handleFormChange = (updates: Partial<ConnectionFormData>) => {
    // Auto-update connection pool type when platform type changes
    if (updates.platformType) {
      updates.connectionPoolLib = getConnectionPoolType(updates.platformType);
    }

    setFormData({ ...formData, ...updates });
    // Clear errors for fields that are being updated
    const newErrors = { ...errors };
    Object.keys(updates).forEach(key => {
      delete newErrors[key];
    });
    setErrors(newErrors);
  };

  const handleSave = async () => {
    console.log('Save button clicked');
    setSaving(true);
    setErrors({});
    
    // Final validation of all fields before saving
    const finalValidation = validateConnectionForm(formData);
    console.log('Validation result:', finalValidation);
    
    if (!finalValidation.isValid) {
      console.log('Validation failed:', finalValidation.errors);
      setErrors(finalValidation.errors);
      setShowErrorDialog(true);
      setSaving(false);
      return;
    }
    
    const requestPayload = {
      name: formData.name,
      description: formData.description,
      environment: formData.environment,
      config: {
        platformType: formData.platformType,
        hcfsNamespace: formData.hcfsNamespace,
        hiveServer2: {
          uri: formData.hs2Uri,
          connectionProperties: {
            user: formData.hs2Username,
            password: formData.hs2Password,
            ...formData.hs2ConnectionProperties
          },
          driverClassName: formData.hs2DriverClassName,
          jarFile: formData.hs2JarFile,
          disconnected: formData.hs2Disconnected
        },
        metastoreDirect: formData.metastoreDirectEnabled ? {
          uri: formData.metastoreDirectUri,
          type: formData.metastoreDirectType,
          connectionProperties: {
            user: formData.metastoreDirectUsername,
            password: formData.metastoreDirectPassword
          },
          connectionPool: {
            min: formData.metastoreDirectMinConnections,
            max: formData.metastoreDirectMaxConnections
          }
        } : undefined,
        connectionPoolLib: formData.connectionPoolLib,
        partitionDiscovery: {
          auto: formData.partitionDiscoveryAuto,
          initMSCK: formData.partitionDiscoveryInitMSCK,
          partitionBucketLimit: formData.partitionBucketLimit
        },
        createIfNotExists: formData.createIfNotExists,
        enableAutoTableStats: formData.enableAutoTableStats,
        enableAutoColumnStats: formData.enableAutoColumnStats
      }
    };
    
    console.log('Sending request payload:', requestPayload);
    
    try {
      const url = isEditMode 
        ? `/hms-mirror/api/v1/connections/${id}`
        : '/hms-mirror/api/v1/connections';
      const method = isEditMode ? 'PUT' : 'POST';
      
      const response = await fetch(url, {
        method,
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(requestPayload)
      });

      console.log('Response status:', response.status);
      console.log('Response ok:', response.ok);

      if (!response.ok) {
        const errorText = await response.text();
        console.error('Server error response:', errorText);
        
        // Try to parse as JSON first
        try {
          const errorData = JSON.parse(errorText);
          const errorMessage = errorData.message || errorData.error || `HTTP ${response.status}`;
          throw new Error(errorMessage);
        } catch (parseError) {
          // If JSON parsing fails, use the raw text
          throw new Error(`Failed to ${isEditMode ? 'update' : 'create'} connection: ${response.status} ${errorText}`);
        }
      }

      const result = await response.json();
      console.log('Success response:', result);

      // Show brief success message before navigation to provide user feedback
      console.log(`Connection ${isEditMode ? 'updated' : 'created'} successfully`);
      setSaveSuccess(true);
      
      // Add a small delay to show success state, then navigate
      setTimeout(() => {
        console.log('Navigating to /connections');
        navigate('/connections');
      }, 500);
    } catch (error) {
      console.error('Error creating connection:', error);
      const errorMessage = error instanceof Error ? error.message : 'Unknown error occurred';
      setErrors({ general: errorMessage });
      setShowErrorDialog(true);
    } finally {
      setSaving(false);
    }
  };

  const CurrentStepComponent = steps[currentStep].component;

  // Show loading spinner while loading connection data in edit mode
  if (loading) {
    return (
      <div className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="flex items-center justify-center h-64">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600"></div>
          <span className="ml-3 text-gray-600">Loading connection data...</span>
        </div>
      </div>
    );
  }

  return (
    <div className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      {/* Header */}
      <div className="mb-8">
        <button
          onClick={handleBack}
          className="flex items-center text-blue-600 hover:text-blue-700 mb-4"
        >
          <ArrowLeftIcon className="h-4 w-4 mr-2" />
          {currentStep === 0 ? 'Back to Connections' : 'Back'}
        </button>
        
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold text-gray-900">
              {isEditMode ? 'Edit Connection' : 'New Connection'}
            </h1>
            <p className="text-gray-600 mt-2">
              {isEditMode ? 'Modify cluster connection profile' : 'Create a new cluster connection profile'}
            </p>
          </div>
        </div>
      </div>

      {/* Progress Indicator */}
      <div className="mb-8">
        <WizardProgress 
          steps={steps}
          currentStep={currentStep}
        />
      </div>

      {/* Step Content */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200">
        <div className="px-6 py-4 border-b border-gray-200">
          <h2 className="text-lg font-semibold text-gray-900">
            Step {currentStep + 1} of {steps.length}: {steps[currentStep].title}
          </h2>
        </div>
        
        <div className="p-6">
          <CurrentStepComponent
            formData={formData}
            errors={errors}
            onChange={handleFormChange}
            onNext={handleNext}
            onBack={handleBack}
            onSave={handleSave}
            isLastStep={currentStep === steps.length - 1}
            saving={saving}
            saveSuccess={saveSuccess}
          />
        </div>
      </div>

      {/* Navigation Buttons */}
      {currentStep < steps.length - 1 && (
        <div className="mt-6 flex justify-between">
          <button
            onClick={handleBack}
            className="px-4 py-2 border border-gray-300 text-gray-700 rounded-lg hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-blue-500"
          >
            {currentStep === 0 ? 'Cancel' : '← Back'}
          </button>
          
          <button
            onClick={handleNext}
            className="px-6 py-2 bg-blue-600 text-white font-medium rounded-lg hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500"
          >
            Continue →
          </button>
        </div>
      )}
      
      {/* Error Dialog */}
      <ErrorDialog
        isOpen={showErrorDialog}
        onClose={() => setShowErrorDialog(false)}
        title="Validation Errors"
        errors={errors}
      />
    </div>
  );
};

export default NewConnectionWizard;