import React, { useState, useEffect } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import { ArrowLeftIcon, ArrowRightIcon, CheckIcon } from '@heroicons/react/24/outline';
import WizardProgress from '../connections/wizard/WizardProgress';
import BasicInfoStep from './wizard/BasicInfoStep';
import DatabaseConfigStep from './wizard/DatabaseConfigStep';
import ReviewAndSaveStep from './wizard/ReviewAndSaveStep';
import { DatasetFormData, DEFAULT_DATASET_FORM } from '../../types/Dataset';
import { datasetApi } from '../../services/api/datasetApi';

const DatasetWizard: React.FC = () => {
  const navigate = useNavigate();
  const location = useLocation();
  const [currentStep, setCurrentStep] = useState(0);
  const [formData, setFormData] = useState<DatasetFormData>(DEFAULT_DATASET_FORM);
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [saving, setSaving] = useState(false);
  const [saveSuccess, setSaveSuccess] = useState(false);
  const [isEditMode, setIsEditMode] = useState(false);
  const [isCopyMode, setIsCopyMode] = useState(false);
  const [originalDatasetKey, setOriginalDatasetKey] = useState<string | null>(null);

  const steps = [
    { 
      id: 'basic', 
      title: 'Basic Info',
      component: BasicInfoStep
    },
    { 
      id: 'databases', 
      title: 'Databases',
      component: DatabaseConfigStep
    },
    { 
      id: 'review', 
      title: 'Review',
      component: ReviewAndSaveStep
    }
  ];

  // Initialize form data for edit or copy mode
  useEffect(() => {
    const state = location.state as { dataset?: DatasetFormData; mode?: string } | null;
    if (state?.dataset) {
      if (state.mode === 'edit') {
        // Edit mode: preserve the key and all data
        setFormData({
          ...state.dataset,
          key: state.dataset.key // Ensure key is preserved for updates
        });
        setIsEditMode(true);
        setOriginalDatasetKey(state.dataset.key || null);
      } else if (state.mode === 'copy') {
        // Copy mode: load data but clear the name and don't copy key
        const { key, ...datasetWithoutKey } = state.dataset;
        setFormData({
          ...datasetWithoutKey,
          name: '' // Clear name for copy mode
          // Don't copy key - backend will generate new key for new dataset
        });
        setIsCopyMode(true);
        setOriginalDatasetKey(null);
      }
    }
  }, [location.state]);

  const validateCurrentStep = (): boolean => {
    const newErrors: Record<string, string> = {};

    switch (currentStep) {
      case 0: // Basic Info
        if (!formData.name.trim()) {
          newErrors.name = 'Dataset name is required';
        }
        if (!formData.description.trim()) {
          newErrors.description = 'Dataset description is required';
        }
        break;
      
      case 1: // Database Configuration
        if (formData.databases.length === 0) {
          newErrors.databases = 'At least one database must be configured';
        } else {
          formData.databases.forEach((db, index) => {
            if (!db.databaseName.trim()) {
              newErrors[`database_${index}_name`] = 'Database name is required';
            }
            if (db.tables.length === 0 && !db.filter) {
              newErrors[`database_${index}_config`] = 'Either specify tables or configure a filter';
            }
          });
        }
        break;
    }

    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  };

  const handleNext = () => {
    if (validateCurrentStep()) {
      if (currentStep < steps.length - 1) {
        setCurrentStep(currentStep + 1);
      }
    }
  };

  const handlePrevious = () => {
    if (currentStep > 0) {
      setCurrentStep(currentStep - 1);
      setErrors({});
    }
  };

  const handleSave = async () => {
    if (!validateCurrentStep()) {
      return;
    }

    setSaving(true);
    try {
      let result;
      // Edit mode: update existing dataset using the key
      if (isEditMode && originalDatasetKey && !isCopyMode) {
        result = await datasetApi.updateDataset(originalDatasetKey, formData);
      } else {
        // Create mode or copy mode: create new dataset
        result = await datasetApi.saveDataset(formData);
      }

      if (result.success) {
        setSaveSuccess(true);
        setTimeout(() => {
          navigate('/datasets');
        }, 2000);
      } else {
        const action = (isEditMode && !isCopyMode) ? 'update' : isCopyMode ? 'copy' : 'save';

        // Check if it's a 409 CONFLICT error (name already exists)
        if (result.status === 409 || result.message?.includes('already exists')) {
          // Navigate back to Basic Info step (step 0) so user can change the name
          setCurrentStep(0);
          setErrors({
            name: result.message || `A dataset with the name "${formData.name}" already exists. Please use a different name.`
          });
        } else {
          setErrors({ save: result.message || `Failed to ${action} dataset` });
        }
      }
    } catch (error: any) {
      const action = (isEditMode && !isCopyMode) ? 'updating' : isCopyMode ? 'copying' : 'saving';

      // Check if error response indicates name conflict
      if (error.status === 409 || error.message?.includes('already exists')) {
        setCurrentStep(0);
        setErrors({
          name: error.message || `A dataset with the name "${formData.name}" already exists. Please use a different name.`
        });
      } else {
        setErrors({ save: error.message || `Network error occurred while ${action} dataset` });
      }
    } finally {
      setSaving(false);
    }
  };

  const updateFormData = (updates: Partial<DatasetFormData>) => {
    setFormData(prev => ({ ...prev, ...updates }));
    setErrors({});
  };

  const CurrentStepComponent = steps[currentStep].component;

  if (saveSuccess) {
    return (
      <div className="container mx-auto px-4 py-8">
        <div className="max-w-2xl mx-auto">
          <div className="bg-green-50 border border-green-200 rounded-lg p-8 text-center">
            <CheckIcon className="mx-auto h-16 w-16 text-green-600 mb-4" />
            <h2 className="text-2xl font-bold text-green-900 mb-2">
              Dataset {isEditMode ? 'Updated' : 'Created'} Successfully!
            </h2>
            <p className="text-green-700 mb-4">
              Your dataset "{formData.name}" has been {isEditMode ? 'updated' : 'saved'} and is ready to use.
            </p>
            <p className="text-sm text-green-600">Redirecting you back to the datasets page...</p>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="container mx-auto px-4 py-8">
      <div className="max-w-4xl mx-auto">
        {/* Header */}
        <div className="mb-8">
          <button
            onClick={() => navigate('/datasets')}
            className="flex items-center text-sm text-gray-500 hover:text-gray-700 mb-4"
          >
            <ArrowLeftIcon className="h-4 w-4 mr-1" />
            Back to Datasets
          </button>
          
          <h1 className="text-3xl font-bold text-gray-900 mb-2">
            {isEditMode ? 'Edit Dataset Wizard' : 'New Dataset Wizard'}
          </h1>
          <p className="text-gray-600">
            {isEditMode 
              ? 'Edit the dataset by updating the basic information and database configurations.'
              : 'Create a new dataset by defining the basic information and database configurations.'
            }
          </p>
        </div>

        {/* Progress Indicator */}
        <div className="mb-8">
          <WizardProgress steps={steps} currentStep={currentStep} />
        </div>

        {/* Wizard Content */}
        <div className="bg-white rounded-lg shadow-lg p-6">
          <div className="mb-6">
            <h2 className="text-xl font-semibold text-gray-900 mb-2">
              {steps[currentStep].title}
            </h2>
          </div>

          <CurrentStepComponent
            formData={formData}
            errors={errors}
            updateFormData={updateFormData}
            isEditMode={isEditMode && !isCopyMode}
          />

          {/* Navigation Buttons */}
          <div className="flex justify-between pt-6 border-t border-gray-200 mt-8">
            <button
              type="button"
              onClick={handlePrevious}
              disabled={currentStep === 0}
              className="flex items-center px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-md hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              <ArrowLeftIcon className="h-4 w-4 mr-2" />
              Previous
            </button>

            <div className="flex space-x-3">
              {currentStep < steps.length - 1 ? (
                <button
                  type="button"
                  onClick={handleNext}
                  className="flex items-center px-6 py-2 text-sm font-medium text-white bg-blue-600 border border-transparent rounded-md hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
                >
                  Next
                  <ArrowRightIcon className="h-4 w-4 ml-2" />
                </button>
              ) : (
                <button
                  type="button"
                  onClick={handleSave}
                  disabled={saving}
                  className="flex items-center px-6 py-2 text-sm font-medium text-white bg-green-600 border border-transparent rounded-md hover:bg-green-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-green-500 disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  {saving ? (
                    <>
                      <svg className="animate-spin -ml-1 mr-3 h-4 w-4 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                        <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                        <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                      </svg>
                      Saving...
                    </>
                  ) : (
                    <>
                      <CheckIcon className="h-4 w-4 mr-2" />
                      {isEditMode ? 'Update Dataset' : 'Save Dataset'}
                    </>
                  )}
                </button>
              )}
            </div>
          </div>

          {/* Error Display */}
          {errors.save && (
            <div className="mt-4 p-3 bg-red-50 border border-red-200 rounded-md">
              <p className="text-sm text-red-600">{errors.save}</p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default DatasetWizard;