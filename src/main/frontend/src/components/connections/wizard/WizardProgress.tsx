import React from 'react';
import { CheckIcon } from '@heroicons/react/24/outline';

interface WizardProgressProps {
  steps: Array<{ id: string; title: string }>;
  currentStep: number;
}

const WizardProgress: React.FC<WizardProgressProps> = ({ steps, currentStep }) => {
  return (
    <nav aria-label="Progress">
      <ol className="flex items-center">
        {steps.map((step, index) => (
          <li key={step.id} className={`relative ${index !== steps.length - 1 ? 'pr-8 sm:pr-20' : ''}`}>
            {/* Connector Line */}
            {index !== steps.length - 1 && (
              <div className="absolute inset-0 flex items-center" aria-hidden="true">
                <div className={`h-0.5 w-full ${
                  index < currentStep ? 'bg-blue-600' : 'bg-gray-200'
                }`} />
              </div>
            )}
            
            {/* Step Circle */}
            <div className="relative flex items-center justify-center">
              <div className={`
                flex h-8 w-8 items-center justify-center rounded-full
                ${index < currentStep 
                  ? 'bg-blue-600 text-white' 
                  : index === currentStep
                  ? 'border-2 border-blue-600 bg-white text-blue-600'
                  : 'border-2 border-gray-300 bg-white text-gray-400'
                }
              `}>
                {index < currentStep ? (
                  <CheckIcon className="h-4 w-4" />
                ) : (
                  <span className="text-sm font-medium">{index + 1}</span>
                )}
              </div>
              
              {/* Step Label */}
              <div className={`
                absolute top-10 left-1/2 transform -translate-x-1/2 whitespace-nowrap text-xs font-medium
                ${index <= currentStep ? 'text-blue-600' : 'text-gray-400'}
              `}>
                {step.title}
              </div>
            </div>
          </li>
        ))}
      </ol>
    </nav>
  );
};

export default WizardProgress;