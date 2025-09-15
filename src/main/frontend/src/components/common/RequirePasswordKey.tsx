import React from 'react';
import { useConfiguration } from '../../contexts/ConfigurationContext';
import { KeyIcon, ExclamationTriangleIcon } from '@heroicons/react/24/outline';

interface RequirePasswordKeyProps {
  children: React.ReactNode;
}

const RequirePasswordKey: React.FC<RequirePasswordKeyProps> = ({ children }) => {
  const { state } = useConfiguration();
  const hasEncryptedPasswordsWithoutKey = state.config?.encryptedPasswords && !state.config?.passwordKey;

  if (hasEncryptedPasswordsWithoutKey) {
    return (
      <div className="p-8">
        <div className="max-w-4xl mx-auto">
          <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-8 text-center">
            <KeyIcon className="h-12 w-12 text-yellow-600 mx-auto mb-4" />
            <h2 className="text-xl font-semibold text-gray-900 mb-2">Password Key Required</h2>
            <p className="text-gray-600 mb-4">
              Your configuration has encrypted passwords, but no password key is set.
            </p>
            <p className="text-gray-600 mb-6">
              The password key is needed to decrypt passwords <em>in memory</em> when connecting to your clusters. 
              Without it, the system cannot establish connections for migration operations.
            </p>
            <a
              href="/encryption"
              className="inline-flex items-center px-4 py-2 bg-yellow-600 text-white rounded-md hover:bg-yellow-700 focus:ring-2 focus:ring-yellow-500 focus:ring-offset-2"
            >
              <KeyIcon className="h-4 w-4 mr-2" />
              Go to Password Encryption
            </a>
          </div>
        </div>
      </div>
    );
  }

  return <>{children}</>;
};

export default RequirePasswordKey;