import React, { useState } from 'react';
import { InformationCircleIcon, KeyIcon, ShieldCheckIcon, ShieldExclamationIcon } from '@heroicons/react/24/outline';
import { useConfiguration } from '../../contexts/ConfigurationContext';
import { configApi } from '../../services/api/configApi';

const PasswordEncryptionPage: React.FC = () => {
  const { state, updateConfig } = useConfiguration();
  const [isEncrypting, setIsEncrypting] = useState(false);
  const [isChecking, setIsChecking] = useState(false);
  const [isResetting, setIsResetting] = useState(false);
  const [showResetConfirm, setShowResetConfirm] = useState(false);
  
  // Get values from server state with defaults
  const encryptedPasswords = state.config?.encryptedPasswords || false;
  const passwordKey = state.config?.passwordKey || '';
  const hasEncryptedPasswordsWithoutKey = encryptedPasswords && !passwordKey;

  const handlePasswordKeyChange = (value: string) => {
    updateConfig({ passwordKey: value });
  };

  const handleEncryptDecrypt = async () => {
    console.log('handleEncryptDecrypt called');
    console.log('Password key length:', passwordKey?.length);
    console.log('Encrypted passwords state:', encryptedPasswords);
    console.log('Current config:', state.config);
    
    if (!passwordKey || passwordKey.length < 6) {
      alert('Password key must be at least 6 characters.');
      return;
    }

    setIsEncrypting(true);
    
    if (encryptedPasswords) {
      // Decrypt passwords
      console.log('Attempting to decrypt passwords');
      const decryptedConfig = await configApi.decryptPasswords(state.config!, passwordKey);
      if (decryptedConfig) {
        updateConfig({ ...decryptedConfig, encryptedPasswords: false });
        alert('Passwords have been decrypted successfully.');
      } else {
        alert('Failed to decrypt passwords. Please check your password key.');
      }
    } else {
      // Encrypt passwords
      console.log('Attempting to encrypt passwords');
      const encryptedConfig = await configApi.encryptPasswords(state.config!, passwordKey);
      console.log('Encryption result:', encryptedConfig);
      if (encryptedConfig) {
        updateConfig({ ...encryptedConfig, encryptedPasswords: true });
        alert('Passwords have been encrypted successfully.');
      } else {
        alert('Failed to encrypt passwords.');
      }
    }
    
    setIsEncrypting(false);
  };

  const handleCheckPasswordKey = async () => {
    if (!passwordKey || passwordKey.length < 6) {
      alert('Please enter a password key of at least 6 characters to check.');
      return;
    }

    if (!encryptedPasswords) {
      alert('Passwords are not currently encrypted. There is nothing to check.');
      return;
    }

    setIsChecking(true);
    
    try {
      // Try to decrypt passwords in memory to verify the key
      const response = await fetch('/hms-mirror/api/v2/config/check-password-key', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          config: state.config,
          passwordKey: passwordKey,
        }),
      });

      if (response.ok) {
        const result = await response.json();
        if (result.valid) {
          alert('✓ Password key is correct! All encrypted passwords can be decrypted successfully.');
        } else {
          alert('✗ Password key is incorrect. Unable to decrypt passwords with this key.');
        }
      } else {
        alert('Failed to check password key. Please try again.');
      }
    } catch (error) {
      console.error('Error checking password key:', error);
      alert('An error occurred while checking the password key.');
    } finally {
      setIsChecking(false);
    }
  };

  const handleResetEncryption = async () => {
    if (!showResetConfirm) {
      setShowResetConfirm(true);
      return;
    }

    setIsResetting(true);
    
    try {
      // Reset encryption by removing encrypted passwords and marking as not encrypted
      const response = await fetch('/hms-mirror/api/v2/config/reset-encryption', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          config: state.config,
        }),
      });

      if (response.ok) {
        const resetConfig = await response.json();
        updateConfig(resetConfig);
        alert('Encryption has been reset. All encrypted passwords have been removed. Please re-enter your passwords and encrypt them again.');
        setShowResetConfirm(false);
      } else {
        alert('Failed to reset encryption. Please try again.');
      }
    } catch (error) {
      console.error('Error resetting encryption:', error);
      alert('An error occurred while resetting encryption.');
    } finally {
      setIsResetting(false);
      setShowResetConfirm(false);
    }
  };

  return (
    <div className="p-8">
      <div className="max-w-4xl mx-auto">
        {/* Header */}
        <div className="mb-8">
          <h1 className="text-3xl font-bold text-gray-900">Password Encryption</h1>
          <p className="text-gray-600 mt-2">Manage password encryption for secure configuration storage</p>
        </div>

        {state.error && (
          <div className="mb-6 bg-red-50 border border-red-200 rounded-md p-4">
            <div className="text-sm text-red-700">{state.error}</div>
          </div>
        )}

        {/* Warning when passwords are encrypted but no key is set */}
        {hasEncryptedPasswordsWithoutKey && (
          <div className="mb-6 bg-red-50 border-2 border-red-300 rounded-lg p-4">
            <div className="flex">
              <div className="flex-shrink-0">
                <InformationCircleIcon className="h-6 w-6 text-red-400" aria-hidden="true" />
              </div>
              <div className="ml-3">
                <h3 className="text-sm font-semibold text-red-800">Password Key Required</h3>
                <div className="mt-2 text-sm text-red-700">
                  <p>Your configuration has encrypted passwords, but no password key is currently set.</p>
                  <p className="mt-2">
                    <strong>How it works:</strong> When passwords are encrypted in the configuration file, 
                    the password key is used to decrypt them <em>in memory</em> when making connections. 
                    The passwords remain encrypted in the saved configuration file.
                  </p>
                  <p className="mt-2">
                    <strong>Action needed:</strong> Enter your password key below to enable connections to your clusters. 
                    The key will be held in memory for the duration of this session.
                  </p>
                </div>
              </div>
            </div>
          </div>
        )}

        {/* Status Card */}
        <div className={`mb-8 rounded-lg shadow-lg border-2 ${
          encryptedPasswords 
            ? 'bg-green-50 border-green-300' 
            : 'bg-yellow-50 border-yellow-300'
        }`}>
          <div className="p-6">
            <div className="flex items-center mb-4">
              {encryptedPasswords ? (
                <>
                  <ShieldCheckIcon className="h-8 w-8 text-green-600 mr-3" />
                  <h2 className="text-xl font-semibold text-green-900">Passwords are Encrypted</h2>
                </>
              ) : (
                <>
                  <ShieldExclamationIcon className="h-8 w-8 text-yellow-600 mr-3" />
                  <h2 className="text-xl font-semibold text-yellow-900">Passwords are Not Encrypted</h2>
                </>
              )}
            </div>
            <p className={`text-sm ${encryptedPasswords ? 'text-green-700' : 'text-yellow-700'}`}>
              {encryptedPasswords 
                ? 'Your configuration passwords are currently encrypted. To modify them, you must decrypt them first using the correct password key.'
                : 'Your configuration passwords are stored in plain text. Consider encrypting them for better security.'}
            </p>
          </div>
        </div>

        {/* Configuration Status */}
        <div className="mb-8 bg-gray-50 rounded-lg p-6">
          <h3 className="text-lg font-medium text-gray-900 mb-4">Configuration Status</h3>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <p className="text-sm text-gray-600">Configuration Name</p>
              <p className="font-medium text-gray-900">{state.configName || 'Not Set'}</p>
            </div>
            <div>
              <p className="text-sm text-gray-600">Encryption Status</p>
              <p className={`font-medium ${encryptedPasswords ? 'text-green-600' : 'text-yellow-600'}`}>
                {encryptedPasswords ? 'Encrypted' : 'Not Encrypted'}
              </p>
            </div>
            <div>
              <p className="text-sm text-gray-600">LEFT Cluster Passwords</p>
              <p className="font-medium text-gray-900">
                {state.config?.clusters?.LEFT?.hiveServer2?.connectionProperties?.password ? 'Set' : 'Not Set'}
              </p>
            </div>
            <div>
              <p className="text-sm text-gray-600">RIGHT Cluster Passwords</p>
              <p className="font-medium text-gray-900">
                {state.config?.clusters?.RIGHT?.hiveServer2?.connectionProperties?.password ? 'Set' : 'Not Set'}
              </p>
            </div>
          </div>
        </div>

        {/* Encryption Management */}
        <div className="bg-white rounded-lg shadow-lg p-6">
          <div className="flex items-center mb-4">
            <KeyIcon className="h-6 w-6 text-blue-600 mr-2" />
            <h3 className="text-lg font-medium text-gray-900">Encryption Management</h3>
          </div>
          
          <div className="space-y-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Password Key
              </label>
              <div className="space-y-3">
                <div className="flex items-center space-x-3">
                  <input
                    type="password"
                    value={passwordKey}
                    onChange={(e) => handlePasswordKeyChange(e.target.value)}
                    className="flex-1 p-3 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                    placeholder="Enter encryption key (minimum 6 characters)"
                    disabled={isEncrypting || isChecking || isResetting}
                  />
                  <button
                    type="button"
                    onClick={handleEncryptDecrypt}
                    disabled={isEncrypting || isChecking || isResetting || !passwordKey || passwordKey.length < 6}
                    className={`px-6 py-3 rounded-md font-medium transition-colors ${
                      encryptedPasswords 
                        ? 'bg-yellow-600 hover:bg-yellow-700 text-white' 
                        : 'bg-blue-600 hover:bg-blue-700 text-white'
                    } disabled:opacity-50 disabled:cursor-not-allowed`}
                  >
                    {isEncrypting 
                      ? (encryptedPasswords ? 'Decrypting...' : 'Encrypting...')
                      : (encryptedPasswords ? 'Decrypt Passwords' : 'Encrypt Passwords')
                    }
                  </button>
                </div>
                
                {/* Additional Actions Row */}
                <div className="flex items-center space-x-3">
                  {encryptedPasswords && (
                    <button
                      type="button"
                      onClick={handleCheckPasswordKey}
                      disabled={isChecking || isEncrypting || isResetting || !passwordKey || passwordKey.length < 6}
                      className="px-4 py-2 bg-green-600 hover:bg-green-700 text-white rounded-md font-medium transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                    >
                      {isChecking ? 'Checking...' : 'Check Password Key'}
                    </button>
                  )}
                  
                  {encryptedPasswords && (
                    <>
                      {!showResetConfirm ? (
                        <button
                          type="button"
                          onClick={handleResetEncryption}
                          disabled={isResetting || isEncrypting || isChecking}
                          className="px-4 py-2 bg-red-600 hover:bg-red-700 text-white rounded-md font-medium transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                        >
                          Reset Encryption
                        </button>
                      ) : (
                        <div className="flex items-center space-x-2">
                          <span className="text-red-600 font-medium">Are you sure?</span>
                          <button
                            type="button"
                            onClick={handleResetEncryption}
                            disabled={isResetting}
                            className="px-3 py-2 bg-red-600 hover:bg-red-700 text-white rounded-md text-sm font-medium"
                          >
                            {isResetting ? 'Resetting...' : 'Yes, Reset'}
                          </button>
                          <button
                            type="button"
                            onClick={() => setShowResetConfirm(false)}
                            className="px-3 py-2 bg-gray-600 hover:bg-gray-700 text-white rounded-md text-sm font-medium"
                          >
                            Cancel
                          </button>
                        </div>
                      )}
                    </>
                  )}
                </div>
              </div>
              <p className="text-xs text-gray-500 mt-2">
                The password key must be at least 6 characters long. This key is used to encrypt/decrypt passwords 
                and is not saved in configuration files.
              </p>
              
              {/* Button Explanations */}
              {encryptedPasswords && (
                <div className="mt-4 p-3 bg-gray-50 rounded-md border border-gray-200">
                  <p className="text-xs text-gray-600 mb-2">
                    <strong>Check Password Key:</strong> Verifies if the entered key can successfully decrypt all encrypted passwords in memory without modifying the configuration.
                  </p>
                  <p className="text-xs text-gray-600">
                    <strong>Reset Encryption:</strong> Use this when the password key has been lost. This will remove all encrypted passwords and set the system to unencrypted state. You'll need to re-enter your passwords and encrypt them again with a new key.
                  </p>
                </div>
              )}
            </div>
          </div>
        </div>

        {/* Information Cards */}
        <div className="mt-8 space-y-4">
          {/* How It Works */}
          <div className="bg-blue-50 border border-blue-200 rounded-lg p-6">
            <h3 className="text-lg font-medium text-blue-900 mb-3">How Password Encryption Works</h3>
            <div className="space-y-2 text-sm text-blue-800">
              <p>• Passwords are encrypted when saved to configuration files for security</p>
              <p>• The encryption key is never stored in configuration files</p>
              <p>• When you provide the key, passwords are decrypted <em>in memory</em> for connections</p>
              <p>• The "Encrypt" and "Decrypt" buttons control how passwords are <em>stored in files</em>, not how they're used</p>
              <p>• With the correct key set, encrypted passwords work seamlessly for all operations</p>
            </div>
          </div>

          {/* Important Notes */}
          <div className="bg-yellow-50 border border-yellow-200 rounded-md p-6">
            <div className="flex">
              <div className="flex-shrink-0">
                <InformationCircleIcon className="h-5 w-5 text-yellow-400" aria-hidden="true" />
              </div>
              <div className="ml-3">
                <h3 className="text-sm font-medium text-yellow-800">Important Security Notes</h3>
                <div className="mt-2 text-sm text-yellow-700">
                  <ul className="list-disc pl-5 space-y-1">
                    <li><strong>Remember your key:</strong> If you forget the encryption key, you cannot recover the passwords</li>
                    <li><strong>Key storage:</strong> Store the encryption key securely, separate from configuration files</li>
                    <li><strong>Migration execution:</strong> The password key must be provided when running migrations with encrypted passwords</li>
                  </ul>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default PasswordEncryptionPage;