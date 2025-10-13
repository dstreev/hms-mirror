import React, { createContext, useContext, useReducer, useEffect, ReactNode } from 'react';

export interface ConfigurationState {
  // Full HmsMirrorConfig data
  config: any | null; // Full HmsMirrorConfig object from backend
  
  // Metadata about the configuration
  configName: string;
  filename: string | null; // null for new configs
  isNew: boolean; // true for new/template configs
  hasUnsavedChanges: boolean;
  lastSavedState: any; // snapshot of config when last saved to file
  isConfigurationLoaded: boolean; // true when user has explicitly loaded/created a config
  pendingDataStrategy?: string; // data strategy to apply after loading config
  skipNextAutoSave?: boolean; // Flag to skip the next auto-save (e.g., after upload/load)
  
  // UI state
  isLoading: boolean;
  error: string | null;
}

export type ConfigurationAction =
  | { type: 'LOAD_CONFIG'; payload: { config: any; filename?: string } }
  | { type: 'CREATE_NEW_CONFIG'; payload?: { dataStrategy?: string } }
  | { type: 'UPDATE_CONFIG'; payload: any } // Update entire config or part of it
  | { type: 'UPDATE_CONFIG_NAME'; payload: string }
  | { type: 'MARK_SAVED'; payload?: { filename?: string } }
  | { type: 'SET_LOADING'; payload: boolean }
  | { type: 'SET_ERROR'; payload: string | null }
  | { type: 'RESET_CONFIG' };

// Start with a clean state - no automatic configuration loading
const getInitialState = (): ConfigurationState => {
  // Always start fresh - users must explicitly create or load a configuration
  return {
    config: null,
    configName: 'New Config',
    filename: null,
    isNew: true,
    hasUnsavedChanges: false,
    lastSavedState: null,
    isConfigurationLoaded: false,
    isLoading: false,
    error: null,
  };
};

const initialState = getInitialState();

function configurationReducer(state: ConfigurationState, action: ConfigurationAction): ConfigurationState {
  switch (action.type) {
    case 'LOAD_CONFIG': {
      let config = action.payload.config;
      
      // Apply pending data strategy if we have one (for new configs)
      if (state.pendingDataStrategy) {
        config = { ...config, dataStrategy: state.pendingDataStrategy };
      }
      
      const newState = {
        ...state,
        config: config,
        filename: action.payload.filename || null,
        isNew: !action.payload.filename,
        hasUnsavedChanges: !!state.pendingDataStrategy, // Mark as unsaved if we applied a data strategy
        lastSavedState: action.payload.config, // Keep original as last saved state
        isConfigurationLoaded: true,
        configName: action.payload.filename 
          ? action.payload.filename.replace(/\.(yaml|yml)$/, '')
          : 'New Config',
        error: null,
        pendingDataStrategy: undefined, // Clear pending strategy
        skipNextAutoSave: true, // Skip auto-save after loading config
      };
      
      // Note: If we applied a data strategy, it will be handled in loadConfiguration function
      
      return newState;
    }

    case 'CREATE_NEW_CONFIG': {
      // This will be followed by loadConfiguration() which will load the actual config
      return {
        ...initialState,
        isConfigurationLoaded: true,
        hasUnsavedChanges: true,
        pendingDataStrategy: action.payload?.dataStrategy || 'SCHEMA_ONLY',
      };
    }

    case 'UPDATE_CONFIG': {
      const newState = {
        ...state,
        config: { ...state.config, ...action.payload },
        hasUnsavedChanges: true,
        error: null,
        skipNextAutoSave: false, // Clear the flag after any update
      };
      return newState;
    }

    case 'UPDATE_CONFIG_NAME': {
      return {
        ...state,
        configName: action.payload,
        hasUnsavedChanges: true,
        error: null,
      };
    }

    case 'MARK_SAVED': {
      const filename = action.payload?.filename || state.filename;
      return {
        ...state,
        filename: filename,
        isNew: false,
        hasUnsavedChanges: false,
        lastSavedState: state.config,
        configName: filename 
          ? filename.replace(/\.(yaml|yml)$/, '') 
          : state.configName,
      };
    }

    case 'SET_LOADING': {
      return {
        ...state,
        isLoading: action.payload,
      };
    }

    case 'SET_ERROR': {
      return {
        ...state,
        error: action.payload,
        isLoading: false,
      };
    }

    case 'RESET_CONFIG': {
      return initialState;
    }

    default:
      return state;
  }
}

// Auto-save to backend (in-memory) and return the updated config
async function autoSaveToBackend(state: ConfigurationState, dispatch: React.Dispatch<ConfigurationAction>) {
  try {
    if (state.config) {
      // TODO: Replace with new configuration storage API
      // Legacy /api/v1/config endpoint has been removed
      console.warn('Auto-save disabled: legacy API endpoint /api/v1/config has been removed');
      return;
    }
  } catch (error) {
    console.error('Auto-save to backend failed:', error);
    // Don't throw - auto-save failures shouldn't break the UI
  }
}

interface ConfigurationContextType {
  state: ConfigurationState;
  dispatch: React.Dispatch<ConfigurationAction>;
  
  // Helper functions
  loadConfiguration: (filename?: string) => Promise<void>;
  loadConfigurationAsTemplate: (filename?: string) => Promise<void>;
  saveToFile: (filename?: string) => Promise<boolean>;
  updateConfig: (configUpdate: any) => void;
  canSave: () => boolean;
  getSaveStatus: () => 'new' | 'saved' | 'unsaved';
}

const ConfigurationContext = createContext<ConfigurationContextType | undefined>(undefined);

interface ConfigurationProviderProps {
  children: ReactNode;
}

export function ConfigurationProvider({ children }: ConfigurationProviderProps) {
  const [state, dispatch] = useReducer(configurationReducer, initialState);

  // DO NOT persist state across page refreshes
  // Users must explicitly create or load a configuration after each session

  // DO NOT automatically load configuration on startup
  // Users must explicitly create or load a configuration

  // Load configuration from backend
  const loadConfiguration = async (filename?: string) => {
    dispatch({ type: 'SET_LOADING', payload: true });
    
    try {
      if (filename) {
        // TODO: Replace with new configuration loading API
        console.warn('File loading disabled: legacy API endpoint /api/v1/config/load has been removed');
        throw new Error('Configuration file loading is currently unavailable due to API migration');
      }
      
      // TODO: Replace with new configuration loading API
      console.warn('Configuration loading disabled: legacy API endpoint /api/v1/config has been removed');
      throw new Error('Configuration loading is currently unavailable due to API migration');
    } catch (error) {
      dispatch({ 
        type: 'SET_ERROR', 
        payload: error instanceof Error ? error.message : 'Failed to load configuration'
      });
    } finally {
      dispatch({ type: 'SET_LOADING', payload: false });
    }
  };

  // Save configuration to file
  const saveToFile = async (filename?: string): Promise<boolean> => {
    try {
      let saveFilename = filename;
      
      // Prompt for filename if this is a new config and no filename provided
      if (state.isNew && !saveFilename) {
        saveFilename = prompt('Enter a name for this configuration:');
        if (!saveFilename?.trim()) {
          return false;
        }
        saveFilename = saveFilename.trim();
      } else if (!saveFilename) {
        saveFilename = state.filename || undefined;
      }

      if (!saveFilename) {
        throw new Error('No filename provided');
      }

      // TODO: Replace with new configuration persistence API
      console.warn('File saving disabled: legacy API endpoint /api/v1/config/persist has been removed');
      throw new Error('Configuration file saving is currently unavailable due to API migration');
    } catch (error) {
      dispatch({ 
        type: 'SET_ERROR', 
        payload: error instanceof Error ? error.message : 'Failed to save configuration'
      });
      return false;
    }
  };

  // Update config with auto-save
  const updateConfig = (configUpdate: any) => {
    dispatch({ type: 'UPDATE_CONFIG', payload: configUpdate });
    // Auto-save to backend after state update (unless flag is set)
    if (!state.skipNextAutoSave) {
      const newState = {
        ...state,
        config: { ...state.config, ...configUpdate },
        hasUnsavedChanges: true,
        error: null,
      };
      autoSaveToBackend(newState, dispatch);
    }
  };

  // Load configuration as a template (for creating new config based on existing)
  const loadConfigurationAsTemplate = async (filename?: string) => {
    dispatch({ type: 'SET_LOADING', payload: true });
    dispatch({ type: 'SET_ERROR', payload: null });

    try {
      // TODO: Replace with new configuration template loading API
      console.warn('Template loading disabled: legacy API endpoint /api/v1/config/load has been removed');
      throw new Error('Configuration template loading is currently unavailable due to API migration');
    } catch (error) {
      dispatch({ 
        type: 'SET_ERROR', 
        payload: error instanceof Error ? error.message : 'Failed to load template'
      });
    } finally {
      dispatch({ type: 'SET_LOADING', payload: false });
    }
  };

  // Check if configuration can be saved
  const canSave = (): boolean => {
    return state.hasUnsavedChanges || state.isNew;
  };

  // Get current save status
  const getSaveStatus = (): 'new' | 'saved' | 'unsaved' => {
    if (state.isNew && state.hasUnsavedChanges) return 'new';
    if (state.hasUnsavedChanges) return 'unsaved';
    return 'saved';
  };

  const contextValue: ConfigurationContextType = {
    state,
    dispatch,
    loadConfiguration,
    loadConfigurationAsTemplate,
    saveToFile,
    updateConfig,
    canSave,
    getSaveStatus,
  };

  return (
    <ConfigurationContext.Provider value={contextValue}>
      {children}
    </ConfigurationContext.Provider>
  );
}

export function useConfiguration() {
  const context = useContext(ConfigurationContext);
  if (context === undefined) {
    throw new Error('useConfiguration must be used within a ConfigurationProvider');
  }
  return context;
}