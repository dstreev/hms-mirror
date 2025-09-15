import { createSlice, PayloadAction } from '@reduxjs/toolkit';
import { HmsMirrorConfig, ConfigValidationResult } from '../../types/api';

interface ConfigState {
  current: HmsMirrorConfig | null;
  isLoaded: boolean;
  isLoading: boolean;
  isSaving: boolean;
  validation: ConfigValidationResult | null;
  error: string | null;
}

const initialState: ConfigState = {
  current: null,
  isLoaded: false,
  isLoading: false,
  isSaving: false,
  validation: null,
  error: null,
};

const configSlice = createSlice({
  name: 'config',
  initialState,
  reducers: {
    setLoading: (state, action: PayloadAction<boolean>) => {
      state.isLoading = action.payload;
    },
    setSaving: (state, action: PayloadAction<boolean>) => {
      state.isSaving = action.payload;
    },
    setConfig: (state, action: PayloadAction<HmsMirrorConfig>) => {
      state.current = action.payload;
      state.isLoaded = true;
      state.error = null;
    },
    clearConfig: (state) => {
      state.current = null;
      state.isLoaded = false;
      state.validation = null;
      state.error = null;
    },
    setValidation: (state, action: PayloadAction<ConfigValidationResult>) => {
      state.validation = action.payload;
    },
    setError: (state, action: PayloadAction<string>) => {
      state.error = action.payload;
      state.isLoading = false;
      state.isSaving = false;
    },
    updateConfigField: (state, action: PayloadAction<{ path: string; value: any }>) => {
      if (state.current) {
        const { path, value } = action.payload;
        const keys = path.split('.');
        let target: any = state.current;
        
        for (let i = 0; i < keys.length - 1; i++) {
          if (!(keys[i] in target)) {
            target[keys[i]] = {};
          }
          target = target[keys[i]];
        }
        
        target[keys[keys.length - 1]] = value;
        
        // Clear validation when config changes
        state.validation = null;
      }
    },
  },
});

export const {
  setLoading,
  setSaving,
  setConfig,
  clearConfig,
  setValidation,
  setError,
  updateConfigField,
} = configSlice.actions;

export default configSlice.reducer;