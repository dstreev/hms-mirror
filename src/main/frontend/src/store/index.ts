import { configureStore } from '@reduxjs/toolkit';
import configSlice from './slices/configSlice';
import runtimeSlice from './slices/runtimeSlice';
import uiSlice from './slices/uiSlice';

export const store = configureStore({
  reducer: {
    config: configSlice,
    runtime: runtimeSlice,
    ui: uiSlice,
  },
  middleware: (getDefaultMiddleware) =>
    getDefaultMiddleware({
      serializableCheck: {
        ignoredActions: ['persist/PERSIST'],
      },
    }),
});

export type RootState = ReturnType<typeof store.getState>;
export type AppDispatch = typeof store.dispatch;