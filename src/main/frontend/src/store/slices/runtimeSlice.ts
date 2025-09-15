import { createSlice, PayloadAction } from '@reduxjs/toolkit';
import { ExecutionStatus, ConnectionStatus } from '../../types/api';

interface RuntimeState {
  execution: ExecutionStatus;
  connections: {
    LEFT: ConnectionStatus;
    RIGHT: ConnectionStatus;
  };
  isConnecting: boolean;
}

const initialState: RuntimeState = {
  execution: {
    status: 'IDLE',
    progress: 0,
  },
  connections: {
    LEFT: {
      endpoint: '',
      status: 'DISCONNECTED',
    },
    RIGHT: {
      endpoint: '',
      status: 'DISCONNECTED',
    },
  },
  isConnecting: false,
};

const runtimeSlice = createSlice({
  name: 'runtime',
  initialState,
  reducers: {
    setExecutionStatus: (state, action: PayloadAction<ExecutionStatus>) => {
      state.execution = action.payload;
    },
    updateExecutionProgress: (state, action: PayloadAction<number>) => {
      state.execution.progress = action.payload;
    },
    setConnectionStatus: (state, action: PayloadAction<{ cluster: 'LEFT' | 'RIGHT'; status: ConnectionStatus }>) => {
      const { cluster, status } = action.payload;
      state.connections[cluster] = status;
    },
    setConnecting: (state, action: PayloadAction<boolean>) => {
      state.isConnecting = action.payload;
    },
    resetExecution: (state) => {
      state.execution = {
        status: 'IDLE',
        progress: 0,
      };
    },
  },
});

export const {
  setExecutionStatus,
  updateExecutionProgress,
  setConnectionStatus,
  setConnecting,
  resetExecution,
} = runtimeSlice.actions;

export default runtimeSlice.reducer;