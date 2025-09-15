import { createSlice, PayloadAction } from '@reduxjs/toolkit';

interface UiState {
  sidebarOpen: boolean;
  expandedCards: Record<string, boolean>;
  notifications: Notification[];
  loading: Record<string, boolean>;
}

interface Notification {
  id: string;
  type: 'success' | 'error' | 'warning' | 'info';
  title: string;
  message: string;
  timestamp: number;
  autoClose?: boolean;
}

const initialState: UiState = {
  sidebarOpen: false,
  expandedCards: {},
  notifications: [],
  loading: {},
};

const uiSlice = createSlice({
  name: 'ui',
  initialState,
  reducers: {
    toggleSidebar: (state) => {
      state.sidebarOpen = !state.sidebarOpen;
    },
    setSidebarOpen: (state, action: PayloadAction<boolean>) => {
      state.sidebarOpen = action.payload;
    },
    toggleCardExpanded: (state, action: PayloadAction<string>) => {
      const cardId = action.payload;
      state.expandedCards[cardId] = !state.expandedCards[cardId];
    },
    setCardExpanded: (state, action: PayloadAction<{ cardId: string; expanded: boolean }>) => {
      const { cardId, expanded } = action.payload;
      state.expandedCards[cardId] = expanded;
    },
    addNotification: (state, action: PayloadAction<Omit<Notification, 'id' | 'timestamp'>>) => {
      const notification: Notification = {
        ...action.payload,
        id: Date.now().toString(),
        timestamp: Date.now(),
      };
      state.notifications.push(notification);
    },
    removeNotification: (state, action: PayloadAction<string>) => {
      state.notifications = state.notifications.filter(n => n.id !== action.payload);
    },
    clearNotifications: (state) => {
      state.notifications = [];
    },
    setLoading: (state, action: PayloadAction<{ key: string; loading: boolean }>) => {
      const { key, loading } = action.payload;
      state.loading[key] = loading;
    },
    clearLoading: (state, action: PayloadAction<string>) => {
      delete state.loading[action.payload];
    },
  },
});

export const {
  toggleSidebar,
  setSidebarOpen,
  toggleCardExpanded,
  setCardExpanded,
  addNotification,
  removeNotification,
  clearNotifications,
  setLoading,
  clearLoading,
} = uiSlice.actions;

export default uiSlice.reducer;