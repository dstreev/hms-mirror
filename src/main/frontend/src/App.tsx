import React from 'react';
import { Routes, Route } from 'react-router-dom';
import Layout from './components/common/Layout';
import ErrorBoundary from './components/common/ErrorBoundary';
import RequireConfiguration from './components/common/RequireConfiguration';
import RequirePasswordKey from './components/common/RequirePasswordKey';
import { ConfigurationProvider } from './contexts/ConfigurationContext';
import ConfigurationPage from './components/config/ConfigurationPage';
import ViewConfigurationsPage from './components/config/ViewConfigurationsPage';
import ReportsPage from './components/reports/ReportsPage';
import ReportDetailsPage from './components/reports/ReportDetailsPage';
import NewConfigPage from './pages/NewConfigPage';
import ExecutionPage from './components/execution/ExecutionPage';
import ConfigWizard from './components/wizards/ConfigWizard';
import PasswordEncryptionPage from './components/encryption/PasswordEncryptionPage';
import SummaryViewPage from './components/summary/SummaryViewPage';
import ConnectionsPage from './components/connections/ConnectionsPage';
import NewConnectionWizard from './components/connections/NewConnectionWizard';
import RocksDBPage from './components/rocksdb/RocksDBPage';
import DatasetsPage from './components/datasets/DatasetsPage';
import DatasetWizard from './components/datasets/DatasetWizard';
import ListDatasetsPage from './components/datasets/ListDatasetsPage';

const App: React.FC = () => {
  return (
    <ErrorBoundary>
      <ConfigurationProvider>
        <Layout>
          <Routes>
        {/* Configuration management routes - always accessible */}
        <Route path="/" element={<ConfigurationPage />} />
        <Route path="/config" element={<ConfigurationPage />} />
        <Route path="/config/manage" element={<ConfigurationPage />} />
        <Route path="/config/view" element={<ViewConfigurationsPage />} />
        <Route path="/config/new" element={<NewConfigPage />} />
        <Route path="/wizards/config" element={<ConfigWizard />} />
        <Route path="/wizards/sql-strategy" element={<ConfigWizard />} />
        
        {/* Connection management routes - always accessible */}
        <Route path="/connections" element={<ConnectionsPage />} />
        <Route path="/connections/new" element={<NewConnectionWizard />} />
        <Route path="/connections/edit/:id" element={<NewConnectionWizard />} />
        
        {/* RocksDB management routes - always accessible */}
        <Route path="/rocksdb" element={<RocksDBPage />} />
        
        {/* Dataset management routes - always accessible */}
        <Route path="/datasets" element={<DatasetsPage />} />
        <Route path="/datasets/new" element={<DatasetWizard />} />
        <Route path="/datasets/list" element={<ListDatasetsPage />} />
        <Route path="/datasets/edit" element={<DatasetWizard />} />
        
        
        {/* Protected feature routes */}
        <Route path="/encryption" element={<RequireConfiguration><PasswordEncryptionPage /></RequireConfiguration>} />
        <Route path="/execution" element={<RequireConfiguration><RequirePasswordKey><ExecutionPage /></RequirePasswordKey></RequireConfiguration>} />
        <Route path="/summary" element={<RequireConfiguration><SummaryViewPage /></RequireConfiguration>} />
        
        {/* Reports routes - always accessible */}
        <Route path="/reports" element={<ReportsPage />} />
        <Route path="/reports/details" element={<ReportDetailsPage />} />
          </Routes>
        </Layout>
      </ConfigurationProvider>
    </ErrorBoundary>
  );
};

export default App;