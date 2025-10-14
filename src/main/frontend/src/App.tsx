import React from 'react';
import { Routes, Route } from 'react-router-dom';
import Layout from './components/common/Layout';
import ErrorBoundary from './components/common/ErrorBoundary';
import RequireConfiguration from './components/common/RequireConfiguration';
import RequirePasswordKey from './components/common/RequirePasswordKey';
import { ConfigurationProvider } from './contexts/ConfigurationContext';
import HomePage from './pages/HomePage';
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
import DatasetWizard from './components/datasets/DatasetWizard';
import ListDatasetsPage from './components/datasets/ListDatasetsPage';
import JobsBuildPage from './components/jobs/JobsBuildPage';
import JobBuildWizard from './components/jobs/JobBuildWizard';

const App: React.FC = () => {
  return (
    <ErrorBoundary>
      <ConfigurationProvider>
        <Layout>
          <Routes>
        {/* Home page */}
        <Route path="/" element={<HomePage />} />

        {/* Configuration management routes - always accessible */}
        <Route path="/config" element={<ViewConfigurationsPage />} />
        <Route path="/config/manage" element={<ViewConfigurationsPage />} />
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
        <Route path="/datasets" element={<ListDatasetsPage />} />
        <Route path="/datasets/new" element={<DatasetWizard />} />
        <Route path="/datasets/edit" element={<DatasetWizard />} />
        <Route path="/datasets/import" element={<div>Import dataset page placeholder</div>} />
        
        {/* Jobs management routes - always accessible */}
        <Route path="/jobs/build" element={<JobsBuildPage />} />
        <Route path="/jobs/list" element={<JobsBuildPage />} />
        <Route path="/jobs/execute" element={<div>Execute page placeholder</div>} />
        <Route path="/jobs/build/wizard" element={<JobBuildWizard />} />
        
        
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