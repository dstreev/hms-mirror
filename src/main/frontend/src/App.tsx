import React from 'react';
import { Routes, Route } from 'react-router-dom';
import Layout from './components/common/Layout';
import ErrorBoundary from './components/common/ErrorBoundary';
import RequireConfiguration from './components/common/RequireConfiguration';
import RequirePasswordKey from './components/common/RequirePasswordKey';
import { ConfigurationProvider } from './contexts/ConfigurationContext';
import ConfigurationPage from './components/config/ConfigurationPage';
import LoadConfigurationPage from './components/config/LoadConfigurationPage';
import TemplatesPage from './components/config/TemplatesPage';
import CurrentConfigurationPage from './components/config/CurrentConfigurationPage';
import ClusterConfigurationWrapper from './components/config/sections/ClusterConfigurationWrapper';
import DatabaseSelectionWrapper from './components/config/sections/DatabaseSelectionWrapper';
import MigrationSettingsWrapper from './components/config/sections/MigrationSettingsWrapper';
import AdvancedOptionsWrapper from './components/config/sections/AdvancedOptionsWrapper';
import DatabaseFiltersWrapper from './components/config/sections/DatabaseFiltersWrapper';
import ReportsPage from './components/reports/ReportsPage';
import ReportDetailsPage from './components/reports/ReportDetailsPage';
import NewConfigPage from './pages/NewConfigPage';
import ExecutionPage from './components/execution/ExecutionPage';
import PasswordEncryptionPage from './components/encryption/PasswordEncryptionPage';
import SummaryViewPage from './components/summary/SummaryViewPage';

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
        <Route path="/config/new" element={<NewConfigPage />} />
        <Route path="/config/load" element={<LoadConfigurationPage />} />
        <Route path="/config/templates" element={<TemplatesPage />} />
        
        {/* Protected configuration editing routes */}
        <Route path="/config/current" element={<RequireConfiguration><CurrentConfigurationPage /></RequireConfiguration>} />
        <Route path="/config/edit/:id" element={<RequireConfiguration><CurrentConfigurationPage /></RequireConfiguration>} />
        <Route path="/config/edit/:id/clusters" element={<RequireConfiguration><ClusterConfigurationWrapper /></RequireConfiguration>} />
        <Route path="/config/edit/:id/databases" element={<RequireConfiguration><DatabaseSelectionWrapper /></RequireConfiguration>} />
        <Route path="/config/edit/:id/migration" element={<RequireConfiguration><MigrationSettingsWrapper /></RequireConfiguration>} />
        <Route path="/config/edit/:id/advanced" element={<RequireConfiguration><AdvancedOptionsWrapper /></RequireConfiguration>} />
        <Route path="/config/edit/current/clusters" element={<RequireConfiguration><ClusterConfigurationWrapper /></RequireConfiguration>} />
        <Route path="/config/edit/current/databases" element={<RequireConfiguration><DatabaseSelectionWrapper /></RequireConfiguration>} />
        <Route path="/config/edit/current/migration" element={<RequireConfiguration><MigrationSettingsWrapper /></RequireConfiguration>} />
        <Route path="/config/edit/current/advanced" element={<RequireConfiguration><AdvancedOptionsWrapper /></RequireConfiguration>} />
        <Route path="/config/edit/:id/filters" element={<RequireConfiguration><DatabaseFiltersWrapper /></RequireConfiguration>} />
        <Route path="/config/edit/current/filters" element={<RequireConfiguration><DatabaseFiltersWrapper /></RequireConfiguration>} />
        
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