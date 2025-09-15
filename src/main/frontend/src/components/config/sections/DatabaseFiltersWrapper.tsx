import React from 'react';
import ConfigurationSectionWrapper from '../ConfigurationSectionWrapper';
import DatabaseFiltersSection from './DatabaseFiltersSection';
import ErrorBoundary from '../../common/ErrorBoundary';

const DatabaseFiltersWrapper: React.FC = () => {
  return (
    <ConfigurationSectionWrapper>
      <ErrorBoundary fallbackMessage="An error occurred while loading the database filters configuration.">
        <DatabaseFiltersSection />
      </ErrorBoundary>
    </ConfigurationSectionWrapper>
  );
};

export default DatabaseFiltersWrapper;