import React from 'react';
import ConfigurationSectionWrapper from '../ConfigurationSectionWrapper';
import DatabaseSelectionSection from './DatabaseSelectionSection';

const DatabaseSelectionWrapper: React.FC = () => {
  return (
    <ConfigurationSectionWrapper sectionName="Database Selection">
      <DatabaseSelectionSection />
    </ConfigurationSectionWrapper>
  );
};

export default DatabaseSelectionWrapper;