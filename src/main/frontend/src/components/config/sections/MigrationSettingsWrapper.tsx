import React from 'react';
import ConfigurationSectionWrapper from '../ConfigurationSectionWrapper';
import MigrationSettingsSection from './MigrationSettingsSection';

const MigrationSettingsWrapper: React.FC = () => {
  return (
    <ConfigurationSectionWrapper sectionName="Migration Settings">
      <MigrationSettingsSection />
    </ConfigurationSectionWrapper>
  );
};

export default MigrationSettingsWrapper;