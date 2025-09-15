import React from 'react';
import ConfigurationSectionWrapper from '../ConfigurationSectionWrapper';
import ClusterConfigurationSection from './ClusterConfigurationSection';

const ClusterConfigurationWrapper: React.FC = () => {
  return (
    <ConfigurationSectionWrapper sectionName="Cluster">
      <ClusterConfigurationSection />
    </ConfigurationSectionWrapper>
  );
};

export default ClusterConfigurationWrapper;