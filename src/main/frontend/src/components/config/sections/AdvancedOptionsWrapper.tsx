import React from 'react';
import ConfigurationSectionWrapper from '../ConfigurationSectionWrapper';
import AdvancedOptionsSection from './AdvancedOptionsSection';

const AdvancedOptionsWrapper: React.FC = () => {
  return (
    <ConfigurationSectionWrapper sectionName="Advanced Options">
      <AdvancedOptionsSection />
    </ConfigurationSectionWrapper>
  );
};

export default AdvancedOptionsWrapper;