import React from 'react';
import { Link } from 'react-router-dom';
import { useConfiguration } from '../../contexts/ConfigurationContext';
import SessionInfo from './SessionInfo';

const Header: React.FC = () => {
  const { state } = useConfiguration();
  
  // Intelligent navigation: go to current config if loaded, otherwise to config management
  const configurationPath = state.isConfigurationLoaded ? '/config/current' : '/config';
  
  return (
    <header className="bg-white shadow-sm border-b border-gray-200">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex justify-between items-center h-16">
          <div className="flex items-center">
            <Link to="/" className="flex items-center space-x-2">
              <div className="w-8 h-8 bg-blue-600 rounded-md flex items-center justify-center">
                <span className="text-white font-bold text-sm">HM</span>
              </div>
              <span className="text-xl font-bold text-gray-900">HMS-Mirror</span>
            </Link>
          </div>
          
          <nav className="flex items-center space-x-4">
            <Link 
              to={configurationPath}
              className="text-gray-700 hover:text-blue-600 px-3 py-2 rounded-md text-sm font-medium transition-colors"
            >
              Configuration
            </Link>
            <Link 
              to="/config"
              className="text-gray-700 hover:text-blue-600 px-3 py-2 rounded-md text-sm font-medium transition-colors"
            >
              Create/Load
            </Link>
            <a 
              href="https://dstreev.github.io/hms-mirror" 
              target="_blank" 
              rel="noopener noreferrer"
              className="text-gray-700 hover:text-blue-600 px-3 py-2 rounded-md text-sm font-medium transition-colors"
            >
              Documentation
            </a>
            <div className="flex items-center pl-4 border-l border-gray-200">
              <SessionInfo compact={true} className="opacity-70" />
            </div>
          </nav>
        </div>
      </div>
    </header>
  );
};

export default Header;