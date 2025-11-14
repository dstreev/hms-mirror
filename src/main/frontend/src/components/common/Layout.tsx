import React from 'react';
import { Link, useLocation } from 'react-router-dom';
import {
  HomeIcon,
  CogIcon,
  CircleStackIcon,
  PlayIcon,
  DocumentTextIcon,
  WifiIcon,
  KeyIcon,
  CodeBracketIcon,
  LockClosedIcon,
  LinkIcon,
  TableCellsIcon,
  ChevronRightIcon,
  ChevronDownIcon,
  WrenchScrewdriverIcon,
  FolderArrowDownIcon
} from '@heroicons/react/24/outline';
import { useConfiguration } from '../../contexts/ConfigurationContext';
import SessionInfo from './SessionInfo';

interface LayoutProps {
  children: React.ReactNode;
}

const Layout: React.FC<LayoutProps> = ({ children }) => {
  const location = useLocation();
  const { state } = useConfiguration();
  const hasConfig = state.isConfigurationLoaded && state.config;
  const hasEncryptedPasswordsWithoutKey = state.config?.encryptedPasswords && !state.config?.passwordKey;
  const [settingsExpanded, setSettingsExpanded] = React.useState(true);

  const [runtimeExpanded, setRuntimeExpanded] = React.useState(true);
  const [utilitiesExpanded, setUtilitiesExpanded] = React.useState(true);

  const navigation = [
    {
      name: 'Settings',
      icon: CogIcon,
      isGroup: true,
      expanded: settingsExpanded,
      onToggle: () => setSettingsExpanded(!settingsExpanded),
      children: [
        { name: 'Connections', href: '/connections', icon: LinkIcon, requiresConfig: false, requiresPasswordKey: false },
        { name: 'Datasets', href: '/datasets', icon: TableCellsIcon, requiresConfig: false, requiresPasswordKey: false },
        { name: 'Configurations', href: '/config/manage', icon: HomeIcon, requiresConfig: false, requiresPasswordKey: false },
      ]
    },
    {
      name: 'Runtime',
      icon: PlayIcon,
      isGroup: true,
      expanded: runtimeExpanded,
      onToggle: () => setRuntimeExpanded(!runtimeExpanded),
      children: [
        { name: 'Define', href: '/jobs/build', icon: CogIcon, requiresConfig: false, requiresPasswordKey: false },
        { name: 'Jobs', href: '/jobs/execute', icon: PlayIcon, requiresConfig: false, requiresPasswordKey: false },
        { name: 'Reports', href: '/reports', icon: DocumentTextIcon, requiresConfig: false, requiresPasswordKey: false },
      ]
    },
    {
      name: 'Utilities',
      icon: WrenchScrewdriverIcon,
      isGroup: true,
      expanded: utilitiesExpanded,
      onToggle: () => setUtilitiesExpanded(!utilitiesExpanded),
      children: [
        { name: 'RocksDB', href: '/rocksdb', icon: CircleStackIcon, requiresConfig: false, requiresPasswordKey: false },
        { name: 'CLI Reports', href: '/utilities/cli-reports', icon: FolderArrowDownIcon, requiresConfig: false, requiresPasswordKey: false },
      ]
    },
  ];

  return (
    <div className="min-h-screen bg-gray-50 flex">
      {/* Sidebar */}
      <div className="w-64 bg-white shadow-lg relative">
        <Link to="/" className="block p-6 hover:bg-gray-50 transition-colors">
          <h1 className="text-xl font-bold text-gray-900">HMS-Mirror</h1>
          <p className="text-sm text-gray-600">Migration Tool</p>
        </Link>
        <nav className="mt-6 pb-24">
          {navigation.map((item) => {
            if (item.isGroup) {
              return (
                <div key={item.name}>
                  <button
                    onClick={item.onToggle}
                    className="flex items-center w-full px-6 py-3 text-sm font-medium text-gray-600 hover:bg-gray-50 hover:text-gray-900 transition-colors"
                  >
                    <item.icon className="w-5 h-5 mr-3" />
                    <span className="flex-1 text-left">{item.name}</span>
                    {item.expanded ? (
                      <ChevronDownIcon className="w-4 h-4" />
                    ) : (
                      <ChevronRightIcon className="w-4 h-4" />
                    )}
                  </button>
                  {item.expanded && item.children && (
                    <div className="bg-gray-50">
                      {item.children.map((childItem) => {
                        // Special handling for Configurations route
                        const isActive =
                          location.pathname === childItem.href ||
                          location.pathname.startsWith(childItem.href + '/') ||
                          (childItem.href === '/config/manage' && (
                            location.pathname === '/config' ||
                            location.pathname === '/config/new'
                          ));
                        const isDisabledNoConfig = childItem.requiresConfig && !hasConfig;
                        const isDisabledNoKey = childItem.requiresPasswordKey && hasEncryptedPasswordsWithoutKey;
                        const isDisabled = isDisabledNoConfig || isDisabledNoKey;
                        
                        if (isDisabled) {
                          const tooltipMessage = isDisabledNoConfig 
                            ? "Please load or create a configuration first"
                            : "Password key required - passwords are encrypted";
                          
                          return (
                            <div
                              key={childItem.name}
                              className="flex items-center pl-12 pr-6 py-2 text-sm font-medium text-gray-400 cursor-not-allowed relative"
                              title={tooltipMessage}
                            >
                              <childItem.icon className="w-4 h-4 mr-3 opacity-50" />
                              <span className="opacity-50">{childItem.name}</span>
                              <LockClosedIcon className="w-3 h-3 ml-auto opacity-40" />
                            </div>
                          );
                        }
                        
                        return (
                          <Link
                            key={childItem.name}
                            to={childItem.href}
                            className={`
                              flex items-center pl-12 pr-6 py-2 text-sm font-medium transition-colors
                              ${isActive 
                                ? 'bg-blue-100 text-blue-700 border-r-2 border-blue-700' 
                                : 'text-gray-600 hover:bg-gray-100 hover:text-gray-900'
                              }
                            `}
                          >
                            <childItem.icon className="w-4 h-4 mr-3" />
                            {childItem.name}
                          </Link>
                        );
                      })}
                    </div>
                  )}
                </div>
              );
            }
            
            // Regular navigation items
            const isActive = 
              location.pathname === item.href || 
              location.pathname.startsWith(item.href + '/') ||
              (item.href === '/config/current' && (
                location.pathname.startsWith('/config/edit/')
              ));
            const isDisabledNoConfig = item.requiresConfig && !hasConfig;
            const isDisabledNoKey = item.requiresPasswordKey && hasEncryptedPasswordsWithoutKey;
            const isDisabled = isDisabledNoConfig || isDisabledNoKey;
            
            if (isDisabled) {
              const tooltipMessage = isDisabledNoConfig 
                ? "Please load or create a configuration first"
                : "Password key required - passwords are encrypted";
              
              return (
                <div
                  key={item.name}
                  className="flex items-center px-6 py-3 text-sm font-medium text-gray-400 cursor-not-allowed relative"
                  title={tooltipMessage}
                >
                  <item.icon className="w-5 h-5 mr-3 opacity-50" />
                  <span className="opacity-50">{item.name}</span>
                  <LockClosedIcon className="w-3 h-3 ml-auto opacity-40" />
                </div>
              );
            }
            
            return (
              <Link
                key={item.name}
                to={item.href}
                className={`
                  flex items-center px-6 py-3 text-sm font-medium transition-colors
                  ${isActive 
                    ? 'bg-blue-50 text-blue-700 border-r-2 border-blue-700' 
                    : 'text-gray-600 hover:bg-gray-50 hover:text-gray-900'
                  }
                `}
              >
                <item.icon className="w-5 h-5 mr-3" />
                {item.name}
              </Link>
            );
          })}
        </nav>
        
        {/* Session info at bottom of sidebar */}
        <div className="absolute bottom-0 left-0 right-0 p-4 border-t border-gray-200 bg-gray-50">
          <SessionInfo showStatus={true} className="text-center" />
        </div>
      </div>

      {/* Main content */}
      <div className="flex-1">
        {children}
      </div>
    </div>
  );
};

export default Layout;