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
  LinkIcon
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

  const navigation = [
    { name: 'Create/Load', href: '/config/manage', icon: HomeIcon, requiresConfig: false, requiresPasswordKey: false },
    { name: 'Connections', href: '/connections', icon: LinkIcon, requiresConfig: false, requiresPasswordKey: false },
    { name: 'RocksDB', href: '/rocksdb', icon: CircleStackIcon, requiresConfig: false, requiresPasswordKey: false },
    { name: 'Configuration', href: '/config/current', icon: CogIcon, requiresConfig: true, requiresPasswordKey: false },
    { name: 'Password Encryption', href: '/encryption', icon: KeyIcon, requiresConfig: true, requiresPasswordKey: false },
    { name: 'Execution', href: '/execution', icon: PlayIcon, requiresConfig: true, requiresPasswordKey: true },
    { name: 'Summary View', href: '/summary', icon: CodeBracketIcon, requiresConfig: true, requiresPasswordKey: false },
    { name: 'Reports', href: '/reports', icon: DocumentTextIcon, requiresConfig: false, requiresPasswordKey: false },
  ];

  return (
    <div className="min-h-screen bg-gray-50 flex">
      {/* Sidebar */}
      <div className="w-64 bg-white shadow-lg relative">
        <div className="p-6">
          <h1 className="text-xl font-bold text-gray-900">HMS-Mirror</h1>
          <p className="text-sm text-gray-600">Migration Tool</p>
        </div>
        <nav className="mt-6 pb-24">
          {navigation.map((item) => {
            // Special handling for Create/Load and Configuration routes
            const isActive = 
              location.pathname === item.href || 
              location.pathname.startsWith(item.href + '/') ||
              (item.href === '/config/manage' && (
                location.pathname === '/' ||
                location.pathname === '/config' ||
                location.pathname === '/config/new' ||
                location.pathname === '/config/load' ||
                location.pathname === '/config/templates'
              )) ||
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