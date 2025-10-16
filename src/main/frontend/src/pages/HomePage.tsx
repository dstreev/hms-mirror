import React, { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  CogIcon,
  CircleStackIcon,
  DocumentTextIcon,
  RocketLaunchIcon,
  ArrowRightIcon,
  CheckCircleIcon
} from '@heroicons/react/24/outline';

const HomePage: React.FC = () => {
  const navigate = useNavigate();
  const [version, setVersion] = useState<string>('');

  useEffect(() => {
    // Fetch application version from the API
    fetch('/hms-mirror/api/v1/app/version')
      .then(response => response.json())
      .then(data => {
        if (data.version) {
          setVersion(data.version);
        }
      })
      .catch(error => {
        console.error('Failed to fetch application version:', error);
        setVersion('4.0.0.0'); // Fallback to default version
      });
  }, []);

  return (
    <div className="min-h-screen bg-gradient-to-b from-blue-50 to-white">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-16">
        {/* Hero Section */}
        <div className="text-center mb-16">
          <h1 className="text-5xl font-bold text-gray-900 mb-4">
            Welcome to HMS-Mirror
          </h1>
          <p className="text-xl text-gray-600 max-w-3xl mx-auto">
            A powerful Hive Metastore migration utility that helps transfer data and metadata
            between different Hive environments with ease and reliability.
          </p>
        </div>

        {/* What is HMS-Mirror Section */}
        <div className="mb-16 bg-white rounded-lg shadow-lg p-8">
          <h2 className="text-3xl font-bold text-gray-900 mb-6 flex items-center">
            <CircleStackIcon className="h-8 w-8 mr-3 text-blue-600" />
            What is HMS-Mirror?
          </h2>
          <div className="text-gray-700 space-y-4">
            <p className="text-lg">
              HMS-Mirror is a specialized tool designed to simplify and automate the process of migrating
              Hive metastore data between different compute clusters. Whether you're upgrading to a new
              platform, consolidating environments, or migrating to the cloud, HMS-Mirror provides the
              flexibility and control you need.
            </p>
          </div>
        </div>

        {/* Key Features */}
        <div className="mb-16">
          <h2 className="text-3xl font-bold text-gray-900 mb-8 text-center">Key Features</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            <div className="bg-white rounded-lg shadow-md p-6">
              <div className="flex items-center mb-4">
                <CheckCircleIcon className="h-6 w-6 text-green-600 mr-2" />
                <h3 className="text-lg font-semibold">Multiple Migration Strategies</h3>
              </div>
              <p className="text-gray-600">
                Choose from SQL, Hybrid, Export/Import, Schema Only, Storage Migration, Linked, Common, and Dump strategies.
              </p>
            </div>

            <div className="bg-white rounded-lg shadow-md p-6">
              <div className="flex items-center mb-4">
                <CheckCircleIcon className="h-6 w-6 text-green-600 mr-2" />
                <h3 className="text-lg font-semibold">Connection Management</h3>
              </div>
              <p className="text-gray-600">
                Create reusable connection profiles for your clusters with support for multiple connection pool types.
              </p>
            </div>

            <div className="bg-white rounded-lg shadow-md p-6">
              <div className="flex items-center mb-4">
                <CheckCircleIcon className="h-6 w-6 text-green-600 mr-2" />
                <h3 className="text-lg font-semibold">Dataset Organization</h3>
              </div>
              <p className="text-gray-600">
                Define collections of databases and tables with flexible filtering options for targeted migrations.
              </p>
            </div>

            <div className="bg-white rounded-lg shadow-md p-6">
              <div className="flex items-center mb-4">
                <CheckCircleIcon className="h-6 w-6 text-green-600 mr-2" />
                <h3 className="text-lg font-semibold">Configuration Wizards</h3>
              </div>
              <p className="text-gray-600">
                Step-by-step wizards guide you through creating and managing migration configurations.
              </p>
            </div>

            <div className="bg-white rounded-lg shadow-md p-6">
              <div className="flex items-center mb-4">
                <CheckCircleIcon className="h-6 w-6 text-green-600 mr-2" />
                <h3 className="text-lg font-semibold">RocksDB Storage</h3>
              </div>
              <p className="text-gray-600">
                Persistent storage for configurations, connections, and datasets using embedded RocksDB.
              </p>
            </div>

            <div className="bg-white rounded-lg shadow-md p-6">
              <div className="flex items-center mb-4">
                <CheckCircleIcon className="h-6 w-6 text-green-600 mr-2" />
                <h3 className="text-lg font-semibold">Comprehensive Reporting</h3>
              </div>
              <p className="text-gray-600">
                Detailed reports on migration progress, issues, and recommendations for troubleshooting.
              </p>
            </div>
          </div>
        </div>

        {/* Getting Started */}
        <div className="mb-16 bg-blue-50 rounded-lg p-8">
          <h2 className="text-3xl font-bold text-gray-900 mb-6 flex items-center">
            <RocketLaunchIcon className="h-8 w-8 mr-3 text-blue-600" />
            Getting Started
          </h2>
          <div className="space-y-6">
            <div className="flex items-start">
              <div className="flex-shrink-0 w-8 h-8 bg-blue-600 text-white rounded-full flex items-center justify-center font-bold mr-4">
                1
              </div>
              <div>
                <h3 className="text-xl font-semibold text-gray-900 mb-2">Set Up Connections</h3>
                <p className="text-gray-700">
                  Navigate to the Connections page and create connection profiles for your source and target clusters.
                  Configure connection pooling, metastore details, and test connectivity.
                </p>
              </div>
            </div>

            <div className="flex items-start">
              <div className="flex-shrink-0 w-8 h-8 bg-blue-600 text-white rounded-full flex items-center justify-center font-bold mr-4">
                2
              </div>
              <div>
                <h3 className="text-xl font-semibold text-gray-900 mb-2">Define Datasets</h3>
                <p className="text-gray-700">
                  Create dataset definitions to organize your databases and tables. Use filters to include/exclude
                  specific tables based on patterns, size, or partition counts.
                </p>
              </div>
            </div>

            <div className="flex items-start">
              <div className="flex-shrink-0 w-8 h-8 bg-blue-600 text-white rounded-full flex items-center justify-center font-bold mr-4">
                3
              </div>
              <div>
                <h3 className="text-xl font-semibold text-gray-900 mb-2">Create Migration Configuration</h3>
                <p className="text-gray-700">
                  Use the Configuration Wizard to create a migration plan. Select your data strategy, choose connections,
                  configure table filters, and set migration options.
                </p>
              </div>
            </div>

            <div className="flex items-start">
              <div className="flex-shrink-0 w-8 h-8 bg-blue-600 text-white rounded-full flex items-center justify-center font-bold mr-4">
                4
              </div>
              <div>
                <h3 className="text-xl font-semibold text-gray-900 mb-2">Execute & Monitor</h3>
                <p className="text-gray-700">
                  Execute your migration and monitor progress through the web interface. Review generated reports
                  and SQL scripts for validation and troubleshooting.
                </p>
              </div>
            </div>
          </div>
        </div>

        {/* Quick Links */}
        <div className="text-center">
          <h2 className="text-3xl font-bold text-gray-900 mb-8">Ready to Get Started?</h2>
          <div className="flex flex-wrap justify-center gap-4">
            <button
              onClick={() => navigate('/connections')}
              className="inline-flex items-center px-6 py-3 border border-transparent text-base font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700 shadow-lg"
            >
              <CogIcon className="h-5 w-5 mr-2" />
              Manage Connections
              <ArrowRightIcon className="h-5 w-5 ml-2" />
            </button>

            <button
              onClick={() => navigate('/datasets')}
              className="inline-flex items-center px-6 py-3 border border-transparent text-base font-medium rounded-md text-white bg-green-600 hover:bg-green-700 shadow-lg"
            >
              <CircleStackIcon className="h-5 w-5 mr-2" />
              Manage Datasets
              <ArrowRightIcon className="h-5 w-5 ml-2" />
            </button>

            <button
              onClick={() => navigate('/config/manage')}
              className="inline-flex items-center px-6 py-3 border border-transparent text-base font-medium rounded-md text-white bg-purple-600 hover:bg-purple-700 shadow-lg"
            >
              <DocumentTextIcon className="h-5 w-5 mr-2" />
              Manage Configurations
              <ArrowRightIcon className="h-5 w-5 ml-2" />
            </button>
          </div>
        </div>

        {/* Version Info */}
        <div className="mt-16 text-center text-sm text-gray-500">
          <p>HMS-Mirror v{version || 'unknown'} - Built with Java 17+ and Spring Boot</p>
          <p className="mt-2">
            For documentation and support, visit the{' '}
            <a
              href="https://github.com/cloudera-labs/hms-mirror"
              target="_blank"
              rel="noopener noreferrer"
              className="text-blue-600 hover:text-blue-800 underline"
            >
              HMS-Mirror GitHub Repository
            </a>
          </p>
        </div>
      </div>
    </div>
  );
};

export default HomePage;
