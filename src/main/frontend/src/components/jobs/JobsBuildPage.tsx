import React from 'react';
import { useNavigate } from 'react-router-dom';
import { 
  PlusIcon, 
  ListBulletIcon
} from '@heroicons/react/24/outline';

const JobsBuildPage: React.FC = () => {
  const navigate = useNavigate();

  const cards = [
    {
      title: 'Create Job',
      description: 'Build a new HMS-Mirror job by selecting datasets, configurations, connections, and data strategy options.',
      icon: PlusIcon,
      action: () => navigate('/jobs/build/wizard'),
      buttonText: 'Create New Job',
      buttonClass: 'bg-blue-600 hover:bg-blue-700 text-white'
    },
    {
      title: 'Job List',
      description: 'View, edit, copy, and manage existing HMS-Mirror jobs that have been created.',
      icon: ListBulletIcon,
      action: () => navigate('/jobs/list'),
      buttonText: 'View Jobs',
      buttonClass: 'bg-gray-600 hover:bg-gray-700 text-white'
    }
  ];

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="max-w-7xl mx-auto py-12 px-4 sm:px-6 lg:px-8">
        <div className="text-center mb-12">
          <h1 className="text-4xl font-bold text-gray-900 mb-4">
            Jobs Build
          </h1>
          <p className="text-xl text-gray-600 max-w-3xl mx-auto">
            Create and manage HMS-Mirror migration jobs. Build new jobs with guided wizards or manage existing job configurations.
          </p>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-8 max-w-4xl mx-auto">
          {cards.map((card, index) => {
            const IconComponent = card.icon;
            return (
              <div
                key={index}
                className="bg-white rounded-lg shadow-lg p-8 hover:shadow-xl transition-shadow duration-300 border border-gray-200"
              >
                <div className="flex items-center justify-center w-16 h-16 bg-blue-100 rounded-lg mx-auto mb-6">
                  <IconComponent className="w-8 h-8 text-blue-600" />
                </div>
                <h3 className="text-2xl font-bold text-gray-900 text-center mb-4">
                  {card.title}
                </h3>
                <p className="text-gray-600 text-center mb-8 leading-relaxed">
                  {card.description}
                </p>
                <div className="text-center">
                  <button
                    onClick={card.action}
                    className={`inline-flex items-center px-6 py-3 border border-transparent text-base font-medium rounded-md focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 transition-colors duration-200 ${card.buttonClass}`}
                  >
                    {card.buttonText}
                  </button>
                </div>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
};

export default JobsBuildPage;