import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { CardGrid } from '../cards/CardGrid';
import { ConfigCard } from '../cards/ConfigCard';
import { ExecuteCard } from '../cards/ExecuteCard';

interface DashboardState {
  config: {
    hasConfig: boolean;
    configName?: string;
  };
  execution: {
    status: 'idle' | 'running' | 'paused' | 'completed' | 'error';
    progress: number;
    canExecute: boolean;
  };
}

const Dashboard: React.FC = () => {
  const navigate = useNavigate();
  const [state, setState] = useState<DashboardState>({
    config: { hasConfig: false },
    execution: { status: 'idle', progress: 0, canExecute: false }
  });

  const handleNewConfig = () => {
    console.log('Creating new configuration...');
    navigate('/config/edit/new');
  };

  const handleLoadConfig = () => {
    console.log('Navigating to load configuration...');
    navigate('/config/load');
  };

  const handleTemplates = () => {
    console.log('Navigating to templates...');
    navigate('/config/templates');
  };

  const handleStart = () => {
    if (state.execution.status === 'idle') {
      setState(prev => ({
        ...prev,
        execution: { ...prev.execution, status: 'running' }
      }));
      simulateProgress();
    } else if (state.execution.status === 'paused') {
      setState(prev => ({
        ...prev,
        execution: { ...prev.execution, status: 'running' }
      }));
    }
  };

  const handlePause = () => {
    setState(prev => ({
      ...prev,
      execution: { ...prev.execution, status: 'paused' }
    }));
  };

  const handleStop = () => {
    setState(prev => ({
      ...prev,
      execution: { 
        ...prev.execution, 
        status: 'idle',
        progress: 0
      }
    }));
  };

  const simulateProgress = () => {
    let progress = 0;
    const interval = setInterval(() => {
      progress += Math.random() * 10;
      if (progress >= 100) {
        progress = 100;
        setState(prev => ({
          ...prev,
          execution: {
            ...prev.execution,
            status: 'completed',
            progress: 100
          }
        }));
        clearInterval(interval);
      } else {
        setState(prev => ({
          ...prev,
          execution: {
            ...prev.execution,
            progress
          }
        }));
      }
    }, 500);
  };

  useEffect(() => {
    const canExecute = state.config.hasConfig;
    setState(prev => ({
      ...prev,
      execution: { ...prev.execution, canExecute }
    }));
  }, [state.config.hasConfig]);

  return (
    <div className="min-h-screen bg-gray-50 py-8">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="mb-8">
          <h1 className="text-3xl font-bold text-gray-900">HMS-Mirror Dashboard</h1>
          <p className="text-gray-600">Manage your Hive Metastore migration workflow</p>
        </div>

        <CardGrid columns={4} gap={6}>
          <ConfigCard
            onNewConfig={handleNewConfig}
            onLoadConfig={handleLoadConfig}
            onTemplates={handleTemplates}
            hasConfig={state.config.hasConfig}
            configName={state.config.configName}
          />

          <ExecuteCard
            status={state.execution.status}
            progress={state.execution.progress}
            onStart={handleStart}
            onPause={handlePause}
            onStop={handleStop}
            canExecute={state.execution.canExecute}
          />
        </CardGrid>
      </div>
    </div>
  );
};

export default Dashboard;