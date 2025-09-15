import React from 'react';
import { PlayIcon, PauseIcon, StopIcon } from '@heroicons/react/24/outline';
import { motion } from 'framer-motion';
import { BaseCard } from './BaseCard';

interface ExecuteCardProps {
  status: 'idle' | 'running' | 'paused' | 'completed' | 'error';
  progress?: number;
  onStart: () => void;
  onPause: () => void;
  onStop: () => void;
  canExecute: boolean;
}

export const ExecuteCard: React.FC<ExecuteCardProps> = ({
  status,
  progress = 0,
  onStart,
  onPause,
  onStop,
  canExecute
}) => {
  const getStatusInfo = () => {
    switch (status) {
      case 'idle':
        return { text: 'Ready to execute', color: 'text-gray-600', cardStatus: 'idle' as const };
      case 'running':
        return { text: `Running (${Math.round(progress)}%)`, color: 'text-blue-600', cardStatus: 'loading' as const };
      case 'paused':
        return { text: 'Paused', color: 'text-yellow-600', cardStatus: 'warning' as const };
      case 'completed':
        return { text: 'Completed successfully', color: 'text-green-600', cardStatus: 'success' as const };
      case 'error':
        return { text: 'Execution failed', color: 'text-red-600', cardStatus: 'error' as const };
      default:
        return { text: 'Unknown status', color: 'text-gray-600', cardStatus: 'idle' as const };
    }
  };

  const statusInfo = getStatusInfo();

  return (
    <BaseCard
      title="Execute Migration"
      description={statusInfo.text}
      icon={<PlayIcon className="w-6 h-6" />}
      status={statusInfo.cardStatus}
      disabled={!canExecute && status === 'idle'}
    >
      <div className="space-y-4">
        {/* Progress Bar */}
        {status === 'running' && (
          <div className="space-y-2">
            <div className="flex justify-between text-sm">
              <span className="text-gray-600">Progress</span>
              <span className="font-medium">{Math.round(progress)}%</span>
            </div>
            <div className="w-full bg-gray-200 rounded-full h-2">
              <motion.div
                className="bg-blue-600 h-2 rounded-full"
                initial={{ width: 0 }}
                animate={{ width: `${progress}%` }}
                transition={{ duration: 0.3 }}
              />
            </div>
          </div>
        )}

        {/* Action Buttons */}
        <div className="flex space-x-2">
          {status === 'idle' && (
            <button
              onClick={onStart}
              disabled={!canExecute}
              className="flex-1 flex items-center justify-center space-x-2 bg-blue-600 text-white px-4 py-2 rounded-md hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
            >
              <PlayIcon className="w-4 h-4" />
              <span>Start Migration</span>
            </button>
          )}

          {status === 'running' && (
            <>
              <button
                onClick={onPause}
                className="flex-1 flex items-center justify-center space-x-2 bg-yellow-600 text-white px-4 py-2 rounded-md hover:bg-yellow-700 transition-colors"
              >
                <PauseIcon className="w-4 h-4" />
                <span>Pause</span>
              </button>
              <button
                onClick={onStop}
                className="flex-1 flex items-center justify-center space-x-2 bg-red-600 text-white px-4 py-2 rounded-md hover:bg-red-700 transition-colors"
              >
                <StopIcon className="w-4 h-4" />
                <span>Stop</span>
              </button>
            </>
          )}

          {status === 'paused' && (
            <button
              onClick={onStart}
              className="flex-1 flex items-center justify-center space-x-2 bg-green-600 text-white px-4 py-2 rounded-md hover:bg-green-700 transition-colors"
            >
              <PlayIcon className="w-4 h-4" />
              <span>Resume</span>
            </button>
          )}
        </div>

        {/* Status Details */}
        {(status === 'running' || status === 'completed' || status === 'error') && (
          <div className="text-xs text-gray-600 space-y-1">
            <div>Tables processed: 15/23</div>
            <div>Current: customer_data</div>
            <div>Elapsed: 00:05:32</div>
          </div>
        )}
      </div>
    </BaseCard>
  );
};

export default ExecuteCard;