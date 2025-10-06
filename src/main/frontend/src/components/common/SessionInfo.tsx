import React, { useState, useEffect } from 'react';
import { sessionApi, SessionInfo as SessionInfoType } from '../../services/api/sessionApi';

interface SessionInfoProps {
  className?: string;
  showStatus?: boolean;
  compact?: boolean;
}

const SessionInfo: React.FC<SessionInfoProps> = ({ 
  className = '', 
  showStatus = false, 
  compact = false 
}) => {
  const [sessionInfo, setSessionInfo] = useState<SessionInfoType | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchSessionInfo = async () => {
      try {
        console.log('Fetching session info from:', '/hms-mirror/api/v2/session/info');
        const info = await sessionApi.getCurrentSessionInfo();
        console.log('Session info received:', info);
        setSessionInfo(info);
        setError(null);
      } catch (err) {
        console.error('Error fetching session info:', err);
        setError('Failed to fetch session info');
      }
    };

    fetchSessionInfo();

    // Refresh session info every 30 seconds
    const interval = setInterval(fetchSessionInfo, 30000);

    return () => clearInterval(interval);
  }, []);

  if (error) {
    return (
      <div className={`text-xs text-gray-400 ${className}`}>
        Session: error ({error})
      </div>
    );
  }

  if (!sessionInfo) {
    return (
      <div className={`text-xs text-gray-400 ${className}`}>
        Loading session...
      </div>
    );
  }

  if (compact) {
    return (
      <div className={`text-xs text-gray-500 font-mono ${className}`}>
        {sessionInfo.sessionId === 'unknown' ? 'Session: unknown' : sessionInfo.sessionId}
      </div>
    );
  }

  return (
    <div className={`text-xs text-gray-500 ${className}`}>
      <div className="font-semibold">Session:</div>
      <div className="font-mono text-gray-600 break-all">
        {sessionInfo.sessionId === 'unknown' ? 'unknown (check console)' : sessionInfo.sessionId}
      </div>
      {showStatus && (
        <div className="mt-1 space-y-1">
          {sessionInfo.running && (
            <div className="flex items-center text-green-600">
              <div className="w-2 h-2 bg-green-500 rounded-full mr-1"></div>
              Running
            </div>
          )}
          {sessionInfo.connected && (
            <div className="flex items-center text-blue-600">
              <div className="w-2 h-2 bg-blue-500 rounded-full mr-1"></div>
              Connected
            </div>
          )}
          <div className="text-gray-500">
            Concurrency: {sessionInfo.concurrency}
          </div>
        </div>
      )}
    </div>
  );
};

export default SessionInfo;