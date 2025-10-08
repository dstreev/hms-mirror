import BaseApi from './baseApi';

export interface SessionInfo {
  sessionId: string;
  connected: boolean;
  running: boolean;
  concurrency: number;
}

export interface ExecuteSession {
  sessionId: string;
  concurrency: number;
  connected: boolean;
  runStatus: any;
  config: any;
  conversion: any;
}

class SessionApi extends BaseApi {
  
  async getCurrentSessionInfo(): Promise<SessionInfo> {
    return this.get<SessionInfo>('/v1/session/info');
  }

  async getCurrentSession(): Promise<ExecuteSession> {
    return this.get<ExecuteSession>('/v2/session/current');
  }

  async createSession(sessionId: string, config?: any): Promise<ExecuteSession> {
    return this.post<ExecuteSession>(`/v2/session/${sessionId}`, config);
  }

  async startSession(sessionId: string, concurrency: number = 10): Promise<any> {
    return this.post<any>(`/v2/session/${sessionId}/start?concurrency=${concurrency}`);
  }

  async saveSessionConfig(sessionId: string, config: any, maxThreads: number = 10): Promise<boolean> {
    return this.put<boolean>(`/v2/session/${sessionId}/config?maxThreads=${maxThreads}`, config);
  }

  async deleteSession(sessionId: string): Promise<void> {
    return this.delete<void>(`/v2/session/${sessionId}`);
  }

  async listSessions(): Promise<Record<string, ExecuteSession>> {
    return this.get<Record<string, ExecuteSession>>('/v2/session/list');
  }
}

export const sessionApi = new SessionApi();