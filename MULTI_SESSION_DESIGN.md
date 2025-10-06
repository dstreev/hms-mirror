# Multi-Session Support Design

## Overview

HMS-Mirror now supports multiple concurrent sessions within the same runtime environment. This enables:

- **Web UI**: Multiple browser sessions can run independent migration operations simultaneously
- **CLI**: Maintains single session behavior for backward compatibility
- **Session Isolation**: Each session has its own configuration, state, and execution context

## Architecture

### Core Components

1. **SessionManager**: Central service for managing multiple ExecuteSession instances
2. **SessionContextHolder**: Thread-local storage for session context using static ThreadLocal
3. **SessionInterceptor**: Web interceptor that associates HTTP sessions with ExecuteSession instances
4. **ExecuteSessionService**: Enhanced to read from thread-local context for web sessions while maintaining CLI compatibility
5. **ReactSpaController**: Enhanced to ensure sessions are created for React UI requests

### Session Association

#### Web UI Sessions
- Each HTTP session is automatically associated with an ExecuteSession
- Sessions are isolated per browser session/tab
- Session state persists across HTTP requests within the same browser session
- Sessions are automatically created when first accessed

#### CLI Sessions
- Uses the default session through ExecuteSessionService
- Maintains single-session behavior for backward compatibility
- No changes required to existing CLI workflows

## Implementation Details

### SessionManager Service

```java
@Service
public class SessionManager {
    public ExecuteSession getCurrentSession()
    public ExecuteSession createSession(String sessionId, HmsMirrorConfig config)
    public Boolean startSession(String sessionId, Integer concurrency)
    public boolean save(String sessionId, HmsMirrorConfig config, int maxThreads)
    public void closeSession(String sessionId)
    public Map<String, ExecuteSession> getAllSessions()
}
```

### Key Features

1. **Automatic Session Association**
   - Web requests automatically get associated with an ExecuteSession
   - Sessions are stored in HTTP session attributes
   - Thread-local context provides session access throughout request processing

2. **Session Lifecycle Management**
   - Sessions are created on-demand
   - Sessions can be explicitly closed when no longer needed
   - HTTP sessions automatically clean up associated ExecuteSession data

3. **Backward Compatibility**
   - CLI continues to use ExecuteSessionService directly
   - Existing code paths remain unchanged
   - Default session behavior preserved

### Session Context Flow

1. **Web Request Processing**:
   ```
   HTTP Request → SessionInterceptor → SessionContextHolder.setSession() 
   → Controller/Service → ExecuteSessionService.getSession() → SessionContextHolder.getSession() → ExecuteSession
   ```

2. **CLI Processing**:
   ```
   CLI → ExecuteSessionService.getSession() → local session field → ExecuteSession (default)
   ```

### Implementation Details

#### SessionContextHolder
```java
public class SessionContextHolder {
    private static final ThreadLocal<ExecuteSession> sessionContext = new ThreadLocal<>();
    
    public static void setSession(ExecuteSession session) {
        sessionContext.set(session);
    }
    
    public static ExecuteSession getSession() {
        return sessionContext.get();
    }
    
    public static void clearSession() {
        sessionContext.remove();
    }
}
```

#### SessionInterceptor
- Intercepts all web requests
- Creates unique session IDs with "web-" prefix
- Associates ExecuteSession with HTTP session
- Sets thread-local context for request processing
- Enhanced logging for debugging session flow

#### ExecuteSessionService Integration
- Modified `getSession()` to check thread-local context first
- Falls back to local session field for CLI usage
- Resolves circular dependency issues with SessionManager

## API Endpoints

### New Session Management Endpoints

- `GET /api/v2/session/current` - Get current session
- `GET /api/v2/session/info` - Get current session basic info (used by React UI)
- `GET /api/v2/session/debug` - Debug session information for troubleshooting
- `GET /api/v2/session/{sessionId}` - Get specific session
- `POST /api/v2/session/{sessionId}` - Create new session
- `POST /api/v2/session/{sessionId}/start` - Start session execution
- `PUT /api/v2/session/{sessionId}/config` - Save session configuration
- `DELETE /api/v2/session/{sessionId}` - Delete session
- `GET /api/v2/session/list` - List all sessions

## Usage Examples

### Web UI - Multiple Sessions

```javascript
// Browser Tab 1 - Creates session automatically
POST /api/v2/session/migration-prod-1
{
  "clusters": { ... },
  "dataStrategy": "HYBRID"
}

// Browser Tab 2 - Creates separate session automatically  
POST /api/v2/session/migration-test-1
{
  "clusters": { ... },
  "dataStrategy": "SCHEMA_ONLY"
}

// Both sessions can run concurrently
POST /api/v2/session/migration-prod-1/start?concurrency=20
POST /api/v2/session/migration-test-1/start?concurrency=10
```

### CLI - Single Session (Unchanged)

```bash
# Works exactly as before
./bin/hms-mirror --config my-config.yaml --dry-run
```

## Configuration

### Web Configuration

The `SessionInterceptor` is automatically configured to intercept all web requests:

```java
@Configuration
public class WebConfig implements WebMvcConfigurer {
    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(sessionInterceptor)
                .addPathPatterns("/**")
                .excludePathPatterns("/static/**", "/css/**", "/js/**");
    }
}
```

### Spring Bean Configuration

All components are automatically configured as Spring beans:

- `@Service SessionManager`
- `@Component SessionInterceptor`
- `@Service ExecuteSessionService` (enhanced)

## Benefits

1. **Concurrent Operations**: Multiple migration operations can run simultaneously
2. **Session Isolation**: Each session maintains independent state and configuration
3. **Resource Efficiency**: Shared connection pools and services across sessions
4. **Backward Compatibility**: Existing CLI and API functionality unchanged
5. **Scalability**: Support for multiple concurrent users in web interface

## Considerations

1. **Memory Usage**: Each session maintains its own state and configuration
2. **Resource Limits**: Consider system limits when running multiple concurrent operations
3. **Session Cleanup**: HTTP session expiration will clean up associated ExecuteSession data
4. **Thread Safety**: All session operations are thread-safe using concurrent collections

## Migration Path

No migration is required for existing functionality:

- CLI applications continue to work unchanged
- Existing web API endpoints maintain backward compatibility
- Single-session web usage continues to work as before
- New multi-session features are opt-in through new API endpoints

## Implementation Challenges & Solutions

### 1. Circular Dependency Resolution
**Problem**: SessionManager depended on ExecuteSessionService, and ExecuteSessionService needed SessionManager for web sessions.

**Solution**: Modified ExecuteSessionService to use SessionContextHolder directly instead of depending on SessionManager:
```java
public ExecuteSession getSession() {
    // Check thread-local context first for web sessions
    ExecuteSession contextSession = SessionContextHolder.getSession();
    if (contextSession != null) {
        return contextSession;
    }
    // Fall back to local session for CLI
    return session;
}
```

### 2. React UI API Integration
**Problem**: React UI showing "Failed to fetch session info" due to incorrect API URLs.

**Issues Resolved**:
- BaseApi baseURL: `/hms-mirror/api` + SessionApi paths: `/api/v2/session/info` = `/hms-mirror/api/api/v2/session/info` (double /api)
- **Solution**: Updated SessionApi to use `/v2/session/info` (removed duplicate /api prefix)

### 3. Session Display in UI
**Implementation**:
- Added `SessionInfo` React component for both Thymeleaf and React UIs
- Shows session ID subtly in the interface
- Supports both compact and full display modes
- Real-time session status updates

### 4. Thread-Local Context Management
**Key Implementation Details**:
- SessionInterceptor sets thread-local context on each request
- Context is automatically cleared after request processing
- Thread-safe session isolation between concurrent web requests
- No impact on CLI session behavior

## React Frontend Integration

### Session API Client
```typescript
class SessionApi extends BaseApi {
  async getCurrentSessionInfo(): Promise<SessionInfo> {
    return this.get<SessionInfo>('/v2/session/info');
  }
  
  async getCurrentSession(): Promise<ExecuteSession> {
    return this.get<ExecuteSession>('/v2/session/current');
  }
}
```

### Session Display Component
```typescript
const SessionInfo: React.FC<SessionInfoProps> = ({ compact = false }) => {
  const [sessionInfo, setSessionInfo] = useState<SessionInfoType | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchSessionInfo = async () => {
      try {
        const info = await sessionApi.getCurrentSessionInfo();
        setSessionInfo(info);
      } catch (err) {
        setError('Failed to fetch session info');
      }
    };
    fetchSessionInfo();
  }, []);
}
```

## Session ID Generation

### Web Sessions
- Format: `web-{httpSessionId}` (e.g., `web-9B563B51`)
- Based on truncated HTTP session ID for uniqueness
- Automatically created by SessionInterceptor

### CLI Sessions  
- Default session ID: `default.yaml`
- Maintains backward compatibility
- Single session per CLI instance

## Debugging and Monitoring

### Debug Endpoint
`GET /api/v2/session/debug` provides comprehensive session state information:
- Thread context session
- HTTP session details
- SessionManager session state
- Request attributes availability

### Logging
Enhanced logging throughout the session lifecycle:
- SessionInterceptor: Request processing and session association
- ExecuteSessionService: Session resolution logic
- SessionManager: Session creation and management operations