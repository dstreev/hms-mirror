# SPA Routing Fix Summary

## Problem

The Job validation button was returning a 404 error with the message:
```
No static resource jobs/build
```

This indicated that Spring Boot was trying to serve React Router paths (like `/jobs/build`) as static files instead of forwarding them to the React SPA.

## Root Causes

### 1. Missing SPA Fallback Controller
The application lacked a controller to forward React Router paths to `index.html`, which is required for client-side routing to work.

### 2. Incorrect Frontend API Path
The `jobApi.ts` was using the wrong base URL:
- **Before**: `super()` → defaulted to `/hms-mirror/api`
- **After**: `super('/hms-mirror/api/v1')` → matches backend API structure

### 3. Path Configuration Alignment
The React app, Spring Boot, and routing needed to be aligned:
- **Spring Context Path**: `/hms-mirror` (configured in `application.yaml`)
- **React Base Path**: `/hms-mirror/` (configured in `vite.config.ts`)
- **API Endpoints**: `/hms-mirror/api/v1/...`

## Changes Made

### 1. Created `SpaFallbackController.java`
```java
@Controller
@GetMapping(value = {"/hms-mirror/**"})
public String forward() {
    return "forward:/hms-mirror/index.html";
}
```

This forwards all non-API, non-static React Router paths to the SPA entry point.

### 2. Updated `jobApi.ts`
```typescript
class JobApi extends BaseApi {
  constructor() {
    super('/hms-mirror/api/v1');  // ✅ Correct base path
  }

  async validateJob(jobKey: string) {
    // Calls: /hms-mirror/api/v1/jobs/{jobKey}/validate
    return await this.post(`/jobs/${jobKey}/validate`, {});
  }
}
```

### 3. Updated `ReactResourceConfig.java`
Added resource handler to serve React app from `/hms-mirror/` path:
```java
registry.addResourceHandler("/hms-mirror/**")
        .addResourceLocations("classpath:/static/react/");
```

### 4. Updated `WebConfig.java`
- Removed old Thymeleaf root redirect
- Updated interceptor exclusions to allow API calls but exclude static resources

## Complete URL Structure

| Type | Path | Example |
|------|------|---------|
| React UI | `/hms-mirror/*` | `/hms-mirror/jobs/build` |
| REST API | `/hms-mirror/api/v1/*` | `/hms-mirror/api/v1/jobs/{key}/validate` |
| Static Assets | `/hms-mirror/static/*` | `/hms-mirror/static/js/main.js` |
| Legacy | `/react/*` | `/react/index.html` (backward compat) |

## How to Rebuild and Test

### Full Rebuild (Recommended)
```bash
# Clean and rebuild everything
mvn clean package -DskipTests

# Run the application
java -jar target/hms-mirror-4.0.0.0.jar
```

### Quick Rebuild (During Development)
```bash
# Rebuild frontend only
cd src/main/frontend
npm run build
cd ../../..

# Restart Spring Boot
mvn spring-boot:run -Dspring-boot.run.arguments="--hms-mirror.rocksdb.enabled=true"
```

### Access the Application
```
http://localhost:8080/hms-mirror/
```

### Test the Validate Button

1. Navigate to: **Runtime → Define** (`/hms-mirror/jobs/build`)
2. Find a job card
3. Click the **Validate** button
4. Check browser DevTools Network tab - should see:
   ```
   POST /hms-mirror/api/v1/jobs/{jobKey}/validate
   Status: 200 OK
   ```

## Verification Checklist

- [ ] Navigate to `/hms-mirror/` → Shows React app
- [ ] Navigate to `/hms-mirror/jobs/build` → Shows Jobs page (no 404)
- [ ] Click Validate button → Returns validation results (no 404)
- [ ] Refresh page on any route → Page loads correctly (no redirect to root)
- [ ] API calls in Network tab show correct paths: `/hms-mirror/api/v1/...`
- [ ] Static assets load from: `/hms-mirror/static/...`

## Configuration Files Reference

### Spring Boot Context Path
**File**: `src/main/resources/application.yaml`
```yaml
server:
  servlet:
    context-path: /hms-mirror
```

### React Base Path
**File**: `src/main/frontend/vite.config.ts`
```typescript
export default defineConfig({
  base: '/hms-mirror/',
  // ...
})
```

### API Base URL
**File**: `src/main/frontend/src/services/api/baseApi.ts`
```typescript
constructor(baseURL: string = '/hms-mirror/api') {
  // ...
}
```

### Backend API Mapping
**File**: `*Controller.java`
```java
@RestController
@RequestMapping("/api/v1/jobs")  // Results in: /hms-mirror/api/v1/jobs
public class JobsController {
  @PostMapping("/{jobKey}/validate")
  public ResponseEntity<...> validateJob(@PathVariable String jobKey) {
    // Endpoint: POST /hms-mirror/api/v1/jobs/{jobKey}/validate
  }
}
```

## Future Considerations

### Option 1: Simplify to Root Path
If you prefer the app at root `/` instead of `/hms-mirror/`:

1. **Change** `application.yaml`:
   ```yaml
   server:
     servlet:
       context-path: /
   ```

2. **Change** `vite.config.ts`:
   ```typescript
   base: '/'
   ```

3. **Change** `baseApi.ts`:
   ```typescript
   constructor(baseURL: string = '/api')
   ```

### Option 2: Use Reverse Proxy
If deploying behind nginx/Apache, configure proxy to strip path:
```nginx
location / {
    proxy_pass http://localhost:8080/hms-mirror/;
}
```

## Troubleshooting

### Still Getting 404 on Validate
1. Clear browser cache
2. Verify rebuild completed: `ls -la src/main/resources/static/react/`
3. Check application logs for endpoint mapping
4. Check browser DevTools → Network → Request URL

### React Routes Show 404
1. Verify `SpaFallbackController` is loaded (check logs)
2. Verify `hms-mirror.ui.version=react` (or not set)
3. Check resource handlers are registered

### Static Assets 404
1. Check build output exists: `src/main/resources/static/react/`
2. Verify resource handlers in `ReactResourceConfig`
3. Check browser DevTools → Network → Response Headers

## Related Files

| File | Purpose |
|------|---------|
| `SpaFallbackController.java` | Forwards React Router paths to SPA |
| `ReactResourceConfig.java` | Configures static resource serving |
| `WebConfig.java` | Configures interceptors and view controllers |
| `jobApi.ts` | Frontend API client (fixed base URL) |
| `vite.config.ts` | React build configuration |
| `application.yaml` | Spring Boot configuration |
