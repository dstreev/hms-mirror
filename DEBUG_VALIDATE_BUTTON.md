# Debug: Validate Button 404 Issue

## Summary
The backend endpoint **works correctly**:
- ✅ POST to `/hms-mirror/api/v1/jobs/{jobKey}/validate` returns proper response
- ✅ Curl test confirms endpoint exists and processes POST requests
- ❌ Browser button click is somehow sending wrong request

## Step-by-Step Debugging

### 1. Open Browser DevTools
Press **F12** or right-click → Inspect

### 2. Go to Console Tab
Check for any JavaScript errors:
- Look for red error messages
- Look for "Uncaught" errors
- Look for "jobApi" or "validateJob" errors

**Take a screenshot and share any errors you see**

### 3. Go to Network Tab
1. Click "Clear" to clear existing requests
2. Make sure "Preserve log" is checked
3. Click the **Validate** button on a job card
4. Look for the request in the Network tab

### 4. Inspect the Failed Request
Find the request (might show as red/failed) and check:

**a) Request URL** - What URL was called?
```
Expected: http://localhost:8090/hms-mirror/api/v1/jobs/{jobKey}/validate
Actual:   ??? (please check)
```

**b) Request Method** - What HTTP method was used?
```
Expected: POST
Actual:   ??? (please check)
```

**c) Status Code** - What status was returned?
```
Expected: 200, 400, or 500 (not 404)
Actual:   ??? (please check)
```

**d) Request Headers** - Check Headers tab:
```
Should include:
  - Content-Type: application/json
  - (possibly other headers)
```

**e) Request Payload** - Check Payload/Request tab:
```
Should be: {} (empty JSON object)
```

**f) Response** - Check Response/Preview tab:
```
What error message does it show?
```

### 5. Check Job Key Value
In the Console tab, when on the Jobs page, type:
```javascript
// This will show you the jobs data
console.log(document.querySelector('[data-job-key]'));
```

Make sure the jobKey is not:
- `undefined`
- `null`
- Empty string `""`
- "jobs/build" (confused with URL)

### 6. Manual Test in Console
Open Console and paste this (replace ACTUAL_JOB_KEY with a real job key from the page):
```javascript
fetch('/hms-mirror/api/v1/jobs/ACTUAL_JOB_KEY/validate', {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json'
  },
  body: '{}'
})
.then(r => r.json())
.then(data => console.log('SUCCESS:', data))
.catch(err => console.error('ERROR:', err));
```

Does this work?

## Common Causes

### A. Browser Cache (Most Likely)
Even after "hard refresh", service workers can cache old JS:

**Fix:**
1. DevTools → Application tab
2. Click "Service Workers" on left
3. Click "Unregister" for hms-mirror
4. Click "Clear storage" on left
5. Click "Clear site data" button
6. Close DevTools
7. Close browser completely
8. Reopen and try again

### B. Incorrect Base URL
The jobApi might still be using old base URL

**Check in Console:**
```javascript
// Paste this in console to see what base URL is being used
console.log(axios.defaults.baseURL);
```

### C. JobKey is Undefined
The job object might not have a jobKey property

**Check:**
Look at the Network request - if URL ends with `/jobs/undefined/validate` or `/jobs/null/validate`, this is the issue.

### D. CORS or Proxy Issue
If running frontend separately from backend

**Check:**
- Are you accessing via http://localhost:8090?
- Or via http://localhost:3000 (React dev server)?

If using port 3000, the proxy might not be configured correctly.

## Expected Working Flow

1. Click "Validate" button
2. JavaScript calls: `jobApi.validateJob(jobKey)`
3. Which calls: `this.post('/jobs/{jobKey}/validate', {})`
4. BaseApi prepends: `/hms-mirror/api/v1`
5. Axios sends: `POST http://localhost:8090/hms-mirror/api/v1/jobs/{jobKey}/validate`
6. Backend receives and processes
7. Returns: 200 OK (or 400/500 for errors, NOT 404)

## What to Report Back

Please provide:
1. **Console tab screenshot** - showing any errors
2. **Network tab screenshot** - showing the failed request details
3. **The actual URL** that was called (from Network → Request URL)
4. **The HTTP method** that was used (GET or POST?)
5. **The Response** content (what error message?)
6. **Browser and version** (Chrome 120? Firefox 121?)

## Quick Test

Can you test these URLs directly in browser (expect errors, but different errors):

1. **GET (will fail with "method not supported"):**
   ```
   http://localhost:8090/hms-mirror/api/v1/jobs/test123/validate
   ```
   Expected: "Request method 'GET' is not supported" ✅ You got this

2. **Test jobs list works:**
   ```
   http://localhost:8090/hms-mirror/api/v1/jobs
   ```
   Expected: JSON list of jobs

3. **Test specific job (use real job key):**
   ```
   http://localhost:8090/hms-mirror/api/v1/jobs/REAL_JOB_KEY
   ```
   Expected: JSON job data

If all 3 work, the backend is fine and issue is in frontend JavaScript/browser.
