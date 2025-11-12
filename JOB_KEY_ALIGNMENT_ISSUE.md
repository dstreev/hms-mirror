# Job Key Alignment Issue - Analysis and Fix

## Problem
The Job Validation endpoint returns `404 NOT_FOUND` with message "Job not found or failed to build conversion result", even though the job exists and can be listed via the jobs API.

## Root Cause Analysis

### Data Flow
1. **Job Creation**: When a job is created, it's stored in RocksDB using `jobDto.getKey()` as the storage key
2. **Job Listing**: The `/api/v1/jobs` endpoint returns jobs with a `jobKey` field
3. **Job Validation**: The `/api/v1/jobs/{jobKey}/validate` endpoint receives the `jobKey` from the URL parameter

### Key Storage (JobRepositoryImpl.java:64-76)
```java
public JobDto save(JobDto jobDto) throws RepositoryException {
    // ...
    if (jobDto.getKey() == null) {
        throw new RepositoryException("JobDto does not have a key");
    }
    return super.save(jobDto.getKey(), jobDto);  // Uses jobDto.getKey() as storage key
}
```

### Key in Job List Response (JobManagementService.java:71-77)
```java
for (Map.Entry<String, JobDto> entry : jobsMap.entrySet()) {
    String rocksDbKey = entry.getKey();  // This is the actual key used in RocksDB
    JobDto jobDto = entry.getValue();

    Map<String, Object> jobInfo = new HashMap<>();
    jobInfo.put("jobKey", rocksDbKey);  // Returns the RocksDB storage key as "jobKey"
    // ...
}
```

### Key in JobDto (JobDto.java:26)
```java
private String key = LocalDateTime.now().format(KEY_FORMATTER) + "_" + UUID.randomUUID().toString().substring(0, 4);
```
Format: `yyyyMMdd_HHmmssSSS_xxxx` where xxxx is a 4-character UUID fragment

## The Issue

The `JobDto` class has a single field called `key`, but:
- **Storage**: Uses `jobDto.getKey()` value directly as RocksDB key
- **API Response**: Returns this as `jobKey` in JSON
- **Validation**: Receives `jobKey` parameter and looks it up

**This should work correctly!** The same key is used throughout. So why is the lookup failing?

## Possible Causes

### 1. Job Not Actually Persisted
The job might be in the list because it's cached or in-memory, but not actually written to RocksDB.

**Verification**:
```bash
# Check if job exists in RocksDB
curl -s http://localhost:8090/hms-mirror/api/v1/rocksdb/data/jobs/20251111_124154255_ab6a
```

### 2. Key Format Mismatch
The key returned by the list API might not match what's stored.

**Verification**: Check logs after calling validate - they should show:
```
=== Building ConversionResult from jobId: 20251111_124154255_ab6a ===
Looking up job with key: 20251111_124154255_ab6a
Job not found with key: 20251111_124154255_ab6a
Available job keys in RocksDB: [actual_key_1, actual_key_2, ...]
```

### 3. RepositoryException During Lookup
The `findByKey()` method might be throwing an exception that's being caught and returning null.

**Verification**: Check for RepositoryException stack traces in logs.

### 4. Job References Invalid
The job exists, but its references (leftConnectionReference, rightConnectionReference, datasetReference, configReference) don't exist, causing buildConversionResultFromJobId to return null.

**Verification**: The enhanced logging shows exactly which reference failed:
- "Left connection not found: {reference}"
- "Right connection not found: {reference}"
- "Dataset not found: {reference}"
- "Configuration not found: {reference}"

## Enhanced Logging Added

### JobManagementService.buildConversionResultFromJobId (lines 425-444)
```java
log.info("=== Building ConversionResult from jobId: {} ===", jobId);
log.info("Looking up job with key: {}", jobId);
// ... lookup ...
if (!jobOpt.isPresent()) {
    log.error("Job not found with key: {}", jobId);
    // List all available keys
    Map<String, JobDto> allJobs = jobRepository.findAll();
    log.error("Available job keys in RocksDB: {}", allJobs.keySet());
    return null;
}
log.info("Found job: {} with key: {}", jobDto.getName(), jobDto.getKey());
```

### JobsController.validateJob (lines 232-234)
```java
log.info("About to call jobManagementService.buildConversionResultFromJobId with key: {}", jobKey);
ConversionResult conversionResult = jobManagementService.buildConversionResultFromJobId(jobKey);
log.info("buildConversionResultFromJobId returned: {}", conversionResult != null ? "non-null" : "null");
```

## Next Steps

1. **Rebuild and restart** with enhanced logging
2. **Call validate endpoint** and check logs
3. **Identify which of the 4 causes** is the actual problem:
   - No log output → Job lookup is throwing exception before logging
   - "Job not found" + key list → Key mismatch issue
   - "Found job" + "Left/Right connection not found" → Reference validation failing
   - "Found job" + "Dataset not found" → Dataset reference failing
   - "Found job" + "Configuration not found" → Config reference failing

4. **Fix based on findings**

## Testing Commands

```bash
# 1. List all jobs and get a jobKey
curl -s http://localhost:8090/hms-mirror/api/v1/jobs | jq '.jobs[0].jobKey'

# 2. Validate that job
curl -X POST -H "Content-Type: application/json" -d "{}" \
  http://localhost:8090/hms-mirror/api/v1/jobs/{jobKey}/validate

# 3. Check logs
tail -100 /tmp/hms-mirror-debug.log | grep -A20 "Building ConversionResult"
```
