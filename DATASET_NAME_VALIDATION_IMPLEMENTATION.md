# Dataset Name Validation & Duplicate Prevention

## Summary

Implemented robust name-based key management for Datasets to prevent accidental overwrites and handle name conflicts gracefully. Jobs continue to use generated keys (timestamp + UUID).

## Key Design Decisions

### 1. Key Strategy by Entity Type

| Entity | Key Strategy | Storage Key | Rationale |
|--------|--------------|-------------|-----------|
| **Dataset** | Name-based | `name` field value | User-friendly, prevents duplicates, easy to reference |
| **Job** | Generated | `yyyyMMdd_HHmmssSSS_xxxx` | Allows multiple jobs with same config, tracks execution history |
| **Connection** | Name-based | `name` field value | User-friendly, easy to reference |
| **Config** | Name-based | `name` field value | User-friendly, easy to reference |

### 2. Dataset Key Implementation

**DatasetDto.java (lines 71-80):**
```java
public String getKey() {
    if (key == null) {
        if (name == null) {
            throw new IllegalStateException("name is required");
        } else {
            key = name;  // Key is derived from name
        }
    }
    return key;
}
```

## Backend Changes

### 1. DatasetController - POST Method (Create Only)

**File:** `src/main/java/com/cloudera/utils/hms/mirror/web/controller/api/v1/DatasetController.java`

**Changes (lines 121-182):**
- **Before**: POST could create OR update datasets (implicit upsert)
- **After**: POST only creates NEW datasets
- **Behavior**: Returns **409 CONFLICT** if name already exists

**Response on Conflict:**
```json
{
  "status": "CONFLICT",
  "message": "A dataset with the name 'MyDataset' already exists. Please use a different name.",
  "existingName": "MyDataset"
}
```

### 2. DatasetController - PUT Method (Update with Rename Support)

**Changes (lines 184-275):**
- **Before**: Simple update without name change validation
- **After**:
  - Validates original dataset exists (404 if not found)
  - Detects name changes (rename operation)
  - Checks for name conflicts during rename (409 if new name exists)
  - Calls `datasetManagementService.rename()` for name changes
  - Calls `datasetManagementService.update()` for in-place updates

**Response on Successful Rename:**
```json
{
  "status": "SUCCESS",
  "operation": "renamed",
  "oldName": "OldDatasetName",
  "newName": "NewDatasetName"
}
```

**Response on Rename Conflict:**
```json
{
  "status": "CONFLICT",
  "message": "A dataset with the name 'NewName' already exists. Please use a different name.",
  "existingName": "NewName",
  "originalName": "OldName"
}
```

### 3. DatasetManagementService - New `rename` Method

**File:** `src/main/java/com/cloudera/utils/hms/mirror/service/DatasetManagementService.java`

**Added (lines 204-250):**
```java
public Map<String, Object> rename(String oldKey, DatasetDto datasetDto) {
    log.info("Renaming dataset from '{}' to '{}'", oldKey, datasetDto.getName());

    // Load existing dataset to preserve creation date
    Map<String, Object> loadResult = load(oldKey);
    if (!"SUCCESS".equals(loadResult.get("status"))) {
        return loadResult;
    }

    DatasetDto existingDataset = (DatasetDto) loadResult.get("data");

    // Preserve creation date
    if (existingDataset.getCreated() != null) {
        datasetDto.setCreated(existingDataset.getCreated());
    }

    // Delete old key
    datasetRepository.deleteById(oldKey);
    log.debug("Deleted old dataset key: {}", oldKey);

    // Save with new key (derived from datasetDto.getName())
    Map<String, Object> saveResult = save(datasetDto);

    if ("SUCCESS".equals(saveResult.get("status"))) {
        saveResult.put("operation", "renamed");
        saveResult.put("oldName", oldKey);
        saveResult.put("newName", datasetDto.getName());
    }

    return saveResult;
}
```

## Frontend Changes

### 1. DatasetWizard - Enhanced Error Handling

**File:** `src/main/frontend/src/components/datasets/DatasetWizard.tsx`

**Changes (lines 115-165):**
- **Before**: Generic error handling
- **After**:
  - Detects 409 CONFLICT responses
  - Automatically navigates to step 0 (Basic Info)
  - Shows name-specific error message

**Example Flow:**
```typescript
// User tries to save dataset with existing name
try {
  result = await datasetApi.saveDataset(formData);

  if (result.status === 409) {
    // Navigate back to Basic Info (step 0)
    setCurrentStep(0);

    // Show error on name field
    setErrors({
      name: `A dataset with the name "${formData.name}" already exists. Please use a different name.`
    });
  }
} catch (error) {
  // Handle network errors
}
```

### 2. DatasetApi - Status Code Propagation

**File:** `src/main/frontend/src/services/api/datasetApi.ts`

**Changes:**

**saveDataset method (lines 151-196):**
- Added `status?: number` to return type
- Checks for `response.status === 'CONFLICT'` → returns `status: 409`
- Checks for `error.response?.status === 409` → returns `status: 409`

**updateDataset method (lines 198-237):**
- Added `status?: number` to return type
- Same conflict detection logic for rename operations

**Response Format:**
```typescript
{
  success: boolean;
  message?: string;
  operation?: string;
  status?: number;  // HTTP status code (e.g., 409 for conflict)
}
```

## User Experience Flow

### Scenario 1: Create New Dataset with Duplicate Name

1. User fills out Dataset Wizard
2. User enters name "Production-Analytics"
3. User completes wizard and clicks "Save"
4. Backend returns 409 CONFLICT
5. **Wizard automatically navigates back to step 1 (Basic Info)**
6. **Name field shows error**: "A dataset with the name 'Production-Analytics' already exists. Please use a different name."
7. User changes name to "Production-Analytics-V2"
8. User proceeds through wizard again
9. Save succeeds ✅

### Scenario 2: Rename Dataset to Existing Name

1. User edits existing dataset "OldName"
2. User changes name to "ExistingName" (already in use)
3. User clicks "Save"
4. Backend returns 409 CONFLICT
5. **Wizard navigates back to step 1 (Basic Info)**
6. **Name field shows error**: "A dataset with the name 'ExistingName' already exists. Please use a different name."
7. User changes name to "NewUniqueName"
8. Update succeeds, old key deleted, new key created ✅

### Scenario 3: Rename Dataset to Unique Name

1. User edits existing dataset "OldName"
2. User changes name to "NewUniqueName" (not in use)
3. User clicks "Save"
4. Backend renames: deletes "OldName", creates "NewUniqueName"
5. Success message shows "renamed" operation ✅

## API Endpoints Summary

### POST /api/v1/datasets
- **Purpose**: Create NEW dataset only
- **Success**: 201 CREATED
- **Name Exists**: 409 CONFLICT
- **Validation Failed**: 400 BAD REQUEST

### PUT /api/v1/datasets/{key}
- **Purpose**: Update existing dataset, supports rename
- **Success**: 200 OK
- **Not Found**: 404 NOT FOUND
- **Name Conflict (rename)**: 409 CONFLICT
- **Validation Failed**: 400 BAD REQUEST

## Testing

### Manual Test Cases

#### Test 1: Create Duplicate Dataset
```bash
# Create first dataset
curl -X POST http://localhost:8090/hms-mirror/api/v1/datasets \
  -H "Content-Type: application/json" \
  -d '{"name":"TestDataset","description":"Test","databases":[{"databaseName":"db1","tables":["t1"]}]}'

# Try to create with same name - should return 409
curl -X POST http://localhost:8090/hms-mirror/api/v1/datasets \
  -H "Content-Type: application/json" \
  -d '{"name":"TestDataset","description":"Test2","databases":[{"databaseName":"db2","tables":["t2"]}]}'

# Expected: 409 CONFLICT with message about duplicate name
```

#### Test 2: Rename to Existing Name
```bash
# Create two datasets
curl -X POST http://localhost:8090/hms-mirror/api/v1/datasets \
  -H "Content-Type: application/json" \
  -d '{"name":"Dataset1","description":"First","databases":[{"databaseName":"db1","tables":["t1"]}]}'

curl -X POST http://localhost:8090/hms-mirror/api/v1/datasets \
  -H "Content-Type: application/json" \
  -d '{"name":"Dataset2","description":"Second","databases":[{"databaseName":"db2","tables":["t2"]}]}'

# Try to rename Dataset1 to Dataset2 - should return 409
curl -X PUT http://localhost:8090/hms-mirror/api/v1/datasets/Dataset1 \
  -H "Content-Type: application/json" \
  -d '{"name":"Dataset2","description":"First","databases":[{"databaseName":"db1","tables":["t1"]}]}'

# Expected: 409 CONFLICT with message about duplicate name
```

#### Test 3: Successful Rename
```bash
# Rename Dataset1 to Dataset1-Renamed (unique name)
curl -X PUT http://localhost:8090/hms-mirror/api/v1/datasets/Dataset1 \
  -H "Content-Type: application/json" \
  -d '{"name":"Dataset1-Renamed","description":"First","databases":[{"databaseName":"db1","tables":["t1"]}]}'

# Expected: 200 OK with operation: "renamed"

# Verify old key is gone
curl http://localhost:8090/hms-mirror/api/v1/datasets/Dataset1
# Expected: 404 NOT_FOUND

# Verify new key exists
curl http://localhost:8090/hms-mirror/api/v1/datasets/Dataset1-Renamed
# Expected: 200 OK with dataset data
```

## Benefits

1. **Data Integrity**: Prevents accidental overwrites of existing datasets
2. **User-Friendly**: Clear error messages guide users to fix the issue
3. **Intuitive UX**: Wizard automatically navigates to the right step
4. **Consistent API**: REST semantics (POST=create, PUT=update)
5. **Rename Support**: Users can rename datasets while preserving data
6. **No Silent Failures**: All conflicts are reported explicitly

## Migration Notes

**For Existing Deployments:**
- Existing datasets will continue to work (name is already used as key)
- No data migration needed
- API behavior change: POST will now reject duplicates instead of silently updating

**For Users:**
- Creating a new dataset with an existing name will now show an error
- Users will be prompted to choose a different name
- Editing a dataset and changing its name will rename the dataset
