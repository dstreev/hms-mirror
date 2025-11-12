# Import/Export Feature Implementation Guide

## Overview
This document describes the implementation of Import/Export functionality for Connections, Datasets, and Configurations.

## Status

### ✅ Completed
1. **Utility Functions** (`src/main/frontend/src/utils/importExport.ts`)
   - `exportToJson<T>(data: T, filename: string)` - Downloads object as JSON file
   - `importFromJson<T>(file: File)` - Reads and parses JSON file
   - `generateExportFilename(name, type)` - Generates safe filenames

2. **Import Dialog Component** (`src/main/frontend/src/components/common/ImportDialog.tsx`)
   - Generic dialog for all entity types
   - Handles file selection and parsing
   - Detects name conflicts (409 responses)
   - Allows user to rename on conflict
   - Retries import with new name

3. **ConnectionCard Component** (`src/main/frontend/src/components/connections/ConnectionCard.tsx`)
   - Added Export button with download icon
   - Added `onExport` prop to handle export

### 🔲 TODO - Connections
Update `ConnectionsPage.tsx`:

```typescript
import ImportDialog from '../common/ImportDialog';
import { exportToJson, generateExportFilename } from '../../utils/importExport';
import { ArrowUpTrayIcon } from '@heroicons/react/24/outline';

// Add state
const [showImportDialog, setShowImportDialog] = useState(false);

// Add Export handler
const handleExport = (connection: Connection) => {
  const filename = generateExportFilename(connection.name, 'connection');
  exportToJson(connection, filename);
};

// Add Import handler
const handleImport = async (data: Connection, newName?: string) => {
  try {
    if (newName) {
      data.name = newName;
    }

    const response = await fetch('/hms-mirror/api/v1/connections', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(data)
    });

    const result = await response.json();

    if (response.status === 409) {
      return { success: false, status: 409, message: result.message };
    }

    if (response.ok) {
      await loadConnections();
      return { success: true };
    }

    return { success: false, message: result.message };
  } catch (error: any) {
    return { success: false, message: error.message };
  }
};

// Add Import button next to "New Connection"
<button
  onClick={() => setShowImportDialog(true)}
  className="inline-flex items-center px-4 py-2 border border-gray-300 rounded-md shadow-sm text-sm font-medium text-gray-700 bg-white hover:bg-gray-50"
>
  <ArrowUpTrayIcon className="h-4 w-4 mr-2" />
  Import
</button>

// Add Import Dialog before closing div
<ImportDialog<Connection>
  isOpen={showImportDialog}
  onClose={() => setShowImportDialog(false)}
  onImport={handleImport}
  title="Import Connection"
  itemType="Connection"
  getNameFromData={(data) => data.name}
  setNameInData={(data, newName) => ({ ...data, name: newName })}
/>

// Update ConnectionCard usage to include onExport
<ConnectionCard
  //... existing props
  onExport={() => handleExport(connection)}
/>
```

### 🔲 TODO - Datasets
Similar updates needed for:
- `DatasetCard.tsx` - Add Export button and `onExport` prop
- `DatasetsPage.tsx` - Add Import button and handlers

### 🔲 TODO - Configurations
Similar updates needed for:
- `ConfigCard.tsx` - Add Export button and `onExport` prop
- `ConfigurationsPage.tsx` - Add Import button and handlers

## User Experience

### Export Flow
1. User clicks "Export" button on a card
2. Browser downloads `{type}-{name}-{date}.json` file
3. File contains complete JSON representation of the object

### Import Flow
1. User clicks "Import" button on list page
2. Import dialog opens
3. User selects JSON file
4. Dialog shows preview of object name
5. User clicks "Import"
6. **If name conflict (409):**
   - Dialog shows warning
   - Name field becomes editable
   - User changes name
   - User clicks "Import" again
7. **On success:**
   - Dialog closes
   - List refreshes
   - New item appears

## API Requirements

All entity APIs must support:

### POST endpoint behavior:
- Returns `201 Created` on success
- Returns `409 Conflict` if name exists
- Response body on conflict:
  ```json
  {
    "status": "CONFLICT",
    "message": "A {entity} with the name '{name}' already exists. Please use a different name."
  }
  ```

### Backend Changes Already Implemented:
- ✅ Datasets: POST returns 409 on duplicate name
- ✅ Datasets: PUT handles renames with conflict detection
- ⚠️ Connections: Need to verify 409 behavior
- ⚠️ Configurations: Need to verify 409 behavior

## File Structure

```
src/main/frontend/src/
├── utils/
│   └── importExport.ts          (✅ Created)
├── components/
│   ├── common/
│   │   └── ImportDialog.tsx     (✅ Created)
│   ├── connections/
│   │   ├── ConnectionCard.tsx   (✅ Updated - Export button added)
│   │   └── ConnectionsPage.tsx  (🔲 TODO - Import functionality)
│   ├── datasets/
│   │   ├── DatasetCard.tsx      (🔲 TODO - Export button)
│   │   └── DatasetsPage.tsx     (🔲 TODO - Import functionality)
│   └── configs/
│       ├── ConfigCard.tsx       (🔲 TODO - Export button)
│       └── ConfigsPage.tsx      (🔲 TODO - Import functionality)
```

## Testing Scenarios

### Test 1: Export Connection
1. Navigate to Connections page
2. Click Export on any connection
3. Verify file downloads as `connection-{name}-{date}.json`
4. Open file and verify JSON structure

### Test 2: Import New Connection
1. Export a connection
2. Delete it from the system
3. Click Import button
4. Select the exported JSON file
5. Verify connection is recreated

### Test 3: Import Duplicate Name
1. Export a connection (e.g., "Production")
2. Click Import button
3. Select the exported file
4. Verify conflict dialog appears
5. Change name to "Production-Copy"
6. Click Import
7. Verify new connection "Production-Copy" is created

### Test 4: Import Invalid JSON
1. Create a text file with invalid JSON
2. Click Import button
3. Select the invalid file
4. Verify error message appears

## Implementation Priority

1. **High Priority** (User-facing features):
   - Complete ConnectionsPage import functionality
   - Add export/import to DatasetsPage
   - Add export/import to ConfigurationsPage

2. **Medium Priority** (Backend verification):
   - Verify Connection API returns 409 on duplicate
   - Verify Configuration API returns 409 on duplicate

3. **Low Priority** (Enhancements):
   - Add bulk export (export multiple selected items)
   - Add export validation (verify exported data structure)
   - Add import preview (show full object details before import)

## Notes

- Export files include ALL data including internal fields (created dates, test results, etc.)
- User can manually edit exported JSON files before importing
- Import validates JSON structure but not business logic (backend handles that)
- File naming uses sanitized names (special characters replaced with underscores)
- Timestamps in filenames use ISO date format (YYYY-MM-DD)
