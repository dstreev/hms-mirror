# Tooltip Integration Example for ConfigWizard

This document shows how to integrate the @Schema annotation tooltips into ConfigWizard (and other wizards).

## Step 1: Import Required Dependencies

Add these imports at the top of ConfigWizard.tsx:

```typescript
import { useState, useEffect } from 'react';
import FieldWithTooltip from '../common/FieldWithTooltip';
import schemaService from '../../services/schemaService';
```

## Step 2: Add State for Schema Descriptions

Add this state near the top of the ConfigWizard component:

```typescript
const [schemaDescriptions, setSchemaDescriptions] = useState<Map<string, string>>(new Map());

// Fetch schema descriptions on mount
useEffect(() => {
  const fetchDescriptions = async () => {
    const descriptions = await schemaService.getClassDescriptions('ConfigLiteDto');
    setSchemaDescriptions(descriptions);
  };
  fetchDescriptions();
}, []);
```

## Step 3: Replace Field Labels with FieldWithTooltip

### Before (Old code):
```typescript
<div>
  <label htmlFor="migrateNonNative" className="block text-sm font-medium text-gray-700 mb-2">
    Migrate Non-Native Tables
  </label>
  <input
    type="checkbox"
    id="migrateNonNative"
    checked={config.migrateNonNative || false}
    onChange={(e) => handleCheckboxChange('migrateNonNative', e.target.checked)}
    className="h-4 w-4"
  />
</div>
```

### After (With tooltip):
```typescript
<div>
  <FieldWithTooltip
    label="Migrate Non-Native Tables"
    tooltip={schemaDescriptions.get('migrateNonNative')}
    htmlFor="migrateNonNative"
    className="mb-2"
  />
  <input
    type="checkbox"
    id="migrateNonNative"
    checked={config.migrateNonNative || false}
    onChange={(e) => handleCheckboxChange('migrateNonNative', e.target.checked)}
    className="h-4 w-4"
  />
</div>
```

## Step 4: Apply to Other Fields

Apply the same pattern to other fields. Here are more examples:

### Text Input Example:
```typescript
<div>
  <FieldWithTooltip
    label="Configuration Name"
    tooltip={schemaDescriptions.get('name')}
    required={true}
    htmlFor="configName"
    className="mb-2"
  />
  <input
    type="text"
    id="configName"
    value={config.name}
    onChange={(e) => handleInputChange('name', e.target.value)}
    className="w-full p-3 border border-gray-300 rounded-md"
  />
</div>
```

### Select Example:
```typescript
<div>
  <FieldWithTooltip
    label="Enable Auto Table Stats"
    tooltip={schemaDescriptions.get('enableAutoTableStats')}
    htmlFor="enableAutoTableStats"
    className="mb-2"
  />
  <select
    id="enableAutoTableStats"
    value={config.enableAutoTableStats ? 'true' : 'false'}
    onChange={(e) => handleCheckboxChange('enableAutoTableStats', e.target.value === 'true')}
    className="w-full p-3 border border-gray-300 rounded-md"
  >
    <option value="false">Disabled</option>
    <option value="true">Enabled</option>
  </select>
</div>
```

## Fields to Update in ConfigWizard

Here's a list of ConfigLiteDto fields that now have @Schema descriptions available for tooltips:

- `name` - Configuration name
- `description` - Configuration description
- `migrateNonNative` - Migrate non-native tables flag
- `createIfNotExists` - Create if not exists flag
- `enableAutoTableStats` - Auto table statistics flag
- `enableAutoColumnStats` - Auto column statistics flag
- `saveWorkingTables` - Save working tables flag
- `copyAvroSchemaUrls` - Copy Avro schema URLs flag
- `forceExternalLocation` - Force external location flag
- `icebergConversion.enabled` - Iceberg conversion flag
- `migrateACID.on` - ACID migration flag
- `migrateVIEW.on` - View migration flag
- And many more nested fields...

## Benefits

1. **Single Source of Truth**: Descriptions come from @Schema annotations in Java code
2. **Consistency**: Same descriptions used in API docs and UI
3. **Maintainability**: Update description once in Java, automatically reflected in UI
4. **Better UX**: Users get helpful context on hover without cluttering the UI

## Testing

After integration, test by:
1. Starting the backend server (it must be running for schema API to work)
2. Opening ConfigWizard
3. Hovering over the info icon (ⓘ) next to field labels
4. Verify tooltip appears with the correct @Schema description

## Applying to Other Wizards

The same pattern can be applied to other wizards:
- **ConnectionWizard**: Use `ConnectionDto` as the className
- **DatasetWizard**: Use `DatasetDto` as the className
- **JobWizard**: Use `JobDto` as the className

Just change the className parameter in the `getClassDescriptions()` call to match the DTO being edited.
