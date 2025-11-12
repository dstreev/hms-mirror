# Property Translation Issues - HmsMirrorConfigurationLoader

## Analysis Date: November 2, 2025

## Summary
Comparison of original `HmsMirrorCommandLineOptions.java` (107 @Bean methods) vs new `HmsMirrorConfigurationLoader.java` revealed **21 property discrepancies**.

## MISSING FROM NEW (Need to Add)

### 1. `hms-mirror.config.debug-dir`
**Original Implementation** (line 168):
```java
@ConditionalOnProperty(name = "hms-mirror.config.debug-dir")
CommandLineRunner runDebugSession(...) {
    // Implementation for debug directory
}
```
**Status**: ❌ Missing in new file
**Action**: Need to add

---

### 2. `hms-mirror.config.force-external-location`
**Original Implementation** (lines 681-697):
```java
@ConditionalOnProperty(name = "hms-mirror.config.force-external-location", havingValue = "true")
CommandLineRunner configForceExternalLocationTrue(...) {
    config.getTranslator().setForceExternalLocation(Boolean.TRUE);
}
```
**Status**: ❌ Incorrectly abbreviated as `.fel` in new file
**Action**: Change `.fel` → `.force-external-location`

---

### 3. `hms-mirror.config.help`
**Original Implementation** (line 719-720):
```java
@ConditionalOnProperty(name = "hms-mirror.config.help")
CommandLineRunner configHelp(HmsMirrorConfig hmsMirrorConfig, @Value("${hms-mirror.config.help}") String value) {
    log.info("help: {}", value);
    // Display help
}
```
**Status**: ❌ Missing in new file
**Action**: Need to add

---

### 4. `hms-mirror.config.migrate-non-native`
**Original Implementation** (lines 848-867):
```java
@ConditionalOnProperty(name = "hms-mirror.config.migrate-non-native", havingValue = "true")
CommandLineRunner configMigrateNonNativeTrue(...) {
    config.setMigrateNonNative(Boolean.TRUE);
}
```
**Status**: ❌ Missing in new file
**Action**: Need to add

---

### 5. `hms-mirror.config.migrate-non-native-only`
**Original Implementation** (lines 872-891):
```java
@ConditionalOnProperty(name = "hms-mirror.config.migrate-non-native-only", havingValue = "true")
CommandLineRunner configMigrateNonNativeOnlyTrue(...) {
    config.setMigrateNonNative(Boolean.TRUE);
}
```
**Status**: ❌ Missing in new file
**Action**: Need to add

---

### 6. `hms-mirror.config.right-is-disconnected`
**Original Implementation** (lines 1055-1072):
```java
@ConditionalOnProperty(name = "hms-mirror.config.right-is-disconnected", havingValue = "true")
CommandLineRunner configRightIsDisconnectedTrue(...) {
    config.getCluster(Environment.RIGHT).setHcfsNamespace("DUMMY");
}
```
**Status**: ❌ Incorrectly abbreviated as `.rid` in new file
**Action**: Change `.rid` → `.right-is-disconnected`

---

### 7. `hms-mirror.config.sort-dynamic-partition-inserts`
**Original Implementation** (lines 1241-1257):
```java
@ConditionalOnProperty(name = "hms-mirror.config.sort-dynamic-partition-inserts", havingValue = "true")
CommandLineRunner configSortDynamicPartitionInsertsTrue(...) {
    config.getOptimization().setSortDynamicPartitionInserts(Boolean.TRUE);
}
```
**Status**: ❌ Incorrectly abbreviated as `.sdpi` in new file
**Action**: Change `.sdpi` → `.sort-dynamic-partition-inserts`

---

### 8. `hms-mirror.config.transfer-ownership`
**Original Implementation** (lines 1403-1438):
```java
@ConditionalOnProperty(name = "hms-mirror.config.transfer-ownership", havingValue = "true")
CommandLineRunner configTransferOwnershipTrue(...) {
    config.getOwnershipTransfer().setDatabase(Boolean.TRUE);
    config.getOwnershipTransfer().setTable(Boolean.TRUE);
}
```
**Status**: ❌ Incorrectly abbreviated as `.to` in new file
**Action**: Change `.to` → `.transfer-ownership`

---

### 9. `hms-mirror.config.transfer-ownership-database`
**Original Implementation** (lines 1379-1386):
```java
@ConditionalOnProperty(name = "hms-mirror.config.transfer-ownership-database", havingValue = "true")
CommandLineRunner configTransferOwnershipDbTrue(...) {
    config.getOwnershipTransfer().setDatabase(Boolean.TRUE);
}
```
**Status**: ❌ Missing in new file
**Action**: Need to add

---

### 10. `hms-mirror.config.transfer-ownership-table`
**Original Implementation** (lines 1391-1398):
```java
@ConditionalOnProperty(name = "hms-mirror.config.transfer-ownership-table", havingValue = "true")
CommandLineRunner configTransferOwnershipTblTrue(...) {
    config.getOwnershipTransfer().setTable(Boolean.TRUE);
}
```
**Status**: ❌ Missing in new file
**Action**: Need to add

---

### 11. `hms-mirror.config.views-only`
**Original Implementation** (lines 1444-1462):
```java
@ConditionalOnProperty(name = "hms-mirror.config.views-only", havingValue = "true")
CommandLineRunner configViewsOnlyTrue(...) {
    config.getMigrateVIEW().setOn(Boolean.TRUE);
}
```
**Status**: ❌ Incorrectly named as `migrate-views-only` in new file
**Action**: Change `.migrate-views-only` → `.views-only` AND change method

---

## INCORRECTLY ADDED IN NEW (Need to Remove/Fix)

### 1. `.fel`
**Issue**: Abbreviated form of `force-external-location`
**Action**: Replace with full property name `.force-external-location`

### 2. `.migrate-views-only`
**Issue**: Should be `.views-only`
**Current Code**: `config.setMigrateVIEW(value);`
**Should Be**: `config.getMigrateVIEW().setOn(value);`
**Action**: Rename and fix implementation

### 3. `.only-acid-table-property`
**Issue**: Not found in original file
**Action**: Verify if this is needed or remove

### 4. `.reset-to-default-location`
**Issue**: Not found in original file
**Action**: Verify if this is needed or remove

### 5. `.rid`
**Issue**: Abbreviated form of `right-is-disconnected`
**Action**: Replace with full property name `.right-is-disconnected`

### 6. `.schema-only`
**Issue**: Not found in original file
**Action**: Verify if this is a CLI shortcut or remove

### 7. `.sdpi`
**Issue**: Abbreviated form of `sort-dynamic-partition-inserts`
**Action**: Replace with full property name `.sort-dynamic-partition-inserts`

### 8. `.so`
**Issue**: Not found in original file - possibly SQL output shortcut?
**Action**: Verify or remove

### 9. `.to`
**Issue**: Abbreviated form of `transfer-ownership`
**Action**: Replace with full property name `.transfer-ownership`

---

## SPECIAL CASES

### `hms-mirror.config.setup`
**Status**: ✅ Correctly handled via `@ConditionalOnProperty` on bean definition
**No action needed**

---

## IMPLEMENTATION CORRECTIONS NEEDED

### views-only (WRONG Implementation)
**Current (INCORRECT)**:
```java
applyBooleanIfPresent(CONFIG_PREFIX + ".migrate-views-only", value -> {
    config.setMigrateVIEW(value);
    log.info("migrate-views-only: {}", value);
});
```

**Should Be (CORRECT)**:
```java
applyBooleanIfPresent(CONFIG_PREFIX + ".views-only", value -> {
    config.getMigrateVIEW().setOn(value);
    log.info("views-only: {}", value);
});
```

### transfer-ownership (INCOMPLETE Implementation)
**Current (INCOMPLETE)**:
```java
applyBooleanIfPresent(CONFIG_PREFIX + ".to", value -> {
    config.setTransferOwnership(value);
    log.info("transfer-ownership: {}", value);
});
```

**Should Be (COMPLETE)**:
```java
// Main property
applyBooleanIfPresent(CONFIG_PREFIX + ".transfer-ownership", value -> {
    config.getOwnershipTransfer().setDatabase(value);
    config.getOwnershipTransfer().setTable(value);
    log.info("transfer-ownership: {}", value);
});

// Database-specific property
applyBooleanIfPresent(CONFIG_PREFIX + ".transfer-ownership-database", value -> {
    config.getOwnershipTransfer().setDatabase(value);
    log.info("transfer-ownership-database: {}", value);
});

// Table-specific property
applyBooleanIfPresent(CONFIG_PREFIX + ".transfer-ownership-table", value -> {
    config.getOwnershipTransfer().setTable(value);
    log.info("transfer-ownership-table: {}", value);
});
```

---

## SUMMARY STATISTICS

| Category | Count |
|----------|-------|
| **Missing Properties** | 11 |
| **Incorrectly Abbreviated** | 5 |
| **Wrong Implementation** | 2 |
| **Needs Verification** | 4 |
| **Total Issues** | 21 |

---

## ACTION PLAN

1. ✅ Document all discrepancies (this file)
2. ⏳ Update `HmsMirrorConfigurationLoader.java` with all corrections
3. ⏳ Add missing properties (11 items)
4. ⏳ Fix abbreviated property names (5 items)
5. ⏳ Correct implementation logic (views-only, transfer-ownership)
6. ⏳ Verify/remove unconfirmed properties (4 items)
7. ⏳ Test all corrections
8. ⏳ Update REFACTORING_NOTES.md

---

## NOTES

- Original file uses boolean havingValue for most flags
- Some properties have both true/false handlers
- Transfer ownership has 3 separate properties that work together
- Views-only uses `getMigrateVIEW().setOn()` not `setMigrateVIEW()`
- All abbreviated forms (fel, rid, sdpi, so, to) should use full names for clarity

---

Generated: November 2, 2025
Based on comparison of:
- Original: `HmsMirrorCommandLineOptions.java.backup-20251102-122824` (2213 lines, 107 @Bean methods)
- New: `HmsMirrorConfigurationLoader.java` (739 lines)
