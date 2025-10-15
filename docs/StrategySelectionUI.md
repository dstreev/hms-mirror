### Strategy Selection UI for React Application

This specification defines an intelligent strategy selection interface for the HMS-Mirror React UI that guides users through choosing the appropriate data strategy based on their specific requirements.

## UI Flow Design

### Initial Screen: Migration Goal Selection
Replace the simple strategy dropdown with an interactive decision tree starting with the primary migration goal.

#### Question 1: What is your primary migration goal?

**Radio Button Options:**
- 🔄 **Move schemas + data between clusters**
  - Description: "Migrate both metadata and data to a different cluster"
  - Most common use case
- 📋 **Move schemas only, handle data separately** 
  - Description: "Migrate metadata only, use distcp for data movement"
- 🧊 **Convert to Iceberg format**
  - Description: "Convert existing tables to Iceberg table format"
- 📦 **Move data within cluster to new storage**
  - Description: "Change storage location within same cluster (HDFS→Ozone, HDFS→S3)"
- 🔗 **Test new cluster with old data (read-only)**
  - Description: "Create read-only access to existing data for testing"
- 🤝 **Clusters share same physical storage**
  - Description: "Only metadata needs to move, data is already accessible"
- 💾 **Extract schemas only (no target yet)**
  - Description: "Generate SQL files for later manual execution"

**Next Button**: Proceeds to appropriate follow-up question based on selection

---

### Flow Branch 1: Move schemas + data between clusters

#### Question 2: Can your clusters access each other's storage?

**Radio Button Options:**
- ✅ **Yes, clusters can access each other's storage**
- ❌ **No, but we have intermediate storage both can access**
- 🚫 **No shared storage access at all**

#### If "Yes" → Question 3a: What describes your table characteristics?

**Radio Button Options:**
- 🔀 **Mix of small and large partitioned tables**
  - Shows: "HYBRID strategy recommended"
  - Details: "Auto-selects best method per table based on partition count"
- 📊 **Mostly tables with < 100 partitions**
  - Shows: "EXPORT_IMPORT strategy recommended" 
  - Details: "Good for small partitioned tables"
- 📈 **Mostly tables with > 100 partitions**
  - Shows: "SQL strategy recommended"
  - Details: "Better for large partitioned tables"

#### If "Intermediate storage" → Shows Strategies:
- **SQL with intermediateStorage** (Recommended)
- **EXPORT_IMPORT with intermediateStorage** (Alternative)

#### If "No shared storage" → Shows Error:
- "❌ Direct data movement not possible without shared storage"
- "Consider: Setting up intermediate storage, or using SCHEMA_ONLY + manual data transfer"

---

### Flow Branch 2: Move schemas only
**Result**: Directly recommends **SCHEMA_ONLY** strategy
- "✅ SCHEMA_ONLY strategy selected"
- "Generates distcp plans for separate data movement"

---

### Flow Branch 3: Convert to Iceberg format

#### Question 2: Where do you want the Iceberg tables?

**Radio Button Options:**
- 🎯 **Same cluster (in-place conversion)**
  - Shows: "STORAGE_MIGRATION with SQL recommended"
- 🔄 **Different cluster (conversion during migration)**
  - Shows: "SQL strategy with Iceberg conversion recommended"
  - Proceeds to cluster access questions (same as Branch 1)

---

### Flow Branch 4: Move data within cluster
**Result**: Directly recommends **STORAGE_MIGRATION** strategy
- "✅ STORAGE_MIGRATION strategy selected"
- "Choose SQL or DISTCP for data movement method"

---

### Flow Branch 5: Test new cluster (read-only)
**Result**: Directly recommends **LINKED** strategy
- "⚠️ LINKED strategy selected (TESTING ONLY)"
- "Automatically sets readOnly=true and noPurge=true"

---

### Flow Branch 6: Shared physical storage
**Result**: Directly recommends **COMMON** strategy
- "✅ COMMON strategy selected"
- "Only metadata moves, no data movement needed"

---

### Flow Branch 7: Extract schemas only
**Result**: Directly recommends **DUMP** strategy
- "✅ DUMP strategy selected"
- "No target cluster required, generates SQL files"

---

## Final Strategy Selection Screen

### Strategy Confirmation Panel
Once a strategy is determined, show a confirmation panel:

```
┌─────────────────────────────────────────────────────────┐
│ 🎯 Recommended Strategy: SQL                             │
├─────────────────────────────────────────────────────────┤
│ Why this strategy?                                      │
│ • You want to move schemas + data between clusters     │
│ • Clusters can access each other's storage             │
│ • You have mostly large partitioned tables (>100)     │
│                                                         │
│ Key Features:                                          │
│ ✅ Supports Iceberg conversion                         │
│ ✅ Better for large partitioned tables                 │
│ ✅ Uses SQL INSERT statements for data movement        │
│                                                         │
│ Requirements:                                          │
│ • Target cluster must be accessible                   │
│ • Sufficient processing capacity for SQL operations    │
└─────────────────────────────────────────────────────────┘

[◄ Change Strategy]  [Continue with SQL Strategy ►]
```

### Alternative Strategies Section
Show other compatible strategies with brief explanations:

```
Other Compatible Strategies:
┌─────────────────────────────────────────────────────────┐
│ HYBRID - Mix of SQL and EXPORT_IMPORT                  │
│ • Auto-selects method per table                        │
│ • Good if you're unsure about partition sizes          │
│                                                         │
│ EXPORT_IMPORT - Hive Export/Import                     │
│ • Better for smaller partitioned tables                │
│ • More robust for complex table structures             │
└─────────────────────────────────────────────────────────┘
```

## UI Implementation Notes

### Progress Indicator
Show a breadcrumb-style progress indicator:
```
Migration Goal > Cluster Access > Table Characteristics > Strategy Selected
```

### Back Navigation
Each step should allow going back to revise previous answers.

### Strategy Validation
Before proceeding to configuration:
- Validate that required connectionDtos exist for the selected strategy
- Show warnings for any missing prerequisites
- Provide links to create missing connectionDto profiles

### Help Integration
Each question should have:
- 💡 **Tooltip help** explaining technical terms
- 🔗 **"Learn more" links** to detailed documentation
- ❓ **"Not sure?" options** that provide additional guidance

### Mobile Responsiveness
- Stack radio buttons vertically on mobile
- Use collapsible sections for strategy details
- Ensure touch-friendly button sizing

This guided approach replaces the intimidating strategy dropdown with an intuitive decision tree that educates users while helping them make the right choice for their specific use case.