

# Company Overview

### Claude Code Prompt:
> In this SQL script I need to create 3 databases, each with 5-10 tables each.  The first database should contain raw data that is from external sources and the resulting tables would be EXTERNAL No
Purge hive tables with Text files as the source format.  The second database is the operational database that tracks transactional entities and would be 90% ACID based tables in ORC format.  Each
table should be built out with at least 5 transactional interactions.  The third database is the anayltics database, built from a combination of the first and second database.  The tables in this
database should be in parquet format and saved as EXTERNAL PURGE tables in Hive.  As the basis for these script, contemplate and document a fictious company called ACME that sells widgets and needs
to track various aspect involve with manufacturing, distribution, and marketing.  Each of the tables should have between 50-200 records for this simulation.  Use INSERT sql to populate the tables.  
Don't use individual INSERT SQL statements for each record, instead apply multiple records in each insert.

## ACME Widget Company - A mid-sized precision widget manufacturer operating 3 manufacturing facilities and 5 distribution centers, serving industrial customers across North America.

## Three-Layer Database Architecture

### 📥 DATABASE 1: acme_raw (Landing Zone)

Format: EXTERNAL tables, TEXT files, NO PURGE
Tables: 6 tables with 50+ records total

1. raw_customer_feed (15 records) - Customer data from CRM
2. raw_supplier_feed (8 records) - Supplier information
3. raw_material_prices (14 records) - Daily commodity pricing
4. raw_web_clickstream (7 records) - Website visitor analytics
5. raw_social_media_mentions (6 records) - Brand sentiment tracking
6. raw_weather_data (8 records) - Weather impact on logistics

### ⚙️ DATABASE 2: acme_ops (Operational)

Format: ACID tables, ORC format, TRANSACTIONAL
Tables: 9 tables with 150+ records total
Demonstrates: 25+ UPDATE/DELETE transactions showing real business operations

1. customers (10 records) - Customer master data
2. products (10 records) - Widget catalog (Standard, Premium, Industrial, Custom)
3. suppliers (8 records) - Vendor relationships
4. inventory (15 records) - Multi-warehouse inventory with 5 transactions
5. orders (10 records) - Customer orders with 5 status updates
6. order_items (11 records) - Order line items with 3 updates
7. shipments (5 records) - Delivery tracking with 5 status transitions
8. production_runs (10 records) - Manufacturing tracking with 5 updates
9. raw_materials (8 records) - Material inventory with 5 transactions

### 📊 DATABASE 3: acme_analytics (Analytics)

Format: EXTERNAL PURGE tables, PARQUET format
Tables: 7 aggregated analytical views

1. customer_360 - Comprehensive customer profile with credit scoring
2. sales_summary - Daily sales metrics by product
3. inventory_forecast - Stock levels, turnover, reorder recommendations
4. production_efficiency - Manufacturing KPIs and yield rates
5. customer_lifetime_value - CLV segmentation and retention analysis
6. supply_chain_metrics - Delivery performance and logistics costs
7. marketing_roi - Campaign effectiveness and channel attribution

## Key Features

### ✅ Realistic Business Scenarios:
- Manufacturing: 3 facilities producing 10 widget types
- Distribution: 5 warehouses managing inventory
- Sales: B2B transactions with credit terms
- Supply Chain: Multi-carrier shipping with tracking

### ✅ Transactional Examples (25+ demonstrated):
- Inventory allocation and releases
- Order status workflows (pending → processing → shipped → delivered)
- Production runs with quality holds
- Shipment status updates
- Material consumption and receipts

### ✅ Batch Inserts: All data loaded using multi-row INSERT statements

### ✅ Data Relationships:
- Customer orders → Order items → Shipments
- Products → Production runs → Raw materials
- Web analytics → Order conversions → Revenue attribution

### ✅ Validation Queries: Included at the end to verify record counts across all databases

### Record Counts

- Raw Database: 58 records across 6 tables
- Operational Database: 150+ records across 9 tables (with transaction history)
- Analytics Database: 100+ analytical records across 7 aggregate views

The script demonstrates a complete data pipeline from raw external feeds → operational transactions → business intelligence reporting, perfectly suited for testing HMS-Mirror migrations across
different table types and formats!

