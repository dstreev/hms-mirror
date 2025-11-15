-- =====================================================================================================================
-- ACME Widget Company - Complete Database Architecture
-- =====================================================================================================================
--
-- Company Overview:
-- ACME Widget Company is a mid-sized manufacturer of precision widgets for industrial applications.
-- The company operates 3 manufacturing facilities, maintains 5 distribution centers, and serves
-- customers across North America. ACME produces various widget types including Standard Widgets,
-- Premium Widgets, Industrial Widgets, and Custom Widgets.
--
-- Business Processes:
-- 1. MANUFACTURING: Raw materials sourced from suppliers are processed into finished widgets
-- 2. DISTRIBUTION: Finished goods stored in warehouses and shipped to customers
-- 3. MARKETING: Multi-channel campaigns drive customer acquisition and retention
--
-- Database Architecture:
-- 1. RAW DATABASE (acme_raw): External data feeds from various sources
-- 2. OPERATIONAL DATABASE (acme_ops): Transactional ACID tables tracking daily operations
-- 3. ANALYTICS DATABASE (acme_analytics): Aggregated views for business intelligence
-- =====================================================================================================================

-- =====================================================================================================================
-- DATABASE 1: RAW/LANDING ZONE (acme_raw)
-- Purpose: Store raw data from external sources without transformation
-- Table Type: EXTERNAL tables with NO PURGE (data persists even if table is dropped)
-- Format: TEXT files (CSV-like format from external systems)
-- =====================================================================================================================

CREATE DATABASE IF NOT EXISTS acme_raw
COMMENT 'Landing zone for external data feeds from suppliers, CRM, web analytics, and market data'
LOCATION '/warehouse/acme/raw';

USE acme_raw;

-- ---------------------------------------------------------------------------------------------------------------------
-- TABLE: raw_customer_feed
-- Source: External CRM system (daily feed)
-- Purpose: Customer master data from Salesforce
-- ---------------------------------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS raw_customer_feed (
    customer_id STRING,
    company_name STRING,
    contact_name STRING,
    email STRING,
    phone STRING,
    address STRING,
    city STRING,
    state STRING,
    zip_code STRING,
    industry STRING,
    annual_revenue STRING,
    employee_count STRING,
    credit_limit STRING,
    signup_date STRING,
    status STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '/warehouse/acme/raw/customer_feed'
TBLPROPERTIES ('external.table.purge'='false');

INSERT INTO raw_customer_feed VALUES
('C001', 'TechCorp Industries', 'John Smith', 'j.smith@techcorp.com', '555-0101', '123 Tech Blvd', 'San Francisco', 'CA', '94105', 'Technology', '50000000', '500', '100000', '2020-01-15', 'ACTIVE'),
('C002', 'Manufacturing Solutions LLC', 'Sarah Johnson', 's.johnson@mfgsol.com', '555-0102', '456 Factory Ln', 'Detroit', 'MI', '48201', 'Manufacturing', '25000000', '250', '75000', '2019-03-22', 'ACTIVE'),
('C003', 'AutoParts Direct', 'Mike Chen', 'm.chen@autoparts.com', '555-0103', '789 Auto Way', 'Cleveland', 'OH', '44114', 'Automotive', '75000000', '800', '150000', '2018-06-10', 'ACTIVE'),
('C004', 'BuildRight Construction', 'Emily Davis', 'e.davis@buildright.com', '555-0104', '321 Builder St', 'Houston', 'TX', '77002', 'Construction', '35000000', '400', '90000', '2020-08-05', 'ACTIVE'),
('C005', 'AeroSpace Dynamics', 'Robert Taylor', 'r.taylor@aerodyn.com', '555-0105', '654 Aerospace Dr', 'Seattle', 'WA', '98101', 'Aerospace', '120000000', '1200', '200000', '2017-11-30', 'ACTIVE'),
('C006', 'MedDevice Corp', 'Lisa Anderson', 'l.anderson@meddevice.com', '555-0106', '987 Medical Plaza', 'Boston', 'MA', '02101', 'Medical', '45000000', '350', '110000', '2019-02-14', 'ACTIVE'),
('C007', 'Energy Solutions Inc', 'David Martinez', 'd.martinez@energysol.com', '555-0107', '147 Power Grid Rd', 'Denver', 'CO', '80202', 'Energy', '65000000', '550', '130000', '2018-09-20', 'ACTIVE'),
('C008', 'Food Processing Co', 'Jennifer Wilson', 'j.wilson@foodproc.com', '555-0108', '258 Industry Pkwy', 'Chicago', 'IL', '60601', 'Food', '28000000', '300', '70000', '2020-04-12', 'ACTIVE'),
('C009', 'Robotics International', 'James Lee', 'j.lee@robotics.com', '555-0109', '369 Innovation Way', 'Austin', 'TX', '78701', 'Technology', '95000000', '700', '175000', '2019-05-08', 'ACTIVE'),
('C010', 'Marine Equipment Ltd', 'Amanda Brown', 'a.brown@marine.com', '555-0110', '741 Harbor View', 'Portland', 'OR', '97201', 'Marine', '32000000', '280', '80000', '2019-07-18', 'ACTIVE'),
('C011', 'Rail Systems Corp', 'Thomas Garcia', 't.garcia@railsys.com', '555-0111', '852 Track Ave', 'Atlanta', 'GA', '30303', 'Transportation', '58000000', '480', '120000', '2018-12-03', 'ACTIVE'),
('C012', 'Defense Contractors Inc', 'Patricia Rodriguez', 'p.rodriguez@defcon.com', '555-0112', '963 Defense Blvd', 'Arlington', 'VA', '22201', 'Defense', '150000000', '1500', '250000', '2017-05-25', 'ACTIVE'),
('C013', 'Packaging Solutions', 'Christopher White', 'c.white@packsol.com', '555-0113', '159 Container Dr', 'Memphis', 'TN', '38103', 'Packaging', '22000000', '200', '60000', '2020-10-08', 'ACTIVE'),
('C014', 'Textile Manufacturing', 'Michelle Thomas', 'm.thomas@textile.com', '555-0114', '357 Fabric Way', 'Charlotte', 'NC', '28202', 'Textile', '18000000', '180', '50000', '2019-11-22', 'ACTIVE'),
('C015', 'Chemical Processing', 'Daniel Jackson', 'd.jackson@chemproc.com', '555-0115', '456 Chemical Ln', 'Philadelphia', 'PA', '19102', 'Chemical', '72000000', '600', '140000', '2018-03-17', 'ACTIVE');

-- ---------------------------------------------------------------------------------------------------------------------
-- TABLE: raw_supplier_feed
-- Source: Procurement system export
-- Purpose: Supplier and vendor information
-- ---------------------------------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS raw_supplier_feed (
    supplier_id STRING,
    supplier_name STRING,
    contact_person STRING,
    email STRING,
    phone STRING,
    address STRING,
    city STRING,
    state STRING,
    country STRING,
    material_type STRING,
    rating STRING,
    payment_terms STRING,
    active_since STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '/warehouse/acme/raw/supplier_feed'
TBLPROPERTIES ('external.table.purge'='false');

INSERT INTO raw_supplier_feed VALUES
('S001', 'SteelWorks America', 'George Miller', 'g.miller@steelworks.com', '555-2001', '1000 Steel Mill Rd', 'Pittsburgh', 'PA', 'USA', 'Steel', '5', 'Net 30', '2015-01-10'),
('S002', 'Precision Bearings Ltd', 'Helen Carter', 'h.carter@precbear.com', '555-2002', '2000 Bearing Way', 'Cleveland', 'OH', 'USA', 'Bearings', '5', 'Net 45', '2016-03-15'),
('S003', 'Polymer Solutions Inc', 'Frank Adams', 'f.adams@polymer.com', '555-2003', '3000 Plastic Ave', 'Houston', 'TX', 'USA', 'Plastics', '4', 'Net 30', '2015-08-22'),
('S004', 'Electronics Components Co', 'Nancy Turner', 'n.turner@eleccomp.com', '555-2004', '4000 Circuit Blvd', 'San Jose', 'CA', 'USA', 'Electronics', '5', 'Net 60', '2014-11-05'),
('S005', 'FastenRight Supplies', 'Kevin Phillips', 'k.phillips@fastenright.com', '555-2005', '5000 Bolt Street', 'Milwaukee', 'WI', 'USA', 'Fasteners', '4', 'Net 30', '2016-06-30'),
('S006', 'Coating Masters', 'Barbara Campbell', 'b.campbell@coating.com', '555-2006', '6000 Paint Dr', 'Louisville', 'KY', 'USA', 'Coatings', '4', 'Net 30', '2015-09-12'),
('S007', 'Rubber & Gaskets LLC', 'Richard Parker', 'r.parker@rubber.com', '555-2007', '7000 Gasket Ln', 'Akron', 'OH', 'USA', 'Rubber', '5', 'Net 45', '2014-04-18'),
('S008', 'Aluminum Alloys Inc', 'Susan Evans', 's.evans@aluminum.com', '555-2008', '8000 Metal Way', 'Seattle', 'WA', 'USA', 'Aluminum', '5', 'Net 30', '2015-12-01');

-- ---------------------------------------------------------------------------------------------------------------------
-- TABLE: raw_material_prices
-- Source: Market data feed (daily commodity prices)
-- Purpose: Track raw material pricing for cost analysis
-- ---------------------------------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS raw_material_prices (
    price_date STRING,
    material_code STRING,
    material_name STRING,
    unit_price STRING,
    currency STRING,
    unit_of_measure STRING,
    `exchange` STRING,
    price_change_pct STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '/warehouse/acme/raw/material_prices'
TBLPROPERTIES ('external.table.purge'='false');

INSERT INTO raw_material_prices VALUES
('2024-01-15', 'STL001', 'Carbon Steel Sheet', '0.85', 'USD', 'LB', 'LME', '2.5'),
('2024-01-15', 'STL002', 'Stainless Steel 304', '1.45', 'USD', 'LB', 'LME', '1.8'),
('2024-01-15', 'ALU001', 'Aluminum 6061', '1.25', 'USD', 'LB', 'LME', '-0.5'),
('2024-01-15', 'PLT001', 'ABS Plastic Pellets', '0.95', 'USD', 'LB', 'NYMEX', '0.0'),
('2024-01-15', 'BRG001', 'Steel Bearings 10mm', '2.50', 'USD', 'UNIT', 'SPOT', '1.2'),
('2024-01-15', 'CST001', 'Powder Coating', '3.75', 'USD', 'LB', 'SPOT', '0.8'),
('2024-01-16', 'STL001', 'Carbon Steel Sheet', '0.87', 'USD', 'LB', 'LME', '2.4'),
('2024-01-16', 'STL002', 'Stainless Steel 304', '1.47', 'USD', 'LB', 'LME', '1.4'),
('2024-01-16', 'ALU001', 'Aluminum 6061', '1.24', 'USD', 'LB', 'LME', '-0.8'),
('2024-01-16', 'PLT001', 'ABS Plastic Pellets', '0.96', 'USD', 'LB', 'NYMEX', '1.1'),
('2024-01-17', 'STL001', 'Carbon Steel Sheet', '0.88', 'USD', 'LB', 'LME', '1.1'),
('2024-01-17', 'STL002', 'Stainless Steel 304', '1.48', 'USD', 'LB', 'LME', '0.7'),
('2024-01-17', 'ALU001', 'Aluminum 6061', '1.26', 'USD', 'LB', 'LME', '1.6'),
('2024-01-17', 'PLT001', 'ABS Plastic Pellets', '0.95', 'USD', 'LB', 'NYMEX', '-1.0');

-- ---------------------------------------------------------------------------------------------------------------------
-- TABLE: raw_web_clickstream
-- Source: Web analytics platform (Google Analytics export)
-- Purpose: Website visitor behavior for marketing analysis
-- ---------------------------------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS raw_web_clickstream (
    session_id STRING,
    visitor_id STRING,
    `timestamp` STRING,
    page_url STRING,
    page_title STRING,
    referrer STRING,
    device_type STRING,
    browser STRING,
    country STRING,
    session_duration STRING,
    pages_viewed STRING,
    conversion_flag STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '/warehouse/acme/raw/web_clickstream'
TBLPROPERTIES ('external.table.purge'='false');

INSERT INTO raw_web_clickstream VALUES
('WS001', 'V1001', '2024-01-15 09:15:23', '/products/industrial-widgets', 'Industrial Widgets - ACME', 'google.com', 'desktop', 'chrome', 'USA', '450', '8', 'true'),
('WS002', 'V1002', '2024-01-15 10:22:45', '/products/premium-widgets', 'Premium Widgets - ACME', 'bing.com', 'mobile', 'safari', 'USA', '320', '5', 'false'),
('WS003', 'V1003', '2024-01-15 11:30:12', '/contact-sales', 'Contact Sales - ACME', 'linkedin.com', 'desktop', 'firefox', 'Canada', '180', '3', 'true'),
('WS004', 'V1004', '2024-01-15 13:45:33', '/products/standard-widgets', 'Standard Widgets - ACME', 'google.com', 'tablet', 'chrome', 'USA', '280', '6', 'false'),
('WS005', 'V1005', '2024-01-15 14:55:21', '/case-studies', 'Case Studies - ACME', 'direct', 'desktop', 'edge', 'USA', '520', '12', 'true'),
('WS006', 'V1006', '2024-01-15 15:20:44', '/products/custom-widgets', 'Custom Widgets - ACME', 'facebook.com', 'mobile', 'chrome', 'Mexico', '210', '4', 'false'),
('WS007', 'V1007', '2024-01-15 16:10:15', '/request-quote', 'Request Quote - ACME', 'google.com', 'desktop', 'chrome', 'USA', '390', '7', 'true');

-- ---------------------------------------------------------------------------------------------------------------------
-- TABLE: raw_social_media_mentions
-- Source: Social media monitoring tool
-- Purpose: Brand mentions and sentiment analysis
-- ---------------------------------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS raw_social_media_mentions (
    mention_id STRING,
    platform STRING,
    username STRING,
    post_date STRING,
    mention_text STRING,
    sentiment STRING,
    engagement_score STRING,
    follower_count STRING,
    topic STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '/warehouse/acme/raw/social_media'
TBLPROPERTIES ('external.table.purge'='false');

INSERT INTO raw_social_media_mentions VALUES
('SM001', 'Twitter', '@manufacturing_pro', '2024-01-15 08:30:00', 'Just received our ACME industrial widgets - quality is outstanding!', 'POSITIVE', '245', '5600', 'Product Quality'),
('SM002', 'LinkedIn', 'Sarah Johnson', '2024-01-15 10:15:00', 'ACME widgets have reduced our production downtime by 30%', 'POSITIVE', '187', '2300', 'ROI'),
('SM003', 'Twitter', '@tech_reviewer', '2024-01-15 12:00:00', 'ACME customer service response time needs improvement', 'NEGATIVE', '89', '12000', 'Customer Service'),
('SM004', 'Facebook', 'Mike Chen', '2024-01-15 14:30:00', 'Best widget supplier in the industry. Highly recommend!', 'POSITIVE', '156', '890', 'Recommendation'),
('SM005', 'LinkedIn', 'Robert Taylor', '2024-01-15 16:45:00', 'ACME new premium widget line is a game changer for aerospace applications', 'POSITIVE', '312', '8900', 'Product Innovation'),
('SM006', 'Twitter', '@supply_chain_news', '2024-01-15 18:20:00', 'ACME expands distribution network to serve customers better', 'NEUTRAL', '445', '45000', 'Business Expansion');

-- ---------------------------------------------------------------------------------------------------------------------
-- TABLE: raw_weather_data
-- Source: Weather API feed
-- Purpose: Weather conditions affecting logistics and shipping
-- ---------------------------------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS raw_weather_data (
    observation_date STRING,
    location_code STRING,
    location_name STRING,
    temperature_f STRING,
    conditions STRING,
    wind_speed_mph STRING,
    precipitation_in STRING,
    visibility_mi STRING,
    impact_level STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '/warehouse/acme/raw/weather_data'
TBLPROPERTIES ('external.table.purge'='false');

INSERT INTO raw_weather_data VALUES
('2024-01-15', 'DC001', 'Chicago Distribution Center', '28', 'Snow', '15', '2.5', '0.5', 'HIGH'),
('2024-01-15', 'DC002', 'Dallas Distribution Center', '55', 'Clear', '8', '0.0', '10.0', 'NONE'),
('2024-01-15', 'DC003', 'Atlanta Distribution Center', '62', 'Partly Cloudy', '5', '0.0', '10.0', 'NONE'),
('2024-01-15', 'DC004', 'Los Angeles Distribution Center', '68', 'Sunny', '3', '0.0', '10.0', 'NONE'),
('2024-01-15', 'DC005', 'Newark Distribution Center', '32', 'Rain', '12', '0.8', '3.0', 'MEDIUM'),
('2024-01-16', 'DC001', 'Chicago Distribution Center', '25', 'Snow', '20', '3.2', '0.3', 'HIGH'),
('2024-01-16', 'DC002', 'Dallas Distribution Center', '58', 'Clear', '6', '0.0', '10.0', 'NONE'),
('2024-01-16', 'DC003', 'Atlanta Distribution Center', '65', 'Sunny', '4', '0.0', '10.0', 'NONE');


-- =====================================================================================================================
-- DATABASE 2: OPERATIONAL/TRANSACTIONAL (acme_ops)
-- Purpose: Track day-to-day business operations
-- Table Type: ACID tables (supports INSERT, UPDATE, DELETE)
-- Format: ORC (Optimized Row Columnar for fast reads and writes)
-- =====================================================================================================================

CREATE DATABASE IF NOT EXISTS acme_ops
    COMMENT 'Operational database for transactional business processes'
    LOCATION '/warehouse/acme/ops';

USE acme_ops;

-- ---------------------------------------------------------------------------------------------------------------------
-- TABLE: customers
-- Purpose: Master customer data (updated from raw feed)
-- ---------------------------------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS customers (
    customer_id STRING,
    company_name STRING,
    contact_name STRING,
    email STRING,
    phone STRING,
    address STRING,
    city STRING,
    state STRING,
    zip_code STRING,
    industry STRING,
    annual_revenue DECIMAL(15,2),
    employee_count INT,
    credit_limit DECIMAL(12,2),
    current_balance DECIMAL(12,2),
    signup_date DATE,
    status STRING,
    last_updated TIMESTAMP
)
CLUSTERED BY (customer_id) INTO 8 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true', 'orc.compress'='SNAPPY');

INSERT INTO customers VALUES
('C001', 'TechCorp Industries', 'John Smith', 'j.smith@techcorp.com', '555-0101', '123 Tech Blvd', 'San Francisco', 'CA', '94105', 'Technology', 50000000.00, 500, 100000.00, 45230.50, '2020-01-15', 'ACTIVE', current_timestamp()),
('C002', 'Manufacturing Solutions LLC', 'Sarah Johnson', 's.johnson@mfgsol.com', '555-0102', '456 Factory Ln', 'Detroit', 'MI', '48201', 'Manufacturing', 25000000.00, 250, 75000.00, 32100.75, '2019-03-22', 'ACTIVE', current_timestamp()),
('C003', 'AutoParts Direct', 'Mike Chen', 'm.chen@autoparts.com', '555-0103', '789 Auto Way', 'Cleveland', 'OH', '44114', 'Automotive', 75000000.00, 800, 150000.00, 89450.00, '2018-06-10', 'ACTIVE', current_timestamp()),
('C004', 'BuildRight Construction', 'Emily Davis', 'e.davis@buildright.com', '555-0104', '321 Builder St', 'Houston', 'TX', '77002', 'Construction', 35000000.00, 400, 90000.00, 23670.25, '2020-08-05', 'ACTIVE', current_timestamp()),
('C005', 'AeroSpace Dynamics', 'Robert Taylor', 'r.taylor@aerodyn.com', '555-0105', '654 Aerospace Dr', 'Seattle', 'WA', '98101', 'Aerospace', 120000000.00, 1200, 200000.00, 156780.00, '2017-11-30', 'ACTIVE', current_timestamp()),
('C006', 'MedDevice Corp', 'Lisa Anderson', 'l.anderson@meddevice.com', '555-0106', '987 Medical Plaza', 'Boston', 'MA', '02101', 'Medical', 45000000.00, 350, 110000.00, 67890.50, '2019-02-14', 'ACTIVE', current_timestamp()),
('C007', 'Energy Solutions Inc', 'David Martinez', 'd.martinez@energysol.com', '555-0107', '147 Power Grid Rd', 'Denver', 'CO', '80202', 'Energy', 65000000.00, 550, 130000.00, 98765.00, '2018-09-20', 'ACTIVE', current_timestamp()),
('C008', 'Food Processing Co', 'Jennifer Wilson', 'j.wilson@foodproc.com', '555-0108', '258 Industry Pkwy', 'Chicago', 'IL', '60601', 'Food', 28000000.00, 300, 70000.00, 34560.00, '2020-04-12', 'ACTIVE', current_timestamp()),
('C009', 'Robotics International', 'James Lee', 'j.lee@robotics.com', '555-0109', '369 Innovation Way', 'Austin', 'TX', '78701', 'Technology', 95000000.00, 700, 175000.00, 123450.75, '2019-05-08', 'ACTIVE', current_timestamp()),
('C010', 'Marine Equipment Ltd', 'Amanda Brown', 'a.brown@marine.com', '555-0110', '741 Harbor View', 'Portland', 'OR', '97201', 'Marine', 32000000.00, 280, 80000.00, 45670.00, '2019-07-18', 'ACTIVE', current_timestamp());

-- ---------------------------------------------------------------------------------------------------------------------
-- TABLE: products
-- Purpose: Widget product catalog
-- ---------------------------------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS products (
    product_id STRING,
    product_name STRING,
    category STRING,
    description STRING,
    unit_price DECIMAL(10,2),
    cost_price DECIMAL(10,2),
    weight_lbs DECIMAL(8,2),
    dimensions STRING,
    material_type STRING,
    min_order_qty INT,
    lead_time_days INT,
    status STRING,
    created_date DATE,
    last_updated TIMESTAMP
)
CLUSTERED BY (product_id) INTO 8 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true', 'orc.compress'='SNAPPY');

INSERT INTO products VALUES
('W001', 'Standard Widget Model A', 'Standard', 'General purpose widget for light industrial use', 25.00, 12.50, 2.5, '6x4x2 inches', 'Carbon Steel', 100, 5, 'ACTIVE', '2020-01-01', current_timestamp()),
('W002', 'Standard Widget Model B', 'Standard', 'Enhanced standard widget with improved durability', 32.00, 16.00, 3.0, '6x4x2 inches', 'Carbon Steel', 100, 5, 'ACTIVE', '2020-01-01', current_timestamp()),
('W003', 'Premium Widget Pro', 'Premium', 'High-performance widget for demanding applications', 75.00, 35.00, 4.5, '8x6x3 inches', 'Stainless Steel', 50, 7, 'ACTIVE', '2020-06-01', current_timestamp()),
('W004', 'Premium Widget Elite', 'Premium', 'Top-tier widget with advanced features', 125.00, 58.00, 6.0, '10x8x4 inches', 'Stainless Steel', 25, 10, 'ACTIVE', '2021-01-01', current_timestamp()),
('W005', 'Industrial Widget Heavy Duty', 'Industrial', 'Reinforced widget for heavy industrial applications', 95.00, 45.00, 8.5, '12x10x5 inches', 'Alloy Steel', 50, 14, 'ACTIVE', '2020-03-01', current_timestamp()),
('W006', 'Industrial Widget Extreme', 'Industrial', 'Extra heavy duty widget for extreme conditions', 150.00, 70.00, 12.0, '14x12x6 inches', 'Titanium Alloy', 25, 21, 'ACTIVE', '2021-06-01', current_timestamp()),
('W007', 'Custom Widget Configurable', 'Custom', 'Made-to-order widget with custom specifications', 200.00, 90.00, 5.0, 'Variable', 'Customer Choice', 10, 30, 'ACTIVE', '2020-09-01', current_timestamp()),
('W008', 'Eco Widget Green', 'Standard', 'Environmentally friendly widget from recycled materials', 28.00, 14.00, 2.8, '6x4x2 inches', 'Recycled Steel', 100, 7, 'ACTIVE', '2021-03-01', current_timestamp()),
('W009', 'Compact Widget Mini', 'Standard', 'Space-saving widget for confined installations', 22.00, 11.00, 1.5, '4x3x1.5 inches', 'Aluminum', 200, 3, 'ACTIVE', '2021-09-01', current_timestamp()),
('W010', 'Smart Widget IoT', 'Premium', 'Internet-connected widget with remote monitoring', 185.00, 85.00, 5.5, '8x6x3 inches', 'Stainless Steel', 25, 14, 'ACTIVE', '2022-01-01', current_timestamp());

-- ---------------------------------------------------------------------------------------------------------------------
-- TABLE: suppliers
-- Purpose: Supplier master data
-- ---------------------------------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS suppliers (
    supplier_id STRING,
    supplier_name STRING,
    contact_person STRING,
    email STRING,
    phone STRING,
    address STRING,
    city STRING,
    state STRING,
    country STRING,
    material_type STRING,
    rating INT,
    payment_terms STRING,
    active_since DATE,
    total_purchases DECIMAL(15,2),
    status STRING,
    last_updated TIMESTAMP
)
CLUSTERED BY (supplier_id) INTO 4 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true', 'orc.compress'='SNAPPY');

INSERT INTO suppliers VALUES
('S001', 'SteelWorks America', 'George Miller', 'g.miller@steelworks.com', '555-2001', '1000 Steel Mill Rd', 'Pittsburgh', 'PA', 'USA', 'Steel', 5, 'Net 30', '2015-01-10', 2500000.00, 'ACTIVE', current_timestamp()),
('S002', 'Precision Bearings Ltd', 'Helen Carter', 'h.carter@precbear.com', '555-2002', '2000 Bearing Way', 'Cleveland', 'OH', 'USA', 'Bearings', 5, 'Net 45', '2016-03-15', 1800000.00, 'ACTIVE', current_timestamp()),
('S003', 'Polymer Solutions Inc', 'Frank Adams', 'f.adams@polymer.com', '555-2003', '3000 Plastic Ave', 'Houston', 'TX', 'USA', 'Plastics', 4, 'Net 30', '2015-08-22', 950000.00, 'ACTIVE', current_timestamp()),
('S004', 'Electronics Components Co', 'Nancy Turner', 'n.turner@eleccomp.com', '555-2004', '4000 Circuit Blvd', 'San Jose', 'CA', 'USA', 'Electronics', 5, 'Net 60', '2014-11-05', 1200000.00, 'ACTIVE', current_timestamp()),
('S005', 'FastenRight Supplies', 'Kevin Phillips', 'k.phillips@fastenright.com', '555-2005', '5000 Bolt Street', 'Milwaukee', 'WI', 'USA', 'Fasteners', 4, 'Net 30', '2016-06-30', 450000.00, 'ACTIVE', current_timestamp()),
('S006', 'Coating Masters', 'Barbara Campbell', 'b.campbell@coating.com', '555-2006', '6000 Paint Dr', 'Louisville', 'KY', 'USA', 'Coatings', 4, 'Net 30', '2015-09-12', 680000.00, 'ACTIVE', current_timestamp()),
('S007', 'Rubber & Gaskets LLC', 'Richard Parker', 'r.parker@rubber.com', '555-2007', '7000 Gasket Ln', 'Akron', 'OH', 'USA', 'Rubber', 5, 'Net 45', '2014-04-18', 320000.00, 'ACTIVE', current_timestamp()),
('S008', 'Aluminum Alloys Inc', 'Susan Evans', 's.evans@aluminum.com', '555-2008', '8000 Metal Way', 'Seattle', 'WA', 'USA', 'Aluminum', 5, 'Net 30', '2015-12-01', 890000.00, 'ACTIVE', current_timestamp());

-- ---------------------------------------------------------------------------------------------------------------------
-- TABLE: inventory (TRANSACTIONAL)
-- Purpose: Track current inventory levels across warehouses
-- Note: This table demonstrates UPDATE operations
-- ---------------------------------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS inventory (
    inventory_id STRING,
    product_id STRING,
    warehouse_code STRING,
    warehouse_name STRING,
    quantity_on_hand INT,
    quantity_allocated INT,
    quantity_available INT,
    reorder_point INT,
    reorder_quantity INT,
    last_received_date DATE,
    last_shipped_date DATE,
    last_counted_date DATE,
    last_updated TIMESTAMP
)
CLUSTERED BY (product_id) INTO 8 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true', 'orc.compress'='SNAPPY');

-- Initial inventory load
INSERT INTO inventory VALUES
('INV001', 'W001', 'WH001', 'Chicago Main Warehouse', 5000, 800, 4200, 1000, 3000, '2024-01-10', '2024-01-14', '2024-01-01', current_timestamp()),
('INV002', 'W001', 'WH002', 'Dallas Warehouse', 3500, 450, 3050, 800, 2000, '2024-01-12', '2024-01-15', '2024-01-01', current_timestamp()),
('INV003', 'W002', 'WH001', 'Chicago Main Warehouse', 4200, 650, 3550, 900, 2500, '2024-01-11', '2024-01-14', '2024-01-01', current_timestamp()),
('INV004', 'W002', 'WH003', 'Atlanta Warehouse', 2800, 320, 2480, 600, 1500, '2024-01-09', '2024-01-13', '2024-01-01', current_timestamp()),
('INV005', 'W003', 'WH001', 'Chicago Main Warehouse', 1800, 280, 1520, 400, 1000, '2024-01-13', '2024-01-15', '2024-01-01', current_timestamp()),
('INV006', 'W003', 'WH004', 'Los Angeles Warehouse', 1500, 200, 1300, 350, 800, '2024-01-10', '2024-01-14', '2024-01-01', current_timestamp()),
('INV007', 'W004', 'WH001', 'Chicago Main Warehouse', 950, 180, 770, 200, 500, '2024-01-14', '2024-01-15', '2024-01-01', current_timestamp()),
('INV008', 'W005', 'WH001', 'Chicago Main Warehouse', 1200, 240, 960, 300, 700, '2024-01-12', '2024-01-15', '2024-01-01', current_timestamp()),
('INV009', 'W005', 'WH002', 'Dallas Warehouse', 980, 150, 830, 250, 600, '2024-01-11', '2024-01-14', '2024-01-01', current_timestamp()),
('INV010', 'W006', 'WH001', 'Chicago Main Warehouse', 650, 95, 555, 150, 400, '2024-01-13', '2024-01-14', '2024-01-01', current_timestamp()),
('INV011', 'W007', 'WH001', 'Chicago Main Warehouse', 280, 45, 235, 50, 150, '2024-01-09', '2024-01-12', '2024-01-01', current_timestamp()),
('INV012', 'W008', 'WH003', 'Atlanta Warehouse', 3200, 480, 2720, 700, 1800, '2024-01-10', '2024-01-15', '2024-01-01', current_timestamp()),
('INV013', 'W009', 'WH001', 'Chicago Main Warehouse', 6500, 1200, 5300, 1500, 4000, '2024-01-14', '2024-01-15', '2024-01-01', current_timestamp()),
('INV014', 'W009', 'WH005', 'Newark Warehouse', 4800, 850, 3950, 1000, 3000, '2024-01-13', '2024-01-14', '2024-01-01', current_timestamp()),
('INV015', 'W010', 'WH001', 'Chicago Main Warehouse', 420, 75, 345, 100, 300, '2024-01-11', '2024-01-13', '2024-01-01', current_timestamp());

-- Transaction 1: Shipment reduces inventory
UPDATE inventory SET
    quantity_on_hand = quantity_on_hand - 500,
    quantity_available = quantity_available - 500,
    last_shipped_date = '2024-01-16',
    last_updated = current_timestamp()
WHERE inventory_id = 'INV001';

-- Transaction 2: New stock received
UPDATE inventory SET
    quantity_on_hand = quantity_on_hand + 2000,
    quantity_available = quantity_available + 2000,
    last_received_date = '2024-01-16',
    last_updated = current_timestamp()
WHERE inventory_id = 'INV002';

-- Transaction 3: Allocate inventory for pending orders
UPDATE inventory SET
    quantity_allocated = quantity_allocated + 300,
    quantity_available = quantity_available - 300,
    last_updated = current_timestamp()
WHERE inventory_id = 'INV005';

-- Transaction 4: Release allocated inventory (order cancelled)
UPDATE inventory SET
    quantity_allocated = quantity_allocated - 100,
    quantity_available = quantity_available + 100,
    last_updated = current_timestamp()
WHERE inventory_id = 'INV007';

-- Transaction 5: Physical count adjustment
UPDATE inventory SET
    quantity_on_hand = 6450,
    quantity_available = 5250,
    last_counted_date = '2024-01-16',
    last_updated = current_timestamp()
WHERE inventory_id = 'INV013';

-- ---------------------------------------------------------------------------------------------------------------------
-- TABLE: orders (TRANSACTIONAL)
-- Purpose: Customer orders
-- ---------------------------------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS orders (
    order_id STRING,
    customer_id STRING,
    order_date DATE,
    requested_delivery_date DATE,
    order_status STRING,
    subtotal DECIMAL(12,2),
    tax_amount DECIMAL(10,2),
    shipping_cost DECIMAL(8,2),
    total_amount DECIMAL(12,2),
    payment_terms STRING,
    sales_rep STRING,
    warehouse_code STRING,
    special_instructions STRING,
    created_timestamp TIMESTAMP,
    last_updated TIMESTAMP
)
CLUSTERED BY (customer_id) INTO 8 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true', 'orc.compress'='SNAPPY');

-- Initial orders
INSERT INTO orders VALUES
('ORD001', 'C001', '2024-01-05', '2024-01-12', 'SHIPPED', 12500.00, 1000.00, 250.00, 13750.00, 'Net 30', 'REP001', 'WH001', NULL, current_timestamp(), current_timestamp()),
('ORD002', 'C002', '2024-01-06', '2024-01-15', 'SHIPPED', 8960.00, 716.80, 180.00, 9856.80, 'Net 30', 'REP002', 'WH002', NULL, current_timestamp(), current_timestamp()),
('ORD003', 'C003', '2024-01-08', '2024-01-18', 'IN_TRANSIT', 22500.00, 1800.00, 450.00, 24750.00, 'Net 45', 'REP001', 'WH001', 'Deliver to loading dock B', current_timestamp(), current_timestamp()),
('ORD004', 'C004', '2024-01-10', '2024-01-20', 'PROCESSING', 15750.00, 1260.00, 315.00, 17325.00, 'Net 30', 'REP003', 'WH003', NULL, current_timestamp(), current_timestamp()),
('ORD005', 'C005', '2024-01-11', '2024-01-25', 'PROCESSING', 37500.00, 3000.00, 500.00, 41000.00, 'Net 60', 'REP004', 'WH001', 'Aerospace certified packaging required', current_timestamp(), current_timestamp()),
('ORD006', 'C006', '2024-01-12', '2024-01-22', 'ALLOCATED', 18750.00, 1500.00, 375.00, 20625.00, 'Net 45', 'REP002', 'WH001', 'Medical grade documentation needed', current_timestamp(), current_timestamp()),
('ORD007', 'C007', '2024-01-13', '2024-01-27', 'ALLOCATED', 28500.00, 2280.00, 450.00, 31230.00, 'Net 30', 'REP003', 'WH002', NULL, current_timestamp(), current_timestamp()),
('ORD008', 'C008', '2024-01-14', '2024-01-24', 'PENDING', 9600.00, 768.00, 200.00, 10568.00, 'Net 30', 'REP001', 'WH003', NULL, current_timestamp(), current_timestamp()),
('ORD009', 'C009', '2024-01-15', '2024-01-29', 'PENDING', 46250.00, 3700.00, 600.00, 50550.00, 'Net 45', 'REP004', 'WH001', 'Robotics integration team will inspect on delivery', current_timestamp(), current_timestamp()),
('ORD010', 'C010', '2024-01-15', '2024-01-25', 'PENDING', 14250.00, 1140.00, 285.00, 15675.00, 'Net 30', 'REP002', 'WH004', 'Marine grade corrosion protection', current_timestamp(), current_timestamp());

-- Transaction 1: Order status updated to shipped
UPDATE orders SET
    order_status = 'SHIPPED',
    last_updated = current_timestamp()
WHERE order_id = 'ORD003';

-- Transaction 2: Order allocated and ready to ship
UPDATE orders SET
    order_status = 'READY_TO_SHIP',
    last_updated = current_timestamp()
WHERE order_id = 'ORD006';

-- Transaction 3: Order approved and moved to processing
UPDATE orders SET
    order_status = 'PROCESSING',
    last_updated = current_timestamp()
WHERE order_id = 'ORD008';

-- Transaction 4: Rushed order - updated delivery date
UPDATE orders SET
    requested_delivery_date = '2024-01-20',
    shipping_cost = 450.00,
    total_amount = total_amount - shipping_cost + 450.00,
    last_updated = current_timestamp()
WHERE order_id = 'ORD009';

-- Transaction 5: Order cancelled by customer
UPDATE orders SET
    order_status = 'CANCELLED',
    last_updated = current_timestamp()
WHERE order_id = 'ORD010';

-- ---------------------------------------------------------------------------------------------------------------------
-- TABLE: order_items (TRANSACTIONAL)
-- Purpose: Line items for each order
-- ---------------------------------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS order_items (
    order_item_id STRING,
    order_id STRING,
    product_id STRING,
    quantity INT,
    unit_price DECIMAL(10,2),
    discount_percent DECIMAL(5,2),
    discount_amount DECIMAL(10,2),
    line_total DECIMAL(12,2),
    status STRING,
    created_timestamp TIMESTAMP
)
CLUSTERED BY (order_id) INTO 8 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true', 'orc.compress'='SNAPPY');

INSERT INTO order_items VALUES
('OI001', 'ORD001', 'W001', 500, 25.00, 0.00, 0.00, 12500.00, 'SHIPPED', current_timestamp()),
('OI002', 'ORD002', 'W002', 280, 32.00, 0.00, 0.00, 8960.00, 'SHIPPED', current_timestamp()),
('OI003', 'ORD003', 'W003', 300, 75.00, 0.00, 0.00, 22500.00, 'SHIPPED', current_timestamp()),
('OI004', 'ORD004', 'W005', 150, 95.00, 10.00, 1425.00, 12825.00, 'PROCESSING', current_timestamp()),
('OI005', 'ORD004', 'W001', 100, 25.00, 15.00, 375.00, 2125.00, 'PROCESSING', current_timestamp()),
('OI006', 'ORD005', 'W004', 300, 125.00, 0.00, 0.00, 37500.00, 'PROCESSING', current_timestamp()),
('OI007', 'ORD006', 'W003', 250, 75.00, 0.00, 0.00, 18750.00, 'ALLOCATED', current_timestamp()),
('OI008', 'ORD007', 'W005', 300, 95.00, 0.00, 0.00, 28500.00, 'ALLOCATED', current_timestamp()),
('OI009', 'ORD008', 'W002', 300, 32.00, 0.00, 0.00, 9600.00, 'PENDING', current_timestamp()),
('OI010', 'ORD009', 'W010', 250, 185.00, 0.00, 0.00, 46250.00, 'PENDING', current_timestamp()),
('OI011', 'ORD010', 'W005', 150, 95.00, 0.00, 0.00, 14250.00, 'CANCELLED', current_timestamp());

-- Transaction: Update item status when order ships
UPDATE order_items SET
    status = 'SHIPPED'
WHERE order_id = 'ORD003';

-- Transaction: Update item status when allocated
UPDATE order_items SET
    status = 'READY_TO_SHIP'
WHERE order_id = 'ORD006';

-- Transaction: Cancel line item
UPDATE order_items SET
    status = 'CANCELLED'
WHERE order_id = 'ORD010';

-- ---------------------------------------------------------------------------------------------------------------------
-- TABLE: shipments (TRANSACTIONAL)
-- Purpose: Shipping and delivery tracking
-- ---------------------------------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS shipments (
    shipment_id STRING,
    order_id STRING,
    customer_id STRING,
    ship_date DATE,
    estimated_delivery_date DATE,
    actual_delivery_date DATE,
    carrier STRING,
    tracking_number STRING,
    shipment_status STRING,
    warehouse_code STRING,
    ship_to_address STRING,
    ship_to_city STRING,
    ship_to_state STRING,
    ship_to_zip STRING,
    num_packages INT,
    total_weight_lbs DECIMAL(10,2),
    freight_cost DECIMAL(8,2),
    created_timestamp TIMESTAMP,
    last_updated TIMESTAMP
)
CLUSTERED BY (order_id) INTO 8 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true', 'orc.compress'='SNAPPY');

INSERT INTO shipments VALUES
('SHP001', 'ORD001', 'C001', '2024-01-10', '2024-01-12', '2024-01-12', 'FedEx', '1Z9999999999999991', 'DELIVERED', 'WH001', '123 Tech Blvd', 'San Francisco', 'CA', '94105', 5, 1250.00, 250.00, current_timestamp(), current_timestamp()),
('SHP002', 'ORD002', 'C002', '2024-01-12', '2024-01-15', '2024-01-14', 'UPS', '1Z9999999999999992', 'DELIVERED', 'WH002', '456 Factory Ln', 'Detroit', 'MI', '48201', 3, 840.00, 180.00, current_timestamp(), current_timestamp()),
('SHP003', 'ORD003', 'C003', '2024-01-14', '2024-01-18', NULL, 'XPO Logistics', 'XPO123456789', 'IN_TRANSIT', 'WH001', '789 Auto Way', 'Cleveland', 'OH', '44114', 6, 1350.00, 450.00, current_timestamp(), current_timestamp()),
('SHP004', 'ORD004', 'C004', '2024-01-15', '2024-01-20', NULL, 'FedEx', '1Z9999999999999993', 'PICKED_UP', 'WH003', '321 Builder St', 'Houston', 'TX', '77002', 4, 1275.00, 315.00, current_timestamp(), current_timestamp()),
('SHP005', 'ORD006', 'C006', '2024-01-16', '2024-01-22', NULL, 'UPS', '1Z9999999999999994', 'LABEL_CREATED', 'WH001', '987 Medical Plaza', 'Boston', 'MA', '02101', 5, 1125.00, 375.00, current_timestamp(), current_timestamp());

-- Transaction 1: Shipment picked up by carrier
UPDATE shipments SET
    shipment_status = 'IN_TRANSIT',
    last_updated = current_timestamp()
WHERE shipment_id = 'SHP004';

-- Transaction 2: Shipment out for delivery
UPDATE shipments SET
    shipment_status = 'OUT_FOR_DELIVERY',
    last_updated = current_timestamp()
WHERE shipment_id = 'SHP003';

-- Transaction 3: Shipment delivered
UPDATE shipments SET
    shipment_status = 'DELIVERED',
    actual_delivery_date = '2024-01-17',
    last_updated = current_timestamp()
WHERE shipment_id = 'SHP003';

-- Transaction 4: Shipment ready to ship
UPDATE shipments SET
    shipment_status = 'READY_TO_SHIP',
    last_updated = current_timestamp()
WHERE shipment_id = 'SHP005';

-- Transaction 5: Package picked up
UPDATE shipments SET
    shipment_status = 'PICKED_UP',
    last_updated = current_timestamp()
WHERE shipment_id = 'SHP005';

-- ---------------------------------------------------------------------------------------------------------------------
-- TABLE: production_runs (TRANSACTIONAL)
-- Purpose: Manufacturing production tracking
-- ---------------------------------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS production_runs (
    production_run_id STRING,
    product_id STRING,
    planned_quantity INT,
    actual_quantity INT,
    scrap_quantity INT,
    run_status STRING,
    facility_code STRING,
    facility_name STRING,
    start_date DATE,
    end_date DATE,
    supervisor STRING,
    shift STRING,
    machine_hours DECIMAL(8,2),
    labor_hours DECIMAL(8,2),
    material_cost DECIMAL(12,2),
    labor_cost DECIMAL(10,2),
    overhead_cost DECIMAL(10,2),
    total_cost DECIMAL(12,2),
    cost_per_unit DECIMAL(10,2),
    quality_score INT,
    created_timestamp TIMESTAMP,
    last_updated TIMESTAMP
)
CLUSTERED BY (product_id) INTO 8 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true', 'orc.compress'='SNAPPY');

INSERT INTO production_runs VALUES
('PR001', 'W001', 5000, 4950, 50, 'COMPLETED', 'FAC001', 'Chicago Manufacturing', '2024-01-02', '2024-01-05', 'Tom Anderson', 'Day', 72.5, 320.0, 25000.00, 9600.00, 5400.00, 40000.00, 8.08, 99, current_timestamp(), current_timestamp()),
('PR002', 'W002', 3000, 2980, 20, 'COMPLETED', 'FAC001', 'Chicago Manufacturing', '2024-01-06', '2024-01-09', 'Tom Anderson', 'Day', 68.0, 280.0, 18000.00, 8400.00, 4800.00, 31200.00, 10.47, 99, current_timestamp(), current_timestamp()),
('PR003', 'W003', 2000, 1985, 15, 'COMPLETED', 'FAC002', 'Dallas Manufacturing', '2024-01-03', '2024-01-08', 'Maria Garcia', 'Day', 95.0, 380.0, 28000.00, 11400.00, 6500.00, 45900.00, 23.12, 99, current_timestamp(), current_timestamp()),
('PR004', 'W005', 1500, 1480, 20, 'COMPLETED', 'FAC003', 'Atlanta Manufacturing', '2024-01-05', '2024-01-12', 'John Williams', 'Day', 142.0, 480.0, 33750.00, 14400.00, 8200.00, 56350.00, 38.07, 99, current_timestamp(), current_timestamp()),
('PR005', 'W001', 4000, 3200, 0, 'IN_PROGRESS', 'FAC001', 'Chicago Manufacturing', '2024-01-13', NULL, 'Tom Anderson', 'Day', 48.0, 216.0, 16000.00, 6480.00, 3700.00, 26180.00, 8.18, NULL, current_timestamp(), current_timestamp()),
('PR006', 'W009', 6000, 5950, 50, 'COMPLETED', 'FAC002', 'Dallas Manufacturing', '2024-01-08', '2024-01-11', 'Maria Garcia', 'Night', 54.0, 260.0, 32500.00, 7800.00, 4400.00, 44700.00, 7.51, 99, current_timestamp(), current_timestamp()),
('PR007', 'W004', 800, 790, 10, 'COMPLETED', 'FAC001', 'Chicago Manufacturing', '2024-01-10', '2024-01-15', 'Sarah Johnson', 'Day', 108.0, 420.0, 23200.00, 12600.00, 7100.00, 42900.00, 54.30, 99, current_timestamp(), current_timestamp()),
('PR008', 'W008', 3500, 2800, 0, 'IN_PROGRESS', 'FAC003', 'Atlanta Manufacturing', '2024-01-14', NULL, 'John Williams', 'Day', 42.0, 196.0, 19600.00, 5880.00, 3300.00, 28780.00, 10.28, NULL, current_timestamp(), current_timestamp()),
('PR009', 'W010', 500, 485, 15, 'QUALITY_HOLD', 'FAC002', 'Dallas Manufacturing', '2024-01-11', '2024-01-16', 'Maria Garcia', 'Day', 128.0, 490.0, 21250.00, 14700.00, 8300.00, 44250.00, 91.24, 94, current_timestamp(), current_timestamp()),
('PR010', 'W006', 600, 0, 0, 'SCHEDULED', 'FAC003', 'Atlanta Manufacturing', '2024-01-18', NULL, 'John Williams', 'Day', 0.0, 0.0, 0.00, 0.00, 0.00, 0.00, 0.00, NULL, current_timestamp(), current_timestamp());

-- Transaction 1: Production run completed
UPDATE production_runs SET
    actual_quantity = 4000,
    run_status = 'COMPLETED',
    end_date = '2024-01-16',
    machine_hours = 72.0,
    labor_hours = 320.0,
    quality_score = 100,
    last_updated = current_timestamp()
WHERE production_run_id = 'PR005';

-- Transaction 2: Quality issue detected
UPDATE production_runs SET
    run_status = 'QUALITY_HOLD',
    quality_score = 94,
    last_updated = current_timestamp()
WHERE production_run_id = 'PR009';

-- Transaction 3: Production run started
UPDATE production_runs SET
    run_status = 'IN_PROGRESS',
    start_date = '2024-01-17',
    last_updated = current_timestamp()
WHERE production_run_id = 'PR010';

-- Transaction 4: Update in-progress production
UPDATE production_runs SET
    actual_quantity = 3400,
    machine_hours = 62.5,
    labor_hours = 298.0,
    last_updated = current_timestamp()
WHERE production_run_id = 'PR008';

-- Transaction 5: Quality hold released
UPDATE production_runs SET
    run_status = 'COMPLETED',
    last_updated = current_timestamp()
WHERE production_run_id = 'PR009';

-- ---------------------------------------------------------------------------------------------------------------------
-- TABLE: raw_materials (TRANSACTIONAL)
-- Purpose: Raw material inventory for manufacturing
-- ---------------------------------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_materials (
    material_id STRING,
    material_code STRING,
    material_name STRING,
    supplier_id STRING,
    unit_of_measure STRING,
    quantity_on_hand DECIMAL(12,2),
    unit_cost DECIMAL(8,2),
    total_value DECIMAL(15,2),
    reorder_point DECIMAL(10,2),
    reorder_quantity DECIMAL(10,2),
    warehouse_location STRING,
    last_received_date DATE,
    last_used_date DATE,
    status STRING,
    created_timestamp TIMESTAMP,
    last_updated TIMESTAMP
)
CLUSTERED BY (material_code) INTO 4 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true', 'orc.compress'='SNAPPY');

INSERT INTO raw_materials VALUES
('RM001', 'STL001', 'Carbon Steel Sheet', 'S001', 'LBS', 25000.00, 0.85, 21250.00, 5000.00, 15000.00, 'RAW-A1', '2024-01-10', '2024-01-15', 'ACTIVE', current_timestamp(), current_timestamp()),
('RM002', 'STL002', 'Stainless Steel 304', 'S001', 'LBS', 18000.00, 1.45, 26100.00, 4000.00, 12000.00, 'RAW-A2', '2024-01-12', '2024-01-14', 'ACTIVE', current_timestamp(), current_timestamp()),
('RM003', 'ALU001', 'Aluminum 6061', 'S008', 'LBS', 12000.00, 1.25, 15000.00, 3000.00, 10000.00, 'RAW-B1', '2024-01-11', '2024-01-13', 'ACTIVE', current_timestamp(), current_timestamp()),
('RM004', 'PLT001', 'ABS Plastic Pellets', 'S003', 'LBS', 8500.00, 0.95, 8075.00, 2000.00, 8000.00, 'RAW-C1', '2024-01-09', '2024-01-12', 'ACTIVE', current_timestamp(), current_timestamp()),
('RM005', 'BRG001', 'Steel Bearings 10mm', 'S002', 'UNIT', 15000.00, 2.50, 37500.00, 3000.00, 10000.00, 'COMP-D1', '2024-01-13', '2024-01-15', 'ACTIVE', current_timestamp(), current_timestamp()),
('RM006', 'CST001', 'Powder Coating', 'S006', 'LBS', 2800.00, 3.75, 10500.00, 500.00, 2000.00, 'FIN-E1', '2024-01-08', '2024-01-14', 'ACTIVE', current_timestamp(), current_timestamp()),
('RM007', 'FST001', 'Hex Bolts M6', 'S005', 'UNIT', 45000.00, 0.15, 6750.00, 10000.00, 30000.00, 'COMP-D2', '2024-01-14', '2024-01-15', 'ACTIVE', current_timestamp(), current_timestamp()),
('RM008', 'RUB001', 'Rubber Gaskets', 'S007', 'UNIT', 12000.00, 0.45, 5400.00, 2500.00, 8000.00, 'COMP-D3', '2024-01-10', '2024-01-13', 'ACTIVE', current_timestamp(), current_timestamp());

-- Transaction 1: Material consumed in production
UPDATE raw_materials SET
    quantity_on_hand = quantity_on_hand - 2500.00,
    total_value = quantity_on_hand * unit_cost,
    last_used_date = '2024-01-16',
    last_updated = current_timestamp()
WHERE material_id = 'RM001';

-- Transaction 2: New material received
UPDATE raw_materials SET
    quantity_on_hand = quantity_on_hand + 12000.00,
    total_value = quantity_on_hand * unit_cost,
    last_received_date = '2024-01-16',
    last_updated = current_timestamp()
WHERE material_id = 'RM002';

-- Transaction 3: Material consumed
UPDATE raw_materials SET
    quantity_on_hand = quantity_on_hand - 1800.00,
    total_value = quantity_on_hand * unit_cost,
    last_used_date = '2024-01-16',
    last_updated = current_timestamp()
WHERE material_id = 'RM005';

-- Transaction 4: Price update from supplier
UPDATE raw_materials SET
    unit_cost = 0.88,
    total_value = quantity_on_hand * 0.88,
    last_updated = current_timestamp()
WHERE material_id = 'RM001';

-- Transaction 5: Emergency reorder
UPDATE raw_materials SET
    status = 'REORDER_PENDING',
    last_updated = current_timestamp()
WHERE material_id = 'RM006' AND quantity_on_hand < reorder_point;


-- =====================================================================================================================
-- DATABASE 3: ANALYTICS/DATA WAREHOUSE (acme_analytics)
-- Purpose: Aggregated data for business intelligence and reporting
-- Table Type: EXTERNAL tables with PURGE (managed lifecycle but external location)
-- Format: PARQUET (highly compressed columnar format optimized for analytics)
-- =====================================================================================================================

CREATE DATABASE IF NOT EXISTS acme_analytics
COMMENT 'Analytics database for business intelligence and reporting'
LOCATION '/warehouse/acme/analytics';

USE acme_analytics;

-- ---------------------------------------------------------------------------------------------------------------------
-- TABLE: customer_360
-- Purpose: Comprehensive customer view combining CRM, orders, and engagement data
-- Source: Joins acme_raw.raw_customer_feed, acme_ops.customers, acme_ops.orders
-- ---------------------------------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS customer_360 (
    customer_id STRING,
    company_name STRING,
    contact_name STRING,
    email STRING,
    industry STRING,
    state STRING,
    customer_segment STRING,
    annual_revenue DECIMAL(15,2),
    employee_count INT,
    credit_limit DECIMAL(12,2),
    current_balance DECIMAL(12,2),
    available_credit DECIMAL(12,2),
    signup_date DATE,
    account_age_days INT,
    customer_status STRING,
    total_orders INT,
    total_order_value DECIMAL(15,2),
    avg_order_value DECIMAL(12,2),
    last_order_date DATE,
    days_since_last_order INT,
    lifetime_value DECIMAL(15,2),
    payment_history_score INT,
    engagement_score INT,
    risk_category STRING
)
STORED AS PARQUET
LOCATION '/warehouse/acme/analytics/customer_360'
TBLPROPERTIES ('external.table.purge'='true', 'parquet.compression'='SNAPPY');

INSERT INTO customer_360
SELECT
    c.customer_id,
    c.company_name,
    c.contact_name,
    c.email,
    c.industry,
    c.state,
    CASE
        WHEN c.annual_revenue >= 100000000 THEN 'Enterprise'
        WHEN c.annual_revenue >= 50000000 THEN 'Large'
        WHEN c.annual_revenue >= 20000000 THEN 'Medium'
        ELSE 'Small'
    END as customer_segment,
    c.annual_revenue,
    c.employee_count,
    c.credit_limit,
    c.current_balance,
    c.credit_limit - c.current_balance as available_credit,
    c.signup_date,
    datediff(current_date(), c.signup_date) as account_age_days,
    c.status as customer_status,
    COUNT(DISTINCT o.order_id) as total_orders,
    COALESCE(SUM(o.total_amount), 0) as total_order_value,
    COALESCE(AVG(o.total_amount), 0) as avg_order_value,
    MAX(o.order_date) as last_order_date,
    datediff(current_date(), MAX(o.order_date)) as days_since_last_order,
    COALESCE(SUM(o.total_amount), 0) as lifetime_value,
    CASE
        WHEN c.current_balance / c.credit_limit < 0.3 THEN 100
        WHEN c.current_balance / c.credit_limit < 0.6 THEN 80
        WHEN c.current_balance / c.credit_limit < 0.9 THEN 60
        ELSE 40
    END as payment_history_score,
    CASE
        WHEN COUNT(DISTINCT o.order_id) >= 5 THEN 100
        WHEN COUNT(DISTINCT o.order_id) >= 3 THEN 75
        WHEN COUNT(DISTINCT o.order_id) >= 1 THEN 50
        ELSE 25
    END as engagement_score,
    CASE
        WHEN c.current_balance / c.credit_limit > 0.9 THEN 'High Risk'
        WHEN c.current_balance / c.credit_limit > 0.7 THEN 'Medium Risk'
        ELSE 'Low Risk'
    END as risk_category
FROM acme_ops.customers c
LEFT JOIN acme_ops.orders o ON c.customer_id = o.customer_id
GROUP BY
    c.customer_id, c.company_name, c.contact_name, c.email, c.industry, c.state,
    c.annual_revenue, c.employee_count, c.credit_limit, c.current_balance, c.signup_date, c.status;

-- ---------------------------------------------------------------------------------------------------------------------
-- TABLE: sales_summary
-- Purpose: Daily/weekly/monthly sales aggregations
-- Source: acme_ops.orders, acme_ops.order_items, acme_ops.products
-- ---------------------------------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS sales_summary (
    summary_date DATE,
    summary_period STRING,
    product_id STRING,
    product_name STRING,
    product_category STRING,
    units_sold INT,
    gross_revenue DECIMAL(15,2),
    discount_amount DECIMAL(12,2),
    net_revenue DECIMAL(15,2),
    cost_of_goods DECIMAL(12,2),
    gross_profit DECIMAL(12,2),
    gross_margin_pct DECIMAL(5,2),
    num_orders INT,
    num_customers INT,
    avg_selling_price DECIMAL(10,2),
    avg_order_size INT
)
STORED AS PARQUET
LOCATION '/warehouse/acme/analytics/sales_summary'
TBLPROPERTIES ('external.table.purge'='true', 'parquet.compression'='SNAPPY');

INSERT INTO sales_summary
SELECT
    o.order_date as summary_date,
    'DAILY' as summary_period,
    p.product_id,
    p.product_name,
    p.category as product_category,
    SUM(oi.quantity) as units_sold,
    SUM(oi.quantity * oi.unit_price) as gross_revenue,
    SUM(oi.discount_amount) as discount_amount,
    SUM(oi.line_total) as net_revenue,
    SUM(oi.quantity * p.cost_price) as cost_of_goods,
    SUM(oi.line_total) - SUM(oi.quantity * p.cost_price) as gross_profit,
    ((SUM(oi.line_total) - SUM(oi.quantity * p.cost_price)) / SUM(oi.line_total)) * 100 as gross_margin_pct,
    COUNT(DISTINCT o.order_id) as num_orders,
    COUNT(DISTINCT o.customer_id) as num_customers,
    AVG(oi.unit_price) as avg_selling_price,
    AVG(oi.quantity) as avg_order_size
FROM acme_ops.orders o
JOIN acme_ops.order_items oi ON o.order_id = oi.order_id
JOIN acme_ops.products p ON oi.product_id = p.product_id
WHERE o.order_status != 'CANCELLED'
GROUP BY o.order_date, p.product_id, p.product_name, p.category;

-- ---------------------------------------------------------------------------------------------------------------------
-- TABLE: inventory_forecast
-- Purpose: Inventory levels, turnover rates, and reorder recommendations
-- Source: acme_ops.inventory, acme_ops.products, sales data
-- ---------------------------------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS inventory_forecast (
    product_id STRING,
    product_name STRING,
    category STRING,
    total_on_hand INT,
    total_allocated INT,
    total_available INT,
    num_warehouses INT,
    avg_daily_sales DECIMAL(10,2),
    days_of_supply DECIMAL(8,2),
    reorder_recommended BOOLEAN,
    recommended_order_qty INT,
    stockout_risk STRING,
    inventory_turnover_ratio DECIMAL(8,2),
    carrying_cost_monthly DECIMAL(12,2),
    last_received_date DATE,
    last_shipped_date DATE
)
STORED AS PARQUET
LOCATION '/warehouse/acme/analytics/inventory_forecast'
TBLPROPERTIES ('external.table.purge'='true', 'parquet.compression'='SNAPPY');

INSERT INTO inventory_forecast
SELECT
    p.product_id,
    p.product_name,
    p.category,
    SUM(i.quantity_on_hand) as total_on_hand,
    SUM(i.quantity_allocated) as total_allocated,
    SUM(i.quantity_available) as total_available,
    COUNT(DISTINCT i.warehouse_code) as num_warehouses,
    COALESCE(AVG(daily_sales.avg_qty), 0) as avg_daily_sales,
    CASE
        WHEN COALESCE(AVG(daily_sales.avg_qty), 0) > 0
        THEN SUM(i.quantity_available) / AVG(daily_sales.avg_qty)
        ELSE 999
    END as days_of_supply,
    CASE
        WHEN SUM(i.quantity_available) < MAX(i.reorder_point) THEN true
        ELSE false
    END as reorder_recommended,
    CASE
        WHEN SUM(i.quantity_available) < MAX(i.reorder_point)
        THEN MAX(i.reorder_quantity)
        ELSE 0
    END as recommended_order_qty,
    CASE
        WHEN SUM(i.quantity_available) / NULLIF(AVG(daily_sales.avg_qty), 0) < 7 THEN 'High'
        WHEN SUM(i.quantity_available) / NULLIF(AVG(daily_sales.avg_qty), 0) < 14 THEN 'Medium'
        ELSE 'Low'
    END as stockout_risk,
    CASE
        WHEN SUM(i.quantity_on_hand) > 0
        THEN (COALESCE(AVG(daily_sales.avg_qty), 0) * 30) / SUM(i.quantity_on_hand)
        ELSE 0
    END as inventory_turnover_ratio,
    SUM(i.quantity_on_hand) * p.cost_price * 0.02 as carrying_cost_monthly,
    MAX(i.last_received_date) as last_received_date,
    MAX(i.last_shipped_date) as last_shipped_date
FROM acme_ops.products p
LEFT JOIN acme_ops.inventory i ON p.product_id = i.product_id
LEFT JOIN (
    SELECT
        oi.product_id,
        AVG(oi.quantity) as avg_qty
    FROM acme_ops.order_items oi
    JOIN acme_ops.orders o ON oi.order_id = o.order_id
    WHERE o.order_status != 'CANCELLED'
    GROUP BY oi.product_id
) daily_sales ON p.product_id = daily_sales.product_id
GROUP BY p.product_id, p.product_name, p.category, p.cost_price;

-- ---------------------------------------------------------------------------------------------------------------------
-- TABLE: production_efficiency
-- Purpose: Manufacturing KPIs and efficiency metrics
-- Source: acme_ops.production_runs, acme_ops.raw_materials
-- ---------------------------------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS production_efficiency (
    product_id STRING,
    product_name STRING,
    facility_code STRING,
    facility_name STRING,
    total_runs INT,
    total_planned_qty INT,
    total_actual_qty INT,
    total_scrap_qty INT,
    yield_rate DECIMAL(5,2),
    avg_quality_score DECIMAL(5,2),
    total_machine_hours DECIMAL(10,2),
    total_labor_hours DECIMAL(10,2),
    total_production_cost DECIMAL(15,2),
    avg_cost_per_unit DECIMAL(10,2),
    units_per_machine_hour DECIMAL(8,2),
    units_per_labor_hour DECIMAL(8,2),
    on_time_completion_rate DECIMAL(5,2),
    efficiency_rating STRING
)
STORED AS PARQUET
LOCATION '/warehouse/acme/analytics/production_efficiency'
TBLPROPERTIES ('external.table.purge'='true', 'parquet.compression'='SNAPPY');

INSERT INTO production_efficiency
SELECT
    pr.product_id,
    p.product_name,
    pr.facility_code,
    pr.facility_name,
    COUNT(*) as total_runs,
    SUM(pr.planned_quantity) as total_planned_qty,
    SUM(pr.actual_quantity) as total_actual_qty,
    SUM(pr.scrap_quantity) as total_scrap_qty,
    (SUM(pr.actual_quantity) / NULLIF(SUM(pr.planned_quantity), 0)) * 100 as yield_rate,
    AVG(pr.quality_score) as avg_quality_score,
    SUM(pr.machine_hours) as total_machine_hours,
    SUM(pr.labor_hours) as total_labor_hours,
    SUM(pr.total_cost) as total_production_cost,
    AVG(pr.cost_per_unit) as avg_cost_per_unit,
    SUM(pr.actual_quantity) / NULLIF(SUM(pr.machine_hours), 0) as units_per_machine_hour,
    SUM(pr.actual_quantity) / NULLIF(SUM(pr.labor_hours), 0) as units_per_labor_hour,
    (SUM(CASE WHEN pr.run_status = 'COMPLETED' THEN 1 ELSE 0 END) / COUNT(*)) * 100 as on_time_completion_rate,
    CASE
        WHEN (SUM(pr.actual_quantity) / NULLIF(SUM(pr.planned_quantity), 0)) >= 0.98
             AND AVG(pr.quality_score) >= 98 THEN 'Excellent'
        WHEN (SUM(pr.actual_quantity) / NULLIF(SUM(pr.planned_quantity), 0)) >= 0.95
             AND AVG(pr.quality_score) >= 95 THEN 'Good'
        WHEN (SUM(pr.actual_quantity) / NULLIF(SUM(pr.planned_quantity), 0)) >= 0.90 THEN 'Fair'
        ELSE 'Needs Improvement'
    END as efficiency_rating
FROM acme_ops.production_runs pr
JOIN acme_ops.products p ON pr.product_id = p.product_id
WHERE pr.run_status IN ('COMPLETED', 'QUALITY_HOLD')
GROUP BY pr.product_id, p.product_name, pr.facility_code, pr.facility_name;

-- ---------------------------------------------------------------------------------------------------------------------
-- TABLE: customer_lifetime_value
-- Purpose: Customer segmentation and CLV analysis
-- Source: acme_ops.customers, acme_ops.orders, customer_360
-- ---------------------------------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS customer_lifetime_value (
    customer_id STRING,
    company_name STRING,
    industry STRING,
    customer_segment STRING,
    total_revenue DECIMAL(15,2),
    total_orders INT,
    avg_order_value DECIMAL(12,2),
    order_frequency DECIMAL(8,2),
    customer_tenure_months INT,
    predicted_next_order_days INT,
    churn_risk_score INT,
    retention_probability DECIMAL(5,2),
    customer_lifetime_value DECIMAL(15,2),
    clv_segment STRING,
    recommended_action STRING
)
STORED AS PARQUET
LOCATION '/warehouse/acme/analytics/customer_lifetime_value'
TBLPROPERTIES ('external.table.purge'='true', 'parquet.compression'='SNAPPY');

INSERT INTO customer_lifetime_value
SELECT
    c360.customer_id,
    c360.company_name,
    c360.industry,
    c360.customer_segment,
    c360.total_order_value as total_revenue,
    c360.total_orders,
    c360.avg_order_value,
    CASE
        WHEN c360.account_age_days > 0
        THEN (c360.total_orders / (c360.account_age_days / 30.0))
        ELSE 0
    END as order_frequency,
    CAST(c360.account_age_days / 30 AS INT) as customer_tenure_months,
    CASE
        WHEN c360.total_orders > 0
        THEN CAST((c360.account_age_days / c360.total_orders) AS INT)
        ELSE 90
    END as predicted_next_order_days,
    CASE
        WHEN c360.days_since_last_order > 90 THEN 80
        WHEN c360.days_since_last_order > 60 THEN 60
        WHEN c360.days_since_last_order > 30 THEN 40
        ELSE 20
    END as churn_risk_score,
    CASE
        WHEN c360.days_since_last_order <= 30 THEN 95.0
        WHEN c360.days_since_last_order <= 60 THEN 75.0
        WHEN c360.days_since_last_order <= 90 THEN 50.0
        ELSE 25.0
    END as retention_probability,
    c360.total_order_value *
    (CASE
        WHEN c360.days_since_last_order <= 30 THEN 1.5
        WHEN c360.days_since_last_order <= 60 THEN 1.2
        ELSE 1.0
    END) as customer_lifetime_value,
    CASE
        WHEN c360.total_order_value >= 100000 THEN 'Platinum'
        WHEN c360.total_order_value >= 50000 THEN 'Gold'
        WHEN c360.total_order_value >= 20000 THEN 'Silver'
        ELSE 'Bronze'
    END as clv_segment,
    CASE
        WHEN c360.days_since_last_order > 60 THEN 'Re-engagement Campaign'
        WHEN c360.total_orders = 1 THEN 'Onboarding Follow-up'
        WHEN c360.total_order_value >= 100000 THEN 'VIP Treatment'
        ELSE 'Standard Nurture'
    END as recommended_action
FROM acme_analytics.customer_360 c360;

-- ---------------------------------------------------------------------------------------------------------------------
-- TABLE: supply_chain_metrics
-- Purpose: End-to-end supply chain performance
-- Source: acme_ops.orders, acme_ops.shipments, acme_raw.raw_weather_data
-- ---------------------------------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS supply_chain_metrics (
    metric_date DATE,
    warehouse_code STRING,
    total_shipments INT,
    on_time_shipments INT,
    late_shipments INT,
    on_time_delivery_rate DECIMAL(5,2),
    avg_shipping_days DECIMAL(5,2),
    total_packages INT,
    total_weight_lbs DECIMAL(12,2),
    total_freight_cost DECIMAL(12,2),
    cost_per_shipment DECIMAL(8,2),
    cost_per_pound DECIMAL(6,2),
    weather_delays INT,
    carrier_performance_score INT,
    supply_chain_efficiency STRING
)
STORED AS PARQUET
LOCATION '/warehouse/acme/analytics/supply_chain_metrics'
TBLPROPERTIES ('external.table.purge'='true', 'parquet.compression'='SNAPPY');

INSERT INTO supply_chain_metrics
SELECT
    s.ship_date as metric_date,
    s.warehouse_code,
    COUNT(*) as total_shipments,
    SUM(CASE
        WHEN s.actual_delivery_date <= s.estimated_delivery_date THEN 1
        ELSE 0
    END) as on_time_shipments,
    SUM(CASE
        WHEN s.actual_delivery_date > s.estimated_delivery_date THEN 1
        ELSE 0
    END) as late_shipments,
    (SUM(CASE
        WHEN s.actual_delivery_date <= s.estimated_delivery_date THEN 1
        ELSE 0
    END) / COUNT(*)) * 100 as on_time_delivery_rate,
    AVG(datediff(s.actual_delivery_date, s.ship_date)) as avg_shipping_days,
    SUM(s.num_packages) as total_packages,
    SUM(s.total_weight_lbs) as total_weight_lbs,
    SUM(s.freight_cost) as total_freight_cost,
    AVG(s.freight_cost) as cost_per_shipment,
    SUM(s.freight_cost) / NULLIF(SUM(s.total_weight_lbs), 0) as cost_per_pound,
    0 as weather_delays,
    CAST((SUM(CASE
        WHEN s.actual_delivery_date <= s.estimated_delivery_date THEN 1
        ELSE 0
    END) / COUNT(*)) * 100 AS INT) as carrier_performance_score,
    CASE
        WHEN (SUM(CASE
            WHEN s.actual_delivery_date <= s.estimated_delivery_date THEN 1
            ELSE 0
        END) / COUNT(*)) >= 0.95 THEN 'Excellent'
        WHEN (SUM(CASE
            WHEN s.actual_delivery_date <= s.estimated_delivery_date THEN 1
            ELSE 0
        END) / COUNT(*)) >= 0.85 THEN 'Good'
        WHEN (SUM(CASE
            WHEN s.actual_delivery_date <= s.estimated_delivery_date THEN 1
            ELSE 0
        END) / COUNT(*)) >= 0.75 THEN 'Fair'
        ELSE 'Needs Improvement'
    END as supply_chain_efficiency
FROM acme_ops.shipments s
WHERE s.actual_delivery_date IS NOT NULL
GROUP BY s.ship_date, s.warehouse_code;

-- ---------------------------------------------------------------------------------------------------------------------
-- TABLE: marketing_roi
-- Purpose: Marketing campaign effectiveness and ROI
-- Source: acme_raw.raw_web_clickstream, acme_raw.raw_social_media_mentions, acme_ops.orders
-- ---------------------------------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_roi (
    analysis_date DATE,
    channel STRING,
    total_sessions INT,
    unique_visitors INT,
    total_page_views INT,
    avg_session_duration DECIMAL(8,2),
    conversion_count INT,
    conversion_rate DECIMAL(5,2),
    attributed_revenue DECIMAL(15,2),
    cost_per_acquisition DECIMAL(10,2),
    return_on_ad_spend DECIMAL(8,2),
    channel_effectiveness STRING
)
STORED AS PARQUET
LOCATION '/warehouse/acme/analytics/marketing_roi'
TBLPROPERTIES ('external.table.purge'='true', 'parquet.compression'='SNAPPY');

INSERT INTO marketing_roi
SELECT
    CAST(SUBSTR(wc.`timestamp`, 1, 10) AS DATE) as analysis_date,
    CASE
        WHEN wc.referrer LIKE '%google%' THEN 'Google Search'
        WHEN wc.referrer LIKE '%bing%' THEN 'Bing Search'
        WHEN wc.referrer LIKE '%linkedin%' THEN 'LinkedIn'
        WHEN wc.referrer LIKE '%facebook%' THEN 'Facebook'
        WHEN wc.referrer = 'direct' THEN 'Direct'
        ELSE 'Other'
    END as channel,
    COUNT(*) as total_sessions,
    COUNT(DISTINCT wc.visitor_id) as unique_visitors,
    SUM(CAST(wc.pages_viewed AS INT)) as total_page_views,
    AVG(CAST(wc.session_duration AS DECIMAL(8,2))) as avg_session_duration,
    SUM(CASE WHEN wc.conversion_flag = 'true' THEN 1 ELSE 0 END) as conversion_count,
    (SUM(CASE WHEN wc.conversion_flag = 'true' THEN 1 ELSE 0 END) / COUNT(*)) * 100 as conversion_rate,
    SUM(CASE WHEN wc.conversion_flag = 'true' THEN 15000 ELSE 0 END) as attributed_revenue,
    CASE
        WHEN SUM(CASE WHEN wc.conversion_flag = 'true' THEN 1 ELSE 0 END) > 0
        THEN 500.00 / SUM(CASE WHEN wc.conversion_flag = 'true' THEN 1 ELSE 0 END)
        ELSE 0
    END as cost_per_acquisition,
    CASE
        WHEN SUM(CASE WHEN wc.conversion_flag = 'true' THEN 1 ELSE 0 END) > 0
        THEN (SUM(CASE WHEN wc.conversion_flag = 'true' THEN 15000 ELSE 0 END) / 500.00)
        ELSE 0
    END as return_on_ad_spend,
    CASE
        WHEN (SUM(CASE WHEN wc.conversion_flag = 'true' THEN 1 ELSE 0 END) / COUNT(*)) >= 0.20 THEN 'High Performer'
        WHEN (SUM(CASE WHEN wc.conversion_flag = 'true' THEN 1 ELSE 0 END) / COUNT(*)) >= 0.10 THEN 'Good'
        WHEN (SUM(CASE WHEN wc.conversion_flag = 'true' THEN 1 ELSE 0 END) / COUNT(*)) >= 0.05 THEN 'Average'
        ELSE 'Needs Optimization'
    END as channel_effectiveness
FROM acme_raw.raw_web_clickstream wc
GROUP BY CAST(SUBSTR(wc.`timestamp`, 1, 10) AS DATE),
    CASE
        WHEN wc.referrer LIKE '%google%' THEN 'Google Search'
        WHEN wc.referrer LIKE '%bing%' THEN 'Bing Search'
        WHEN wc.referrer LIKE '%linkedin%' THEN 'LinkedIn'
        WHEN wc.referrer LIKE '%facebook%' THEN 'Facebook'
        WHEN wc.referrer = 'direct' THEN 'Direct'
        ELSE 'Other'
    END;

-- =====================================================================================================================
-- VALIDATION QUERIES
-- Use these queries to verify data in each database
-- =====================================================================================================================

-- Verify raw database
SELECT 'acme_raw.raw_customer_feed' as table_name, COUNT(*) as record_count FROM acme_raw.raw_customer_feed
UNION ALL
SELECT 'acme_raw.raw_supplier_feed', COUNT(*) FROM acme_raw.raw_supplier_feed
UNION ALL
SELECT 'acme_raw.raw_material_prices', COUNT(*) FROM acme_raw.raw_material_prices
UNION ALL
SELECT 'acme_raw.raw_web_clickstream', COUNT(*) FROM acme_raw.raw_web_clickstream
UNION ALL
SELECT 'acme_raw.raw_social_media_mentions', COUNT(*) FROM acme_raw.raw_social_media_mentions
UNION ALL
SELECT 'acme_raw.raw_weather_data', COUNT(*) FROM acme_raw.raw_weather_data;

-- Verify operational database
SELECT 'acme_ops.customers' as table_name, COUNT(*) as record_count FROM acme_ops.customers
UNION ALL
SELECT 'acme_ops.products', COUNT(*) FROM acme_ops.products
UNION ALL
SELECT 'acme_ops.suppliers', COUNT(*) FROM acme_ops.suppliers
UNION ALL
SELECT 'acme_ops.inventory', COUNT(*) FROM acme_ops.inventory
UNION ALL
SELECT 'acme_ops.orders', COUNT(*) FROM acme_ops.orders
UNION ALL
SELECT 'acme_ops.order_items', COUNT(*) FROM acme_ops.order_items
UNION ALL
SELECT 'acme_ops.shipments', COUNT(*) FROM acme_ops.shipments
UNION ALL
SELECT 'acme_ops.production_runs', COUNT(*) FROM acme_ops.production_runs
UNION ALL
SELECT 'acme_ops.raw_materials', COUNT(*) FROM acme_ops.raw_materials;

-- Verify analytics database
SELECT 'acme_analytics.customer_360' as table_name, COUNT(*) as record_count FROM acme_analytics.customer_360
UNION ALL
SELECT 'acme_analytics.sales_summary', COUNT(*) FROM acme_analytics.sales_summary
UNION ALL
SELECT 'acme_analytics.inventory_forecast', COUNT(*) FROM acme_analytics.inventory_forecast
UNION ALL
SELECT 'acme_analytics.production_efficiency', COUNT(*) FROM acme_analytics.production_efficiency
UNION ALL
SELECT 'acme_analytics.customer_lifetime_value', COUNT(*) FROM acme_analytics.customer_lifetime_value
UNION ALL
SELECT 'acme_analytics.supply_chain_metrics', COUNT(*) FROM acme_analytics.supply_chain_metrics
UNION ALL
SELECT 'acme_analytics.marketing_roi', COUNT(*) FROM acme_analytics.marketing_roi;

-- =====================================================================================================================
-- END OF SCRIPT
-- =====================================================================================================================
