/*
 * Copyright (c) 2023-2025. Cloudera, Inc. All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.cloudera.utils.hms.mirror.domain.support;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class ConversionRequestTest {

    @Test
    public void testConversionRequestYamlSerialization() throws Exception {
        ConversionRequest conversionRequest = new ConversionRequest();
        
        // Database 1: retail - 15 tables
        List<String> retailTables = new ArrayList<>();
        retailTables.add("customers");
        retailTables.add("orders");
        retailTables.add("order_items");
        retailTables.add("products");
        retailTables.add("categories");
        retailTables.add("inventory");
        retailTables.add("suppliers");
        retailTables.add("warehouses");
        retailTables.add("shipments");
        retailTables.add("returns");
        retailTables.add("payments");
        retailTables.add("discounts");
        retailTables.add("reviews");
        retailTables.add("wishlists");
        retailTables.add("promotions");
        conversionRequest.getDatabases().put("retail", retailTables);
        
        // Database 2: financial - 12 tables
        List<String> financialTables = new ArrayList<>();
        financialTables.add("accounts");
        financialTables.add("transactions");
        financialTables.add("balances");
        financialTables.add("loans");
        financialTables.add("deposits");
        financialTables.add("withdrawals");
        financialTables.add("transfers");
        financialTables.add("interest_rates");
        financialTables.add("credit_scores");
        financialTables.add("audit_logs");
        financialTables.add("compliance_reports");
        financialTables.add("risk_assessments");
        conversionRequest.getDatabases().put("financial", financialTables);
        
        // Database 3: healthcare - 18 tables
        List<String> healthcareTables = new ArrayList<>();
        healthcareTables.add("patients");
        healthcareTables.add("doctors");
        healthcareTables.add("appointments");
        healthcareTables.add("medical_records");
        healthcareTables.add("prescriptions");
        healthcareTables.add("diagnoses");
        healthcareTables.add("treatments");
        healthcareTables.add("medications");
        healthcareTables.add("procedures");
        healthcareTables.add("lab_results");
        healthcareTables.add("imaging_studies");
        healthcareTables.add("insurance_claims");
        healthcareTables.add("billing");
        healthcareTables.add("departments");
        healthcareTables.add("facilities");
        healthcareTables.add("staff_schedules");
        healthcareTables.add("equipment");
        healthcareTables.add("allergies");
        conversionRequest.getDatabases().put("healthcare", healthcareTables);
        
        // Serialize to YAML
        ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
        String yamlOutput = yamlMapper.writeValueAsString(conversionRequest);
        
        System.out.println("ConversionRequest YAML Output:");
        System.out.println("================================");
        System.out.println(yamlOutput);
        System.out.println("================================");
        
        System.out.println("\nSummary:");
        System.out.println("- Total databases: " + conversionRequest.getDatabases().size());
        conversionRequest.getDatabases().forEach((db, tables) -> 
            System.out.println("- " + db + ": " + tables.size() + " tables")
        );
    }
}