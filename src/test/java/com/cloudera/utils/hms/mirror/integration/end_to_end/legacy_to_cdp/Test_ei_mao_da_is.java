/*
 * Copyright (c) 2024-2025. Cloudera, Inc. All Rights Reserved
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

package com.cloudera.utils.hms.mirror.integration.end_to_end.legacy_to_cdp;

import com.cloudera.utils.hms.mirror.PhaseState;
import com.cloudera.utils.hms.mirror.cli.Mirror;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.integration.end_to_end.E2EBaseTest;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = Mirror.class,
        args = {
                "--hms-mirror.config.comment='Test Export Import for ACID tables ONLY using Intermediate Storage.  This will also DOWNGRADE the tables to EXTERNAL'",
                "--hms-mirror.config.data-strategy=EXPORT_IMPORT",
                "--hms-mirror.config.migrate-acid-only=true",
                "--hms-mirror.config.warehouse-directory=/warehouse/managed",
                "--hms-mirror.config.external-warehouse-directory=/warehouse/external",
                "--hms-mirror.config.downgrade-acid=true",
                "--hms-mirror.config.intermediate-storage=s3a://my_is_bucket",
//                "--hms-mirror.config.reset-to-default-location=true",
//                "--hms-mirror.config.distcp=true",
                "--hms-mirror.conversion.test-filename=/test_data/assorted_tbls_01.yaml",
                "--hms-mirror.config.filename=/config/default.yaml.hdp2-cdp",
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/e2e/legacy_cdp/ei_mao_da_is"
        })
@Slf4j
public class Test_ei_mao_da_is extends E2EBaseTest {
    //        String[] args = new String[]{"-d", "EXPORT_IMPORT",
//                "-mao", "-da", "-is", INTERMEDIATE_STORAGE,
//                "-ltd", ASSORTED_TBLS_04,
//                "-cfg", HDP2_CDP,
//                "-o", outputDir
//        };
//
//        long rtn = 0;
//        MirrorLegacy mirror = new MirrorLegacy();
//        rtn = mirror.go(args);
//        int check = 3;
//        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);

    @Test
    public void returnCodeTest() {
        // Get Runtime Return Code.
        long rtn = getReturnCode();
        // Verify the return code is 3 (3 tables with errors)
        long check = 3L;
        assertEquals(check, rtn, "Return Code Failure: " + rtn);
    }

    @Test
    public void databaseLocationTest() {
        // Validate database locations
        validateDBLocation("assorted_test_db", Environment.LEFT,
                "hdfs://HDP50/warehouse/tablespace/external/hive/assorted_test_db.db");
        validateDBLocation("assorted_test_db", Environment.RIGHT,
                "hdfs://HOME90/warehouse/tablespace/external/hive/assorted_test_db.db");
    }

    @Test
    public void tableCountTest() {
        // Validate that only 3 ACID tables are processed with migrate-acid-only
        assertNotNull(getConversion().getDatabase("assorted_test_db"), "Database should exist");
        assertEquals(3, 
                getConversion().getDatabase("assorted_test_db").getTableMirrors().size(),
                "Should have only 3 ACID tables with migrate-acid-only option");
    }

    @Test
    public void acid_01_phaseErrorTest() {
        // Validate phase state for acid_01 - ERROR due to version incompatibility
        validatePhase("assorted_test_db", "acid_01", PhaseState.ERROR);
    }

    @Test
    public void acid_02_phaseErrorTest() {
        // Validate phase state for acid_02 - ERROR due to version incompatibility
        validatePhase("assorted_test_db", "acid_02", PhaseState.ERROR);
    }

    @Test
    public void acid_03_phaseErrorTest() {
        // Validate phase state for acid_03 - ERROR due to version incompatibility
        validatePhase("assorted_test_db", "acid_03", PhaseState.ERROR);
    }

    @Test
    public void validateExportImportStrategy() {
        // Validate that all tables use EXPORT_IMPORT strategy
        assertEquals("EXPORT_IMPORT", getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_01").getStrategy().toString());
        assertEquals("EXPORT_IMPORT", getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_02").getStrategy().toString());
        assertEquals("EXPORT_IMPORT", getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_03").getStrategy().toString());
    }

    @Test
    public void validateVersionIncompatibilityIssue() {
        // Validate all ACID tables have version incompatibility issue
        validateTableIssueCount("assorted_test_db", "acid_01", Environment.LEFT, 1);
        validateTableIssueCount("assorted_test_db", "acid_02", Environment.LEFT, 1);
        validateTableIssueCount("assorted_test_db", "acid_03", Environment.LEFT, 1);
        
        // Check the specific issue message
        var acid01Issues = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.LEFT).getIssues();
        assertTrue(acid01Issues.get(0).contains("ACID table EXPORTs are NOT compatible for IMPORT to clusters on a different major version of Hive"),
                "Should have version incompatibility issue");
    }

    @Test
    public void acidTableValidationTest() {
        // Validate all tables are ACID tables
        validateTableIsACID("assorted_test_db", "acid_01", Environment.LEFT);
        validateTableIsACID("assorted_test_db", "acid_02", Environment.LEFT);
        validateTableIsACID("assorted_test_db", "acid_03", Environment.LEFT);
    }

    @Test
    public void validateTransactionalProperties() {
        // Validate transactional properties
        validateTableProperty("assorted_test_db", "acid_01", Environment.LEFT,
                "transactional", "true");
        validateTableProperty("assorted_test_db", "acid_01", Environment.LEFT,
                "transactional_properties", "default");
        
        validateTableProperty("assorted_test_db", "acid_02", Environment.LEFT,
                "transactional", "true");
        validateTableProperty("assorted_test_db", "acid_02", Environment.LEFT,
                "transactional_properties", "default");
        
        validateTableProperty("assorted_test_db", "acid_03", Environment.LEFT,
                "transactional", "true");
        validateTableProperty("assorted_test_db", "acid_03", Environment.LEFT,
                "transactional_properties", "default");
    }

    @Test
    public void validateNoSQLGenerated() {
        // No SQL should be generated due to version incompatibility error
        var acid01LeftSql = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.LEFT).getSql();
        assertTrue(acid01LeftSql.isEmpty(), "LEFT SQL should be empty for acid_01");
        
        var acid01RightSql = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.RIGHT).getSql();
        assertTrue(acid01RightSql.isEmpty(), "RIGHT SQL should be empty for acid_01");
    }

    @Test
    public void validateIntermediateStorageNotUsed() {
        // Even though intermediate storage was specified, it shouldn't be used due to error
        // Verify by checking there are no EXPORT commands generated
        var acid01LeftSql = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.LEFT).getSql();
        assertFalse(acid01LeftSql.stream().anyMatch(pair -> pair.getDescription().equals("EXPORT Table")),
                "Should not have EXPORT command due to error");
    }

    @Test
    public void validateTableLocations() {
        // Validate table locations remain unchanged
        validateTableLocation("assorted_test_db", "acid_01", Environment.LEFT,
                "hdfs://HDP50/warehouse/tablespace/managed/hive/assorted_test_db.db/acid_01");
        validateTableLocation("assorted_test_db", "acid_02", Environment.LEFT,
                "hdfs://HDP50/warehouse/tablespace/managed/hive/assorted_test_db.db/acid_02");
        validateTableLocation("assorted_test_db", "acid_03", Environment.LEFT,
                "hdfs://HDP50/warehouse/tablespace/managed/hive/assorted_test_db.db/acid_03");
    }

    @Test
    public void validateBucketingForACIDTables() {
        // Validate acid_01 has 2 buckets
        var acid01LeftDef = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.LEFT).getDefinition();
        assertTrue(acid01LeftDef.stream().anyMatch(line -> line.contains("INTO 2 BUCKETS")));
        
        // acid_02 doesn't have buckets
        
        // Validate acid_03 has 6 buckets
        var acid03LeftDef = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_03").getEnvironmentTable(Environment.LEFT).getDefinition();
        assertTrue(acid03LeftDef.stream().anyMatch(line -> line.contains("INTO 6 BUCKETS")));
    }

    @Test
    public void validatePartitionCount() {
        // acid_03 has partitions
        validatePartitionCount("assorted_test_db", "acid_03", Environment.LEFT, 200);
    }

    @Test
    public void validateTableProperties() {
        // Validate bucketing_version for all ACID tables
        validateTableProperty("assorted_test_db", "acid_01", Environment.LEFT,
                "bucketing_version", "2");
        validateTableProperty("assorted_test_db", "acid_02", Environment.LEFT,
                "bucketing_version", "2");
        validateTableProperty("assorted_test_db", "acid_03", Environment.LEFT,
                "bucketing_version", "2");
    }

    @Test
    public void validateDatabaseSql() {
        // Database SQL should still be generated
        assertTrue(validateDBSqlPair("assorted_test_db", Environment.RIGHT,
                "Create Database",
                "CREATE DATABASE IF NOT EXISTS assorted_test_db\n"));
        assertTrue(validateDBSqlPair("assorted_test_db", Environment.RIGHT,
                "Alter Database Location",
                "ALTER DATABASE assorted_test_db SET LOCATION \"hdfs://HOME90/warehouse/tablespace/external/hive/assorted_test_db.db\""));
    }

    @Test
    public void validateTableExistence() {
        // Validate that all ACID tables exist on LEFT
        assertTrue(getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.LEFT).isExists());
        assertTrue(getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_02").getEnvironmentTable(Environment.LEFT).isExists());
        assertTrue(getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_03").getEnvironmentTable(Environment.LEFT).isExists());
        
        // Tables don't exist on RIGHT (not migrated due to error)
        assertFalse(getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.RIGHT).isExists());
        assertFalse(getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_02").getEnvironmentTable(Environment.RIGHT).isExists());
        assertFalse(getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_03").getEnvironmentTable(Environment.RIGHT).isExists());
    }

    @Test
    public void validateOnlyACIDTablesProcessed() {
        // Verify that only ACID tables are in the output (migrate-acid-only)
        var tableMirrors = getConversion().getDatabase("assorted_test_db").getTableMirrors();
        assertTrue(tableMirrors.containsKey("acid_01"));
        assertTrue(tableMirrors.containsKey("acid_02"));
        assertTrue(tableMirrors.containsKey("acid_03"));
        
        // Non-ACID tables should not be present
        assertFalse(tableMirrors.containsKey("ext_part_01"));
        assertFalse(tableMirrors.containsKey("ext_part_02"));
        assertFalse(tableMirrors.containsKey("legacy_mngd_01"));
        assertFalse(tableMirrors.containsKey("ext_missing_01"));
    }

    @Test
    public void validatePhaseSummary() {
        // Validate phase summary shows all tables in ERROR phase
        var phaseSummary = getConversion().getDatabase("assorted_test_db").getPhaseSummary();
        assertNotNull(phaseSummary);
        assertEquals(3, phaseSummary.get(PhaseState.ERROR).intValue(), 
                "Should have 3 tables in ERROR phase");
    }

    @Test
    public void validateTotalPhaseCount() {
        // Validate total phase count for tables
        assertEquals(2, getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_01").getTotalPhaseCount().get(), 
                "acid_01 should have 2 total phases");
        assertEquals(2, getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_02").getTotalPhaseCount().get(), 
                "acid_02 should have 2 total phases");
        assertEquals(2, getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_03").getTotalPhaseCount().get(), 
                "acid_03 should have 2 total phases");
    }

    @Test
    public void validateCurrentPhase() {
        // All tables should be at phase 1 (stopped due to error)
        assertEquals(1, getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_01").getCurrentPhase().get(), 
                "acid_01 should be at phase 1");
        assertEquals(1, getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_02").getCurrentPhase().get(), 
                "acid_02 should be at phase 1");
        assertEquals(1, getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_03").getCurrentPhase().get(), 
                "acid_03 should be at phase 1");
    }

    @Test
    public void validateLegacyToCDPMigration() {
        // This is a legacy to CDP migration (HDP2 to CDP)
        // The config file used is hdp2-cdp which indicates legacy source
        assertNotNull(getConfig(), "Config should exist");
        // Intermediate storage should be configured
        assertEquals("s3a://my_is_bucket", getConfig().getTransfer().getIntermediateStorage(),
                "Intermediate storage should be configured");
    }

    @Test
    public void validateDowngradeAcidWithIS() {
        // This test validates EXPORT_IMPORT with migrate-acid-only, downgrade-acid, and intermediate storage
        // for a legacy to CDP migration scenario
        // All ACID tables should fail due to version incompatibility
        assertNotNull(getConversion().getDatabase("assorted_test_db"), "Database should exist");
        
        // Verify all tables are in ERROR state
        assertEquals(PhaseState.ERROR, getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_01").getPhaseState());
        assertEquals(PhaseState.ERROR, getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_02").getPhaseState());
        assertEquals(PhaseState.ERROR, getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_03").getPhaseState());
    }

}
