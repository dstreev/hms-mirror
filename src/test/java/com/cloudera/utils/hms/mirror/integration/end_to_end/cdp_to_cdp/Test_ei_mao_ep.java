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

package com.cloudera.utils.hms.mirror.integration.end_to_end.cdp_to_cdp;

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
                "--hms-mirror.config.data-strategy=EXPORT_IMPORT",
                "--hms-mirror.config.migrate-acid-only=true",
                "--hms-mirror.config.export-partition-count=500",
                "--hms-mirror.conversion.test-filename=/test_data/assorted_tbls_01.yaml",
                "--hms-mirror.config.filename=/config/default.yaml.cdp-cdp",
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/e2e/cdp_cdp/ei_mao_ep"
        })
@Slf4j
public class Test_ei_mao_ep extends E2EBaseTest {
//        String[] args = new String[]{"-d", "EXPORT_IMPORT",
//                "-mao",
//                "-ep", "500",
//                "-ltd", ASSORTED_TBLS_04,
//                "-cfg", CDP_CDP,
//                "-o", outputDir
//        };
//
//        long rtn = 0;
//        MirrorLegacy mirror = new MirrorLegacy();
//        rtn = mirror.go(args);
//        int check = 0;
//        // theres 1 non acid table in the test dataset.
//        // TODO: BUG in loadTestData..  Doesn't check -mao option after testdata loaded.
//
//        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check * -1, check * -1, rtn);

    @Test
    public void returnCodeTest() {
        // Get Runtime Return Code.
        long rtn = getReturnCode();
        // Verify the return code is 0 (success)
        long check = 0L;
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
    public void acid_01_phaseTest() {
        // Validate phase state for acid_01
        validatePhase("assorted_test_db", "acid_01", PhaseState.CALCULATED_SQL);
    }

    @Test
    public void acid_02_phaseTest() {
        // Validate phase state for acid_02
        validatePhase("assorted_test_db", "acid_02", PhaseState.CALCULATED_SQL);
    }

    @Test
    public void acid_03_phaseTest() {
        // Validate phase state for acid_03 with partitions
        validatePhase("assorted_test_db", "acid_03", PhaseState.CALCULATED_SQL);
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
    public void validateExportPaths() {
        // Validate export paths for ACID tables
        var acid01LeftSql = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.LEFT).getSql();
        boolean foundExport = false;
        for (var pair : acid01LeftSql) {
            if (pair.getDescription().equals("EXPORT Table")) {
                assertEquals("EXPORT TABLE acid_01 TO \"hdfs://HDP50/apps/hive/warehouse/export_assorted_test_db/acid_01\"",
                        pair.getAction());
                foundExport = true;
            }
        }
        assertTrue(foundExport, "EXPORT statement not found for acid_01");
    }

    @Test
    public void validateImportPaths() {
        // Validate import paths for tables
        var acid01RightSql = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.RIGHT).getSql();
        boolean foundImport = false;
        for (var pair : acid01RightSql) {
            if (pair.getDescription().equals("IMPORT Table")) {
                assertEquals("IMPORT TABLE acid_01 FROM \"hdfs://HDP50/apps/hive/warehouse/export_assorted_test_db/acid_01\"",
                        pair.getAction());
                foundImport = true;
            }
        }
        assertTrue(foundImport, "IMPORT statement not found for acid_01");
    }

    @Test
    public void validatePartitionCount() {
        // acid_03 has 200 partitions
        // The export-partition-count is set to 500, so this should succeed
        validatePartitionCount("assorted_test_db", "acid_03", Environment.LEFT, 200);
    }

    @Test
    public void validateExportPartitionLimit() {
        // Verify that acid_03 with 200 partitions is within the 500 limit
        // No errors should be present
        validateTableErrorCount("assorted_test_db", "acid_03", Environment.LEFT, 0);
        validateTableIssueCount("assorted_test_db", "acid_03", Environment.LEFT, 0);
    }

    @Test
    public void validateTableLocations() {
        // Validate table locations
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
        
        // Validate acid_03 has 6 buckets and is partitioned
        var acid03LeftDef = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_03").getEnvironmentTable(Environment.LEFT).getDefinition();
        assertTrue(acid03LeftDef.stream().anyMatch(line -> line.contains("INTO 6 BUCKETS")));
        assertTrue(acid03LeftDef.stream().anyMatch(line -> line.contains("PARTITIONED BY")));
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
        // Validate database creation SQL on RIGHT
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
        
        // Tables don't exist on RIGHT yet (just SQL generated)
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
        // Validate phase summary shows all tables in CALCULATED_SQL phase
        var phaseSummary = getConversion().getDatabase("assorted_test_db").getPhaseSummary();
        assertNotNull(phaseSummary);
        assertEquals(3, phaseSummary.get(PhaseState.CALCULATED_SQL).intValue(), 
                "Should have 3 tables in CALCULATED_SQL phase");
    }

    @Test
    public void validateTotalPhaseCount() {
        // Validate total phase count for tables
        assertEquals(6, getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_01").getTotalPhaseCount().get(), 
                "acid_01 should have 6 total phases");
        assertEquals(6, getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_02").getTotalPhaseCount().get(), 
                "acid_02 should have 6 total phases");
        assertEquals(6, getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_03").getTotalPhaseCount().get(), 
                "acid_03 should have 6 total phases");
    }

    @Test
    public void validateCurrentPhase() {
        // All tables should be at phase 1
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
    public void validateSQLGeneration() {
        // Validate that SQL is generated for both LEFT and RIGHT
        assertFalse(getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.LEFT).getSql().isEmpty(),
                "LEFT SQL should be generated for acid_01");
        assertFalse(getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.RIGHT).getSql().isEmpty(),
                "RIGHT SQL should be generated for acid_01");
        
        assertFalse(getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_03").getEnvironmentTable(Environment.LEFT).getSql().isEmpty(),
                "LEFT SQL should be generated for acid_03");
        assertFalse(getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_03").getEnvironmentTable(Environment.RIGHT).getSql().isEmpty(),
                "RIGHT SQL should be generated for acid_03");
    }

    @Test
    public void validateExportPartitionCountConfig() {
        // The test sets export-partition-count to 500
        // This allows acid_03 with 200 partitions to be processed
        assertNotNull(getConfig(), "Config should exist");
        assertEquals(500, getConfig().getHybrid().getExportImportPartitionLimit(),
                "Export partition limit should be set to 500");
    }

}
