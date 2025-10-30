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

import com.cloudera.utils.hms.mirror.cli.Mirror;
import com.cloudera.utils.hms.mirror.domain.core.TableMirror;
import com.cloudera.utils.hms.mirror.domain.support.DataStrategyEnum;
import com.cloudera.utils.hms.mirror.integration.end_to_end.E2EBaseTest;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.cloudera.utils.hms.mirror.PhaseState;
import com.cloudera.utils.hms.mirror.domain.support.Environment;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = Mirror.class,
        args = {
                "--hms-mirror.config.data-strategy=EXPORT_IMPORT",
                "--hms-mirror.config.migrate-acid-only=true",
                "--hms-mirror.config.downgrade-acid=true",
                "--hms-mirror.config.sync=true",
                "--hms-mirror.conversion.test-filename=/test_data/assorted_tbls_01.yaml",
                "--hms-mirror.config.filename=/config/default.yaml.cdp-cdp",
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/e2e/cdp_cdp/ei_mao_da"
        })
@Slf4j
public class Test_ei_mao_da extends E2EBaseTest {
//
//        String[] args = new String[]{"-d", "EXPORT_IMPORT",
//                "-mao", "-da",
//                "-ltd", ASSORTED_TBLS_04,
//                "-cfg", CDP_CDP,
//                "-o", outputDir
//        };
//
//        int check = 1; // partition counts exceed limit of 100 (default).
//        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, rtn, check);

    @Test
    public void returnCodeTest() {
        // Get Runtime Return Code.
        long rtn = getReturnCode();
        // Verify the return code is 1 due to acid_03 partition limit exceeded
        long check = 1L;
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
        validateTableCount("assorted_test_db", 3);

//        assertNotNull(getConversion().getDatabase("assorted_test_db"), "Database should exist");
//        assertEquals(3,
//                getConversion().getDatabase("assorted_test_db").getTableMirrors().size(),
//                "Should have only 3 ACID tables with migrate-acid-only option");
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
    public void acid_03_phaseErrorTest() {
        // Validate phase state for acid_03 - should be ERROR due to partition limit
        validatePhase("assorted_test_db", "acid_03", PhaseState.ERROR);
    }

    @Test
    public void validateExportImportStrategy() {
        // Validate that all tables use EXPORT_IMPORT strategy
        validateTableStrategy("assorted_test_db", "acid_01", DataStrategyEnum.EXPORT_IMPORT);
        validateTableStrategy("assorted_test_db", "acid_02", DataStrategyEnum.EXPORT_IMPORT);
        validateTableStrategy("assorted_test_db", "acid_03", DataStrategyEnum.EXPORT_IMPORT);

//        assertEquals("EXPORT_IMPORT", getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("acid_01").getStrategy().toString());
//        assertEquals("EXPORT_IMPORT", getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("acid_02").getStrategy().toString());
//        assertEquals("EXPORT_IMPORT", getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("acid_03").getStrategy().toString());
    }

    @Test
    public void acidTableValidationTest() {
        // Validate all tables are ACID tables
        validateTableIsACID("assorted_test_db", "acid_01", Environment.LEFT);
        validateTableIsACID("assorted_test_db", "acid_02", Environment.LEFT);
        validateTableIsACID("assorted_test_db", "acid_03", Environment.LEFT);
    }

    @Test
    public void checkTransactionalProperties() {
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
    public void checkAcid03PartitionLimitError() {
        // Validate acid_03 has error due to partition limit exceeded
//        validateTableErrorCount("assorted_test_db", "acid_03", Environment.LEFT, 1);
        validateTableError("assorted_test_db", "acid_03", Environment.LEFT,
                "The number of partitions: 200 exceeds the configuration limit");
        validateTableIssue("assorted_test_db", "acid_03", Environment.LEFT,
                "(hybrid->exportImportPartitionLimit) of 100");
        // Check the specific error message
//        var acid03Errors = getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("acid_03").getEnvironmentTable(Environment.LEFT).getErrors();
//        assertTrue(acid03Errors.get(0).contains("The number of partitions: 200 exceeds the configuration limit"));
//        assertTrue(acid03Errors.get(0).contains("(hybrid->exportImportPartitionLimit) of 100"));
    }

    @Test
    public void checkExportPaths() {
        // Validate export paths for ACID tables
        validateTableSqlPair("assorted_test_db", Environment.LEFT, "acid_01", "EXPORT Table",
                "EXPORT TABLE acid_01 TO \"hdfs://HDP50/apps/hive/warehouse/export_assorted_test_db/acid_01\"");
//        var acid01LeftSql = getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.LEFT).getSql();
//        boolean foundExport = false;
//        for (var pair : acid01LeftSql) {
//            if (pair.getDescription().equals("EXPORT Table")) {
//                assertEquals("EXPORT TABLE acid_01 TO \"hdfs://HDP50/apps/hive/warehouse/export_assorted_test_db/acid_01\"",
//                        pair.getAction());
//                foundExport = true;
//            }
//        }
//        assertTrue(foundExport, "EXPORT statement not found for acid_01");
    }

    @Test
    public void checkImportAsExternal() {
        // With downgrade-acid, ACID tables should be imported as EXTERNAL
        validateTableSqlPair("assorted_test_db", Environment.RIGHT, "acid_01", "IMPORT Table",
                "IMPORT EXTERNAL TABLE acid_01 FROM \"hdfs://HDP50/apps/hive/warehouse/export_assorted_test_db/acid_01\"");

//        var acid01RightSql = getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.RIGHT).getSql();
//        boolean foundImport = false;
//        for (var pair : acid01RightSql) {
//            if (pair.getDescription().equals("IMPORT Table")) {
//                assertEquals("IMPORT EXTERNAL TABLE acid_01 FROM \"hdfs://HDP50/apps/hive/warehouse/export_assorted_test_db/acid_01\"",
//                        pair.getAction());
//                foundImport = true;
//            }
//        }
//        assertTrue(foundImport, "IMPORT EXTERNAL statement not found for acid_01");
    }

    @Test
    public void checkTableLocations() {
        // Validate table locations
        validateTableLocation("assorted_test_db", "acid_01", Environment.LEFT,
                "hdfs://HDP50/warehouse/tablespace/managed/hive/assorted_test_db.db/acid_01");
        validateTableLocation("assorted_test_db", "acid_02", Environment.LEFT,
                "hdfs://HDP50/warehouse/tablespace/managed/hive/assorted_test_db.db/acid_02");
        validateTableLocation("assorted_test_db", "acid_03", Environment.LEFT,
                "hdfs://HDP50/warehouse/tablespace/managed/hive/assorted_test_db.db/acid_03");
    }

    @Test
    public void checkBucketingForACIDTables() {
        // Validate acid_01 has 2 buckets
        validateTableBuckets("assorted_test_db", "acid_01", Environment.LEFT, 2);
//        var acid01LeftDef = getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.LEFT).getDefinition();
//        assertTrue(acid01LeftDef.stream().anyMatch(line -> line.contains("INTO 2 BUCKETS")));
        
        // acid_02 doesn't have buckets
        
        // Validate acid_03 has 6 buckets
        validateTableBuckets("assorted_test_db", "acid_03", Environment.LEFT, 6);
//        var acid03LeftDef = getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("acid_03").getEnvironmentTable(Environment.LEFT).getDefinition();
//        assertTrue(acid03LeftDef.stream().anyMatch(line -> line.contains("INTO 6 BUCKETS")));
    }

    @Test
    public void checkPartitionCount() {
        // acid_03 has 200 partitions which exceeds limit
        validatePartitionCount("assorted_test_db", "acid_03", Environment.LEFT, 200);
    }

    @Test
    public void checkTableProperties() {
        // Validate bucketing_version for all ACID tables
        validateTableProperty("assorted_test_db", "acid_01", Environment.LEFT,
                "bucketing_version", "2");
        validateTableProperty("assorted_test_db", "acid_02", Environment.LEFT,
                "bucketing_version", "2");
        validateTableProperty("assorted_test_db", "acid_03", Environment.LEFT,
                "bucketing_version", "2");
    }

    @Test
    public void checkDatabaseSql() {
        // Validate database creation SQL on RIGHT
        validateDBSqlPair("assorted_test_db", Environment.RIGHT,
                "Create Database",
                "CREATE DATABASE IF NOT EXISTS assorted_test_db\n");
        validateDBSqlPair("assorted_test_db", Environment.RIGHT,
                "Alter Database Location",
                "ALTER DATABASE assorted_test_db SET LOCATION \"hdfs://HOME90/warehouse/tablespace/external/hive/assorted_test_db.db\"");
    }

    @Test
    public void checkTableExistence() {
        // Validate that all ACID tables exist on LEFT
        TableMirror tableMirror = getTableMirrorOrFail("assorted_test_db", "acid_01");
        TableMirror tableMirror2 = getTableMirrorOrFail("assorted_test_db", "acid_02");
        TableMirror tableMirror3 = getTableMirrorOrFail("assorted_test_db", "acid_03");

//        assertTrue(getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.LEFT).isExists());
//        assertTrue(getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("acid_02").getEnvironmentTable(Environment.LEFT).isExists());
//        assertTrue(getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("acid_03").getEnvironmentTable(Environment.LEFT).isExists());
        
        // Tables don't exist on RIGHT yet (not migrated)
        validateTableEnvironment("assorted_test_db", "acid_01", Environment.RIGHT);
        validateTableEnvironment("assorted_test_db", "acid_02", Environment.RIGHT);
        validateTableEnvironment("assorted_test_db", "acid_03", Environment.RIGHT);

//        assertFalse(getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.RIGHT).isExists());
//        assertFalse(getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("acid_02").getEnvironmentTable(Environment.RIGHT).isExists());
//        assertFalse(getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("acid_03").getEnvironmentTable(Environment.RIGHT).isExists());
    }

    @Test
    public void checkOnlyACIDTablesProcessed() {
        // Verify that only ACID tables are in the output (migrate-acid-only)
        TableMirror tableMirror = getTableMirrorOrFail("assorted_test_db", "acid_01");
        TableMirror tableMirror2 = getTableMirrorOrFail("assorted_test_db", "acid_02");
        TableMirror tableMirror3 = getTableMirrorOrFail("assorted_test_db", "acid_03");

//        var tableMirrors = getConversion().getDatabase("assorted_test_db").getTableMirrors();
//        assertTrue(tableMirrors.containsKey("acid_01"));
//        assertTrue(tableMirrors.containsKey("acid_02"));
//        assertTrue(tableMirrors.containsKey("acid_03"));
        
        // Non-ACID tables should not be present
        validateTableMissing("assorted_test_db", "ext_part_01");
        validateTableMissing("assorted_test_db", "ext_part_02");
        validateTableMissing("assorted_test_db", "legacy_mngd_01");
        validateTableMissing("assorted_test_db", "ext_missing_01");

//        assertFalse(tableMirrors.containsKey("ext_part_01"));
//        assertFalse(tableMirrors.containsKey("ext_part_02"));
//        assertFalse(tableMirrors.containsKey("legacy_mngd_01"));
//        assertFalse(tableMirrors.containsKey("ext_missing_01"));
    }

    @Test
    public void checkPhaseSummary() {
        // Validate phase summary
        validateDBInPhaseSummaryCount("assorted_test_db", PhaseState.CALCULATED_SQL, 2);
        validateDBInPhaseSummaryCount("assorted_test_db", PhaseState.ERROR, 1);

//        var phaseSummary = getConversion().getDatabase("assorted_test_db").getPhaseSummary();
//        assertNotNull(phaseSummary);
//        assertEquals(2, phaseSummary.get(PhaseState.CALCULATED_SQL).intValue(),
//                "Should have 2 tables in CALCULATED_SQL phase");
//        assertEquals(1, phaseSummary.get(PhaseState.ERROR).intValue(),
//                "Should have 1 table in ERROR phase (acid_03)");
    }

    @Test
    public void checkTotalPhaseCount() {
        // Validate total phase count for tables
        validateTablePhaseTotalCount("assorted_test_db", "acid_01", 6);
        validateTablePhaseTotalCount("assorted_test_db", "acid_02", 6);
        validateTablePhaseTotalCount("assorted_test_db", "acid_03", 6);

//        assertEquals(6, getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("acid_01").getTotalPhaseCount().get(),
//                "acid_01 should have 6 total phases");
//        assertEquals(6, getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("acid_02").getTotalPhaseCount().get(),
//                "acid_02 should have 6 total phases");
//        assertEquals(6, getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("acid_03").getTotalPhaseCount().get(),
//                "acid_03 should have 6 total phases");
    }


}
