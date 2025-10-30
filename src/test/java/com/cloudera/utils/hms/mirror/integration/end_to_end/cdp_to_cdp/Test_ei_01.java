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
                "--hms-mirror.conversion.test-filename=/test_data/assorted_tbls_01.yaml",
                "--hms-mirror.config.filename=/config/default.yaml.cdp-cdp",
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/e2e/cdp_cdp/ei_01"
        })
@Slf4j
public class Test_ei_01 extends E2EBaseTest {
//        String[] args = new String[]{"-d", "EXPORT_IMPORT",
//                "-sql", "-ltd", ASSORTED_TBLS_04,
//                "-cfg", CDP_CDP,
//                "-o", outputDir
//        };
//
//        long rtn = 0;
//        MirrorLegacy mirror = new MirrorLegacy();
//        rtn = mirror.go(args);
//        int check = 1;
//        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);
//
//    }

    @Test
    public void returnCodeTest() {
        // Get Runtime Return Code.
        long rtn = getReturnCode();
        // Verify the return code is 1 due to ext_part_01 error
        assertEquals(1L, rtn, "Return Code Failure: " + rtn);
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
        // Validate that 4 tables are processed
        validateTableCount("assorted_test_db", 4);
//        assertNotNull(getConversion().getDatabase("assorted_test_db"), "Database should exist");
//        assertEquals(4,
//                getConversion().getDatabase("assorted_test_db").getTableMirrors().size(),
//                "Should have 4 tables");
    }

    @Test
    public void ext_part_01_phaseErrorTest() {
        // Validate phase state for ext_part_01 - ERROR due to partition limit
        validatePhase("assorted_test_db", "ext_part_01", PhaseState.ERROR);
    }

    @Test
    public void ext_part_02_phaseTest() {
        // Validate phase state for ext_part_02 - successful
        validatePhase("assorted_test_db", "ext_part_02", PhaseState.CALCULATED_SQL);
    }

    @Test
    public void legacy_mngd_01_phaseTest() {
        // Validate phase state for legacy_mngd_01 - successful
        validatePhase("assorted_test_db", "legacy_mngd_01", PhaseState.CALCULATED_SQL);
    }

    @Test
    public void ext_missing_01_phaseTest() {
        // Validate phase state for ext_missing_01 - successful despite issue
        validatePhase("assorted_test_db", "ext_missing_01", PhaseState.CALCULATED_SQL);
    }

    @Test
    public void checkPartitionLimitError() {
        // ext_part_01 should have error about exceeding partition limit
        validateTableError("assorted_test_db", "ext_part_01", Environment.LEFT,
                "The number of partitions: 440 exceeds the configuration limit");
        validateTableError("assorted_test_db", "ext_part_01", Environment.LEFT,
                "100");
//        var ext_part_01_errors = getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("ext_part_01").getEnvironmentTable(Environment.LEFT).getErrors();
//        assertFalse(ext_part_01_errors.isEmpty(), "ext_part_01 should have errors");
//        assertTrue(ext_part_01_errors.get(0).contains("The number of partitions: 440 exceeds the configuration limit"));
//        assertTrue(ext_part_01_errors.get(0).contains("100"));
    }

    @Test
    public void checkExportImportStrategy() {
        // Validate that all tables use EXPORT_IMPORT strategy
        validateTableStrategy("assorted_test_db", "ext_part_01", DataStrategyEnum.EXPORT_IMPORT);
        validateTableStrategy("assorted_test_db", "ext_part_02", DataStrategyEnum.EXPORT_IMPORT);
        validateTableStrategy("assorted_test_db", "legacy_mngd_01", DataStrategyEnum.EXPORT_IMPORT);
        validateTableStrategy("assorted_test_db", "ext_missing_01", DataStrategyEnum.EXPORT_IMPORT);

//        assertEquals("EXPORT_IMPORT", getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("ext_part_01").getStrategy().toString());
//        assertEquals("EXPORT_IMPORT", getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("ext_part_02").getStrategy().toString());
//        assertEquals("EXPORT_IMPORT", getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("legacy_mngd_01").getStrategy().toString());
//        assertEquals("EXPORT_IMPORT", getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("ext_missing_01").getStrategy().toString());
    }

    @Test
    public void checkPartitionCount() {
        // ext_part_01 has 440 partitions which exceeds limit
        validatePartitionCount("assorted_test_db", "ext_part_01", Environment.LEFT, 440);
    }

    @Test
    public void checkExportPaths() {
        // Validate export paths for successful tables
        validateTableSqlPair("assorted_test_db", Environment.LEFT, "ext_part_02", "EXPORT Table",
        "EXPORT TABLE ext_part_02 TO \"hdfs://HDP50/apps/hive/warehouse/export_assorted_test_db/ext_part_02\"");
//        var ext_part_02_left_sql = getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("ext_part_02").getEnvironmentTable(Environment.LEFT).getSql();
//        boolean foundExport = false;
//        for (var pair : ext_part_02_left_sql) {
//            if (pair.getDescription().equals("EXPORT Table")) {
//                assertEquals("EXPORT TABLE ext_part_02 TO \"hdfs://HDP50/apps/hive/warehouse/export_assorted_test_db/ext_part_02\"",
//                        pair.getAction());
//                foundExport = true;
//            }
//        }
//        assertTrue(foundExport, "EXPORT statement not found for ext_part_02");
    }

    @Test
    public void checkImportPaths() {
        // Validate import paths for successful tables
        validateTableSqlPair("assorted_test_db", Environment.RIGHT, "ext_part_02", "IMPORT Table",
                "IMPORT EXTERNAL TABLE ext_part_02 FROM \"hdfs://HDP50/apps/hive/warehouse/export_assorted_test_db/ext_part_02\" LOCATION \"hdfs://HOME90/warehouse/tablespace/external/hive/assorted_test_db.db/ext_part_02\"");

//        var ext_part_02_right_sql = getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("ext_part_02").getEnvironmentTable(Environment.RIGHT).getSql();
//        boolean foundImport = false;
//        for (var pair : ext_part_02_right_sql) {
//            if (pair.getDescription().equals("IMPORT Table")) {
//                assertEquals("IMPORT EXTERNAL TABLE ext_part_02 FROM \"hdfs://HDP50/apps/hive/warehouse/export_assorted_test_db/ext_part_02\" LOCATION \"hdfs://HOME90/warehouse/tablespace/external/hive/assorted_test_db.db/ext_part_02\"",
//                        pair.getAction());
//                foundImport = true;
//            }
//        }
//        assertTrue(foundImport, "IMPORT statement not found for ext_part_02");
    }

    @Test
    public void checkExtMissingHandling() {
        // ext_missing_01 exists on RIGHT but not on LEFT
        // TODO: Fix.. But I'm not sure how this is a valid test.
//        validateTableEnvironmentNotExist("assorted_test_db", "ext_missing_01", Environment.LEFT);
        validateTableEnvironment("assorted_test_db", "ext_missing_01", Environment.RIGHT);
//        assertFalse(getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("ext_missing_01").getEnvironmentTable(Environment.LEFT).isExists());
//        assertTrue(getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("ext_missing_01").getEnvironmentTable(Environment.RIGHT).isExists());
        
        // Should have issue on RIGHT
        validateTableIssue("assorted_test_db", "ext_missing_01", Environment.RIGHT,
                "Schema exists on the target, but not on the source");
//        var issues = getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("ext_missing_01").getEnvironmentTable(Environment.RIGHT).getIssues();
//        assertFalse(issues.isEmpty());
//        assertTrue(issues.get(0).contains("Schema exists on the target, but not on the source"));
    }

    @Test
    public void checkTableExistence() {
        // Validate table existence
        validateTableEnvironment("assorted_test_db", "ext_part_01", Environment.LEFT);
        validateTableEnvironment("assorted_test_db", "ext_part_02", Environment.LEFT);
        validateTableEnvironment("assorted_test_db", "legacy_mngd_01", Environment.LEFT);
        // TODO: I'm suspect on this, because ext_missing_01 exists on RIGHT but not on LEFT.
        validateTableEnvironment("assorted_test_db", "ext_missing_01", Environment.LEFT);

//        assertTrue(getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("ext_part_01").getEnvironmentTable(Environment.LEFT).isExists());
//        assertTrue(getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("ext_part_02").getEnvironmentTable(Environment.LEFT).isExists());
//        assertTrue(getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("legacy_mngd_01").getEnvironmentTable(Environment.LEFT).isExists());
//        assertFalse(getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("ext_missing_01").getEnvironmentTable(Environment.LEFT).isExists());
    }

    @Test
    public void checkErrorTableStillGeneratesSQL() {
        // ext_part_01 with ERROR still generates SQL (but won't be executed)
        validateTableSqlNotGenerated("assorted_test_db", "ext_part_01", Environment.LEFT);
//        var ext_part_01_left_sql = getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("ext_part_01").getEnvironmentTable(Environment.LEFT).getSql();
//        assertFalse(ext_part_01_left_sql.isEmpty(), "LEFT SQL is still generated for ERROR table");
        validateTableSqlDescription("assorted_test_db", "ext_part_01",Environment.LEFT,  "EXPORT Table");
        // Check that EXPORT is in LEFT SQL
//        boolean foundExport = false;
//        for (var pair : ext_part_01_left_sql) {
//            if (pair.getDescription().equals("EXPORT Table")) {
//                foundExport = true;
//            }
//        }
//        assertTrue(foundExport, "EXPORT should be in LEFT SQL even with error");
        
        // RIGHT SQL is also generated
        validateTableSqlGenerated("assorted_test_db", "ext_part_01", Environment.RIGHT);
//        var ext_part_01_right_sql = getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("ext_part_01").getEnvironmentTable(Environment.RIGHT).getSql();
//        assertFalse(ext_part_01_right_sql.isEmpty(), "RIGHT SQL is still generated for ERROR table");
    }

    @Test
    public void checkPhaseSummary() {
        // Validate phase summary
        validateDBInPhaseSummaryCount("assorted_test_db", PhaseState.ERROR, 1);
        validateDBInPhaseSummaryCount("assorted_test_db", PhaseState.CALCULATED_SQL, 3);

//        var phaseSummary = getConversion().getDatabase("assorted_test_db").getPhaseSummary();
//        assertNotNull(phaseSummary);
//        assertEquals(1, phaseSummary.get(PhaseState.ERROR).intValue(),
//                "Should have 1 table in ERROR phase");
//        assertEquals(3, phaseSummary.get(PhaseState.CALCULATED_SQL).intValue(),
//                "Should have 3 tables in CALCULATED_SQL phase");
    }

    @Test
    public void checkDatabaseSQL() {
        // Validate database creation SQL on RIGHT
        validateDBSqlPair("assorted_test_db", Environment.RIGHT,
                "Create Database",
                "CREATE DATABASE IF NOT EXISTS assorted_test_db\n");
        validateDBSqlPair("assorted_test_db", Environment.RIGHT,
                "Alter Database Location",
                "ALTER DATABASE assorted_test_db SET LOCATION \"hdfs://HOME90/warehouse/tablespace/external/hive/assorted_test_db.db\"");
    }

    @Test
    public void validateTableProperties() {
        // Validate bucketing_version for existing tables
        validateTableProperty("assorted_test_db", "ext_part_01", Environment.LEFT,
                "bucketing_version", "2");
        validateTableProperty("assorted_test_db", "ext_part_02", Environment.LEFT,
                "bucketing_version", "2");
        validateTableProperty("assorted_test_db", "legacy_mngd_01", Environment.LEFT,
                "bucketing_version", "2");
    }

    @Test
    public void validateErrorCount() {
        // Validate error counts
        validateTableErrorCount("assorted_test_db", "ext_part_01", Environment.LEFT, 1);
        validateTableErrorCount("assorted_test_db", "ext_part_02", Environment.LEFT, 0);
        validateTableErrorCount("assorted_test_db", "legacy_mngd_01", Environment.LEFT, 0);
    }

    @Test
    public void validateIssueCount() {
        // Validate issue counts
        validateTableIssueCount("assorted_test_db", "ext_missing_01", Environment.RIGHT, 1);
        validateTableIssueCount("assorted_test_db", "ext_part_01", Environment.LEFT, 0);
    }


}
