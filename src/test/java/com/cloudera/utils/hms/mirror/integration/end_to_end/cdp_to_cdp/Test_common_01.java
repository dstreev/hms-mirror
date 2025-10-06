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
                "--hms-mirror.config.data-strategy=COMMON",
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/e2e/cdp_cdp/common_01",
                "--hms-mirror.conversion.test-filename=/test_data/assorted_tbls_01.yaml",
                "--hms-mirror.config.filename=/config/default.yaml.cdp-cdp"
        })
@Slf4j
public class Test_common_01 extends E2EBaseTest {
    //
//        String[] args = new String[]{"-d", "COMMON",
//                "-sql",
//                "-ltd", ASSORTED_TBLS_04,
//                "-cfg", CDP_CDP,
//                "-o", outputDir
//        };
//
//        long rtn = 0;
//        MirrorLegacy mirror = new MirrorLegacy();
//        rtn = mirror.go(args);
//        int check = 0;
//        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);
//
//    }
    @Test
    public void returnCodeTest() {
        // Get Runtime Return Code.
        long rtn = getReturnCode();
        // Verify the return code.
        assertEquals(0L, rtn, "Return Code Failure: " + rtn);
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
        // Validate that 4 tables are processed with COMMON strategy  
        assertNotNull(getConversion().getDatabase("assorted_test_db"), "Database should exist");
        assertEquals(4, 
                getConversion().getDatabase("assorted_test_db").getTableMirrors().size(),
                "Should have 4 tables");
    }

    @Test
    public void ext_part_01_phaseTest() {
        // Validate phase state for ext_part_01
        validatePhase("assorted_test_db", "ext_part_01", PhaseState.CALCULATED_SQL);
    }

    @Test
    public void ext_part_02_phaseTest() {
        // Validate phase state for ext_part_02
        validatePhase("assorted_test_db", "ext_part_02", PhaseState.CALCULATED_SQL);
    }

    @Test
    public void legacy_mngd_01_phaseTest() {
        // Validate phase state for legacy_mngd_01
        validatePhase("assorted_test_db", "legacy_mngd_01", PhaseState.CALCULATED_SQL);
    }

    @Test
    public void ext_missing_01_phaseTest() {
        // Validate phase state for ext_missing_01
        validatePhase("assorted_test_db", "ext_missing_01", PhaseState.CALCULATED_SQL);
    }

    @Test
    public void validateCommonStrategy() {
        // Validate that all tables use COMMON strategy
        assertEquals("COMMON", getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_part_01").getStrategy().toString());
        assertEquals("COMMON", getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_part_02").getStrategy().toString());
        assertEquals("COMMON", getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("legacy_mngd_01").getStrategy().toString());
        assertEquals("COMMON", getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_missing_01").getStrategy().toString());
    }

    @Test
    public void validateTableLocations() {
        // Validate table locations - COMMON strategy points RIGHT tables to LEFT locations
        validateTableLocation("assorted_test_db", "ext_part_01", Environment.LEFT,
                "hdfs://HDP50/warehouse/tablespace/external/hive/assorted_test_db.db/ext_part_01");
        validateTableLocation("assorted_test_db", "ext_part_02", Environment.LEFT,
                "hdfs://HDP50/warehouse/tablespace/external/hive/assorted_test_db.db/ext_part_02");
        validateTableLocation("assorted_test_db", "legacy_mngd_01", Environment.LEFT,
                "hdfs://HDP50/warehouse/tablespace/external/hive/assorted_test_db.db/legacy_mngd_01");
        // ext_missing_01 exists on RIGHT but not LEFT - location on RIGHT
        validateTableLocation("assorted_test_db", "ext_missing_01", Environment.RIGHT,
                "hdfs://HDP50/warehouse/tablespace/external/hive/assorted_test_db.db/ext_missing_01");
    }

    @Test
    public void validateRightTablesPointToLeftData() {
        // RIGHT tables should point to LEFT data locations in COMMON strategy
        var ext_part_01_right_sql = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_part_01").getEnvironmentTable(Environment.RIGHT).getSql();
        boolean foundLocation = false;
        for (var pair : ext_part_01_right_sql) {
            if (pair.getDescription().equals("Creating Table")) {
                assertTrue(pair.getAction().contains("hdfs://HDP50/warehouse/tablespace/external/hive/assorted_test_db.db/ext_part_01"),
                        "RIGHT table should point to LEFT data location");
                foundLocation = true;
            }
        }
        assertTrue(foundLocation, "Should have CREATE TABLE with LEFT location");
    }

    @Test
    public void validatePartitionedTableHandling() {
        // ext_part_01 is partitioned - should have MSCK REPAIR
        var ext_part_01_right_sql = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_part_01").getEnvironmentTable(Environment.RIGHT).getSql();
        boolean foundMSCK = false;
        for (var pair : ext_part_01_right_sql) {
            if (pair.getDescription().equals("Repairing Table (MSCK)")) {
                assertEquals("MSCK REPAIR TABLE ext_part_01", pair.getAction());
                foundMSCK = true;
            }
        }
        assertTrue(foundMSCK, "Partitioned table should have MSCK REPAIR");
    }

    @Test
    public void validateNonPartitionedTableNoMSCK() {
        // ext_part_02 is non-partitioned - should NOT have MSCK REPAIR
        var ext_part_02_right_sql = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_part_02").getEnvironmentTable(Environment.RIGHT).getSql();
        boolean foundMSCK = false;
        for (var pair : ext_part_02_right_sql) {
            if (pair.getDescription().equals("Repairing Table (MSCK)")) {
                foundMSCK = true;
            }
        }
        assertFalse(foundMSCK, "Non-partitioned table should not have MSCK REPAIR");
    }

    @Test
    public void validateExternalTablePurgeProperty() {
        // All tables should have external.table.purge=true
        var ext_part_01_right_sql = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_part_01").getEnvironmentTable(Environment.RIGHT).getSql();
        boolean foundPurge = false;
        for (var pair : ext_part_01_right_sql) {
            if (pair.getDescription().equals("Creating Table")) {
                assertTrue(pair.getAction().contains("'external.table.purge'='true'"),
                        "Table should have external.table.purge property");
                foundPurge = true;
            }
        }
        assertTrue(foundPurge, "Should have external.table.purge property");
    }

    @Test
    public void validateDiscoverPartitionsProperty() {
        // Partitioned tables should have discover.partitions=true
        var ext_part_01_right_sql = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_part_01").getEnvironmentTable(Environment.RIGHT).getSql();
        boolean foundDiscover = false;
        for (var pair : ext_part_01_right_sql) {
            if (pair.getDescription().equals("Creating Table")) {
                assertTrue(pair.getAction().contains("'discover.partitions'='true'"),
                        "Partitioned table should have discover.partitions property");
                foundDiscover = true;
            }
        }
        assertTrue(foundDiscover, "Should have discover.partitions property");
    }

    @Test
    public void validateTableExistence() {
        // Validate table existence - ext_missing_01 only exists on RIGHT
        assertTrue(getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_part_01").getEnvironmentTable(Environment.LEFT).isExists());
        assertTrue(getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_part_01").getEnvironmentTable(Environment.RIGHT).isExists());
        
        assertTrue(getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_part_02").getEnvironmentTable(Environment.LEFT).isExists());
        assertTrue(getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_part_02").getEnvironmentTable(Environment.RIGHT).isExists());
        
        assertTrue(getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("legacy_mngd_01").getEnvironmentTable(Environment.LEFT).isExists());
        assertTrue(getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("legacy_mngd_01").getEnvironmentTable(Environment.RIGHT).isExists());
        
        // ext_missing_01 exists on RIGHT but not on LEFT
        assertFalse(getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_missing_01").getEnvironmentTable(Environment.LEFT).isExists(),
                "ext_missing_01 should not exist on LEFT");
        assertTrue(getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_missing_01").getEnvironmentTable(Environment.RIGHT).isExists(),
                "ext_missing_01 should exist on RIGHT");
    }

    @Test
    public void validateExtMissing01NoSQL() {
        // ext_missing_01 exists on RIGHT but not LEFT - has issue on LEFT
        var ext_missing_left = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_missing_01").getEnvironmentTable(Environment.LEFT);
        assertFalse(ext_missing_left.getIssues().isEmpty(), "Should have issue on LEFT");
        assertTrue(ext_missing_left.getIssues().get(0).contains("Schema exists on the target, but not on the source"));
        
        // No SQL generated for RIGHT since table already exists there
        var ext_missing_right_sql = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_missing_01").getEnvironmentTable(Environment.RIGHT).getSql();
        assertTrue(ext_missing_right_sql.isEmpty(), "ext_missing_01 should have no SQL on RIGHT");
        
        assertEquals("NOTHING", getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_missing_01").getEnvironmentTable(Environment.RIGHT).getCreateStrategy().toString());
    }

    @Test
    public void validateLegacyManagedTableHandling() {
        // legacy_mngd_01 creates non-transactional table on RIGHT
        var legacy_right_sql = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("legacy_mngd_01").getEnvironmentTable(Environment.RIGHT).getSql();
        boolean foundCreate = false;
        for (var pair : legacy_right_sql) {
            if (pair.getDescription().equals("Creating Table")) {
                // Should create as regular table, not EXTERNAL
                assertTrue(pair.getAction().startsWith("CREATE TABLE"));
                assertFalse(pair.getAction().contains("EXTERNAL"));
                foundCreate = true;
            }
        }
        assertTrue(foundCreate, "Should have CREATE TABLE for legacy managed table");
    }

    @Test
    public void validatePartitionCount() {
        // ext_part_01 has 440 partitions
        validatePartitionCount("assorted_test_db", "ext_part_01", Environment.LEFT, 440);
    }

    @Test
    public void validateTableProperties() {
        // Validate bucketing_version for tables that exist on LEFT
        validateTableProperty("assorted_test_db", "ext_part_01", Environment.LEFT,
                "bucketing_version", "2");
        validateTableProperty("assorted_test_db", "ext_part_02", Environment.LEFT,
                "bucketing_version", "2");
        validateTableProperty("assorted_test_db", "legacy_mngd_01", Environment.LEFT,
                "bucketing_version", "2");
        // ext_missing_01 doesn't exist on LEFT, validate on RIGHT
        validateTableProperty("assorted_test_db", "ext_missing_01", Environment.RIGHT,
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
    public void validatePhaseSummary() {
        // Validate phase summary shows all tables in CALCULATED_SQL phase
        var phaseSummary = getConversion().getDatabase("assorted_test_db").getPhaseSummary();
        assertNotNull(phaseSummary);
        assertEquals(4, phaseSummary.get(PhaseState.CALCULATED_SQL).intValue(), 
                "Should have 4 tables in CALCULATED_SQL phase");
    }

    @Test
    public void validateTotalPhaseCount() {
        // Validate total phase count for tables based on actual values
        assertEquals(5, getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_part_01").getTotalPhaseCount().get(), 
                "ext_part_01 should have 5 total phases");
        assertEquals(4, getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_part_02").getTotalPhaseCount().get(), 
                "ext_part_02 should have 4 total phases");
        assertEquals(4, getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("legacy_mngd_01").getTotalPhaseCount().get(), 
                "legacy_mngd_01 should have 4 total phases");
        assertEquals(2, getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_missing_01").getTotalPhaseCount().get(), 
                "ext_missing_01 should have 2 total phases");
    }

    @Test
    public void validateSQLGeneration() {
        // Validate that SQL is generated for tables (except ext_missing_01 on RIGHT)
        assertFalse(getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_part_01").getEnvironmentTable(Environment.RIGHT).getSql().isEmpty(),
                "RIGHT SQL should be generated for ext_part_01");
        assertFalse(getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_part_02").getEnvironmentTable(Environment.RIGHT).getSql().isEmpty(),
                "RIGHT SQL should be generated for ext_part_02");
        assertFalse(getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("legacy_mngd_01").getEnvironmentTable(Environment.RIGHT).getSql().isEmpty(),
                "RIGHT SQL should be generated for legacy_mngd_01");
        assertTrue(getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_missing_01").getEnvironmentTable(Environment.RIGHT).getSql().isEmpty(),
                "RIGHT SQL should NOT be generated for ext_missing_01");
    }

}
