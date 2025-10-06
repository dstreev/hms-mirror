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
                "--hms-mirror.config.data-strategy=HYBRID",
                "--hms-mirror.config.sync=true",
                "--hms-mirror.conversion.test-filename=/test_data/assorted_tbls_01.yaml",
                "--hms-mirror.config.filename=/config/default.yaml.cdp-cdp",
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/e2e/cdp_cdp/hybrid_01"
        })
@Slf4j
public class Test_hybrid_01 extends E2EBaseTest {
    //        String[] args = new String[]{"-d", "HYBRID",
//                "-sql",
//                "-ltd", ASSORTED_TBLS_04,
//                "-cfg", CDP_CDP,
//                "-o", outputDir
//        };
//
//        long rtn = 0;
//        MirrorLegacy mirror = new MirrorLegacy();
//        rtn = mirror.go(args);
//        int check = 0; // exceed partition count
//        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);
    @Test
    public void returnCodeTest() {
        // Get Runtime Return Code.
        long rtn = getReturnCode();
        // Verify the return code is 0 (success)
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
        // Validate that 4 tables are processed  
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
    public void validateHybridStrategy() {
        // Validate that ext_part_01 uses SQL strategy (HYBRID with partitions > 100 uses SQL)
        assertEquals("SQL", getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_part_01").getStrategy().toString());
        // Other tables use EXPORT_IMPORT strategy when sync=true
        assertEquals("EXPORT_IMPORT", getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_part_02").getStrategy().toString());
        assertEquals("EXPORT_IMPORT", getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("legacy_mngd_01").getStrategy().toString());
        assertEquals("EXPORT_IMPORT", getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_missing_01").getStrategy().toString());
    }

    @Test
    public void validateShadowTableCreation() {
        // ext_part_01 should have shadow table creation SQL
        var ext_part_01_right_sql = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_part_01").getEnvironmentTable(Environment.RIGHT).getSql();
        boolean foundShadow = false;
        for (var pair : ext_part_01_right_sql) {
            if (pair.getDescription().equals("Creating Shadow Table")) {
                assertTrue(pair.getAction().contains("hms_mirror_shadow_ext_part_01"));
                assertTrue(pair.getAction().contains("hdfs://HDP50/warehouse/tablespace/external/hive/assorted_test_db.db/ext_part_01"),
                        "Shadow table should point to LEFT location");
                foundShadow = true;
            }
        }
        assertTrue(foundShadow, "Should have shadow table creation");
    }

    @Test
    public void validateDataMovementSQL() {
        // ext_part_01 should have INSERT OVERWRITE to move data
        var ext_part_01_right_sql = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_part_01").getEnvironmentTable(Environment.RIGHT).getSql();
        boolean foundInsert = false;
        for (var pair : ext_part_01_right_sql) {
            if (pair.getDescription().equals("Moving data to partitioned (440) transfer table")) {
                assertTrue(pair.getAction().contains("FROM hms_mirror_shadow_ext_part_01 INSERT OVERWRITE TABLE ext_part_01"));
                assertTrue(pair.getAction().contains("DISTRIBUTE BY `num`"));
                foundInsert = true;
            }
        }
        assertTrue(foundInsert, "Should have INSERT OVERWRITE for data movement");
    }

    @Test
    public void validateCleanupSQL() {
        // Should have cleanup SQL to drop shadow table
        var ext_part_01_cleanup = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_part_01").getEnvironmentTable(Environment.RIGHT).getCleanUpSql();
        boolean foundCleanup = false;
        for (var pair : ext_part_01_cleanup) {
            if (pair.getDescription().equals("Dropping Shadow Table")) {
                assertEquals("DROP TABLE IF EXISTS hms_mirror_shadow_ext_part_01", pair.getAction());
                foundCleanup = true;
            }
        }
        assertTrue(foundCleanup, "Should have shadow table cleanup");
    }

    @Test
    public void validatePartitionCount() {
        // ext_part_01 has 440 partitions
        validatePartitionCount("assorted_test_db", "ext_part_01", Environment.LEFT, 440);
    }

    @Test
    public void validateTableLocations() {
        // Validate table locations
        validateTableLocation("assorted_test_db", "ext_part_01", Environment.LEFT,
                "hdfs://HDP50/warehouse/tablespace/external/hive/assorted_test_db.db/ext_part_01");
        validateTableLocation("assorted_test_db", "ext_part_02", Environment.LEFT,
                "hdfs://HDP50/warehouse/tablespace/external/hive/assorted_test_db.db/ext_part_02");
        validateTableLocation("assorted_test_db", "legacy_mngd_01", Environment.LEFT,
                "hdfs://HDP50/warehouse/tablespace/external/hive/assorted_test_db.db/legacy_mngd_01");
    }

    @Test
    public void validateRightTableLocations() {
        // RIGHT tables should have new locations
        var ext_part_01_right_sql = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_part_01").getEnvironmentTable(Environment.RIGHT).getSql();
        boolean foundCreate = false;
        for (var pair : ext_part_01_right_sql) {
            if (pair.getDescription().equals("Creating Table")) {
                assertTrue(pair.getAction().contains("hdfs://HOME90/warehouse/tablespace/external/hive/assorted_test_db.db/ext_part_01"),
                        "RIGHT table should have HOME90 location");
                foundCreate = true;
            }
        }
        assertTrue(foundCreate, "Should have CREATE TABLE for RIGHT");
    }

    @Test
    public void validateMSCKRepair() {
        // Shadow table should have MSCK REPAIR
        var ext_part_01_right_sql = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_part_01").getEnvironmentTable(Environment.RIGHT).getSql();
        boolean foundMSCK = false;
        for (var pair : ext_part_01_right_sql) {
            if (pair.getDescription().equals("Repairing Table (MSCK)")) {
                assertEquals("MSCK REPAIR TABLE hms_mirror_shadow_ext_part_01", pair.getAction());
                foundMSCK = true;
            }
        }
        assertTrue(foundMSCK, "Should have MSCK REPAIR for shadow table");
    }

    @Test
    public void validateTableExistence() {
        // Validate table existence
        assertTrue(getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_part_01").getEnvironmentTable(Environment.LEFT).isExists());
        assertTrue(getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_part_02").getEnvironmentTable(Environment.LEFT).isExists());
        assertTrue(getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("legacy_mngd_01").getEnvironmentTable(Environment.LEFT).isExists());
        assertFalse(getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_missing_01").getEnvironmentTable(Environment.LEFT).isExists());
    }

    @Test
    public void validateExtMissingHandling() {
        // ext_missing_01 exists on RIGHT but not on LEFT
        assertFalse(getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_missing_01").getEnvironmentTable(Environment.LEFT).isExists());
        assertTrue(getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_missing_01").getEnvironmentTable(Environment.RIGHT).isExists());
    }

    @Test
    public void validateHiveSettings() {
        // Validate Hive settings are set for SQL data movement
        var ext_part_01_right_sql = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_part_01").getEnvironmentTable(Environment.RIGHT).getSql();
        boolean foundStatsGather = false;
        boolean foundDynamicPartition = false;
        for (var pair : ext_part_01_right_sql) {
            if (pair.getDescription().equals("Setting: hive.stats.autogather")) {
                assertEquals("SET hive.stats.autogather=false", pair.getAction());
                foundStatsGather = true;
            }
            if (pair.getDescription().equals("Setting hive.optimize.sort.dynamic.partition")) {
                assertEquals("SET hive.optimize.sort.dynamic.partition=false", pair.getAction());
                foundDynamicPartition = true;
            }
        }
        assertTrue(foundStatsGather, "Should set hive.stats.autogather");
        assertTrue(foundDynamicPartition, "Should set hive.optimize.sort.dynamic.partition");
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
    public void validateDatabaseSQL() {
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
        // Validate phase summary
        var phaseSummary = getConversion().getDatabase("assorted_test_db").getPhaseSummary();
        assertNotNull(phaseSummary);
        assertEquals(4, phaseSummary.get(PhaseState.CALCULATED_SQL).intValue(), 
                "Should have 4 tables in CALCULATED_SQL phase");
    }

    @Test
    public void validateTotalPhaseCount() {
        // Validate total phase count for tables
        assertEquals(12, getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_part_01").getTotalPhaseCount().get(), 
                "ext_part_01 should have 12 total phases (SQL strategy)");
        assertEquals(6, getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_part_02").getTotalPhaseCount().get(), 
                "ext_part_02 should have 6 total phases (EXPORT_IMPORT)");
        assertEquals(6, getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("legacy_mngd_01").getTotalPhaseCount().get(), 
                "legacy_mngd_01 should have 6 total phases (EXPORT_IMPORT)");
        assertEquals(2, getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_missing_01").getTotalPhaseCount().get(), 
                "ext_missing_01 should have 2 total phases");
    }

    @Test
    public void validateSyncMode() {
        // Test was run with --sync=true
        assertNotNull(getConfig(), "Config should exist");
        assertTrue(getConfig().isSync(), "Sync mode should be enabled");
    }

    @Test
    public void validateDiscoverPartitionsProperty() {
        // Shadow table should have discover.partitions=true
        var ext_part_01_right_sql = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_part_01").getEnvironmentTable(Environment.RIGHT).getSql();
        boolean foundDiscover = false;
        for (var pair : ext_part_01_right_sql) {
            if (pair.getDescription().equals("Creating Shadow Table")) {
                assertTrue(pair.getAction().contains("'discover.partitions'='true'"));
                foundDiscover = true;
            }
        }
        assertTrue(foundDiscover, "Shadow table should have discover.partitions property");
    }

    @Test
    public void validateShadowTableMarker() {
        // Shadow table should have hms-mirror_shadow_table property
        var ext_part_01_right_sql = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_part_01").getEnvironmentTable(Environment.RIGHT).getSql();
        boolean foundMarker = false;
        for (var pair : ext_part_01_right_sql) {
            if (pair.getDescription().equals("Creating Shadow Table")) {
                assertTrue(pair.getAction().contains("'hms-mirror_shadow_table'='true'"));
                foundMarker = true;
            }
        }
        assertTrue(foundMarker, "Shadow table should have hms-mirror_shadow_table marker");
    }


}
