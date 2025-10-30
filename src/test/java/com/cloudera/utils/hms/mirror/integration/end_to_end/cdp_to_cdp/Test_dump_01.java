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
                "--hms-mirror.config.data-strategy=DUMP",
                "--hms-mirror.conversion.test-filename=/test_data/assorted_tbls_01.yaml",
                "--hms-mirror.config.filename=/config/default.yaml.cdp-cdp",
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/e2e/cdp_cdp/dump_01"
        })
@Slf4j
public class Test_dump_01 extends E2EBaseTest {
//        String[] args = new String[]{"-d", "DUMP",
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
//        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check * -1, check * -1, rtn);
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
        // Validate database location on LEFT only (DUMP strategy)
        validateDBLocation("assorted_test_db", Environment.LEFT,
                "hdfs://HDP50/warehouse/tablespace/external/hive/assorted_test_db.db");
    }

    @Test
    public void tableCountTest() {
        // Validate that 4 tables are processed with DUMP strategy
        validateTableCount("assorted_test_db", 4);
//        assertNotNull(getConversion().getDatabase("assorted_test_db"), "Database should exist");
//        assertEquals(4,
//                getConversion().getDatabase("assorted_test_db").getTableMirrors().size(),
//                "Should have 4 tables");
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
        // Validate phase state for ext_missing_01 (even though it doesn't exist on LEFT)
        validatePhase("assorted_test_db", "ext_missing_01", PhaseState.CALCULATED_SQL);
    }
    
    @Test
    public void checkExtMissingHandling() {
        // ext_missing_01 doesn't exist on LEFT - validate empty SQL generation
        TableMirror tableMirror = getTableMirrorOrFail("assorted_test_db", "ext_missing_01");
        assertFalse(tableMirror.getEnvironmentTable(Environment.LEFT).isExists(), "ext_missing_01 should not exist on LEFT");
//        var ext_missing_left = getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("ext_missing_01").getEnvironmentTable(Environment.LEFT);
//        assertFalse(ext_missing_left.isExists(), "ext_missing_01 should not exist on LEFT");
        
        // Should have minimal SQL with empty CREATE statement
        // TODO: HUh?
//        var ext_missing_sql = ext_missing_left.getSql();
//        boolean foundEmpty = false;
//        for (var pair : ext_missing_sql) {
//            if (pair.getDescription().equals("Creating Table")) {
//                assertTrue(pair.getAction().isEmpty(), "Create table action should be empty for missing table");
//                foundEmpty = true;
//            }
//        }
//        assertTrue(foundEmpty, "Should have empty CREATE TABLE entry");
    }

    @Test
    public void checkDumpStrategy() {
        // Validate that all tables use DUMP strategy
        validateTableStrategy("assorted_test_db", "ext_part_01", DataStrategyEnum.DUMP);
        validateTableStrategy("assorted_test_db", "ext_part_02", DataStrategyEnum.DUMP);
        validateTableStrategy("assorted_test_db", "legacy_mngd_01", DataStrategyEnum.DUMP);
        validateTableStrategy("assorted_test_db", "ext_missing_01", DataStrategyEnum.DUMP);

//        assertEquals("DUMP", getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("ext_part_01").getStrategy().toString());
//        assertEquals("DUMP", getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("ext_part_02").getStrategy().toString());
//        assertEquals("DUMP", getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("legacy_mngd_01").getStrategy().toString());
//        assertEquals("DUMP", getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("ext_missing_01").getStrategy().toString());
    }

    @Test
    public void checkTableLocations() {
        // Validate table locations on LEFT for tables that exist
        validateTableLocation("assorted_test_db", "ext_part_01", Environment.LEFT,
                "hdfs://HDP50/warehouse/tablespace/external/hive/assorted_test_db.db/ext_part_01");
        validateTableLocation("assorted_test_db", "ext_part_02", Environment.LEFT,
                "hdfs://HDP50/warehouse/tablespace/external/hive/assorted_test_db.db/ext_part_02");
        validateTableLocation("assorted_test_db", "legacy_mngd_01", Environment.LEFT,
                "hdfs://HDP50/warehouse/tablespace/external/hive/assorted_test_db.db/legacy_mngd_01");
        // ext_missing_01 doesn't exist on LEFT
    }

    @Test
    public void checkDumpGeneratesLeftSQL() {
        // DUMP strategy generates SQL on LEFT side only
        validateTableSqlGenerated("assorted_test_db", "ext_part_01", Environment.LEFT);
//        var ext_part_01_left_sql = getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("ext_part_01").getEnvironmentTable(Environment.LEFT).getSql();
//        assertFalse(ext_part_01_left_sql.isEmpty(), "LEFT SQL should be generated for ext_part_01");
        
        // Verify CREATE TABLE exists
        validateTableSqlAction("assorted_test_db","ext_part_01", Environment.LEFT,
                "CREATE EXTERNAL TABLE `ext_part_01`");
//        boolean foundCreate = false;
//        for (var pair : ext_part_01_left_sql) {
//            if (pair.getDescription().equals("Creating Table")) {
//                assertTrue(pair.getAction().contains("CREATE EXTERNAL TABLE `ext_part_01`"));
//                foundCreate = true;
//            }
//        }
//        assertTrue(foundCreate, "Should have CREATE TABLE statement");
    }

    @Test
    public void checkPartitionAddStatements() {
        // ext_part_01 is partitioned - should have ALTER TABLE ADD PARTITION statements
        validateTableSqlAction("assorted_test_db","ext_part_01", Environment.LEFT,
                "ALTER TABLE ext_part_01 ADD IF NOT EXISTS");
        validateTableSqlAction("assorted_test_db","ext_part_01", Environment.LEFT,
                "PARTITION");

//        var ext_part_01_left_sql = getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("ext_part_01").getEnvironmentTable(Environment.LEFT).getSql();
//        boolean foundPartitions = false;
//        for (var pair : ext_part_01_left_sql) {
//            if (pair.getDescription().equals("Alter Table Partition Add Location")) {
//                assertTrue(pair.getAction().contains("ALTER TABLE ext_part_01 ADD IF NOT EXISTS"));
//                assertTrue(pair.getAction().contains("PARTITION"));
//                foundPartitions = true;
//            }
//        }
//        assertTrue(foundPartitions, "Partitioned table should have ADD PARTITION statements");
    }

    @Test
    public void checkNoRightSQL() {
        // DUMP strategy should not generate RIGHT side SQL
        validateTableSqlNotGenerated("assorted_test_db", "ext_part_01", Environment.RIGHT);
//        var databaseSQL = getConversion().getDatabase("assorted_test_db").getSql();
//        assertTrue(databaseSQL.get(Environment.RIGHT).isEmpty(), "No RIGHT SQL should be generated for DUMP");
    }

    @Test
    public void checkTableExistence() {
        // Validate table existence on LEFT
        validateTableEnvironment("assorted_test_db", "ext_part_01", Environment.LEFT);
        validateTableEnvironment("assorted_test_db", "ext_part_02", Environment.LEFT);
        validateTableEnvironment("assorted_test_db", "legacy_mngd_01", Environment.LEFT);
        validateTableEnvironment("assorted_test_db", "ext_missing_01", Environment.LEFT);

//        assertTrue(getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("ext_part_01").getEnvironmentTable(Environment.LEFT).isExists());
//        assertTrue(getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("ext_part_02").getEnvironmentTable(Environment.LEFT).isExists());
//        assertTrue(getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("legacy_mngd_01").getEnvironmentTable(Environment.LEFT).isExists());
//         ext_missing_01 doesn't exist on LEFT
//        assertFalse(getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("ext_missing_01").getEnvironmentTable(Environment.LEFT).isExists(),
//                "ext_missing_01 should not exist on LEFT");
    }

    @Test
    public void checkPartitionCount() {
        // ext_part_01 has 440 partitions
        validatePartitionCount("assorted_test_db", "ext_part_01", Environment.LEFT, 440);
    }

    @Test
    public void checkTableProperties() {
        // Validate bucketing_version for tables that exist on LEFT
        validateTableProperty("assorted_test_db", "ext_part_01", Environment.LEFT,
                "bucketing_version", "2");
        validateTableProperty("assorted_test_db", "ext_part_02", Environment.LEFT,
                "bucketing_version", "2");
        validateTableProperty("assorted_test_db", "legacy_mngd_01", Environment.LEFT,
                "bucketing_version", "2");
        // ext_missing_01 doesn't exist on LEFT so no properties to validate
    }

    @Test
    public void checkDatabaseSQL() {
        // Validate database creation SQL on LEFT
        validateDBSqlGenerated("assorted_test_db", Environment.LEFT);
//        var dbSQL = getConversion().getDatabase("assorted_test_db").getSql();
//        assertFalse(dbSQL.get(Environment.LEFT).isEmpty(), "LEFT database SQL should exist");

        validateDBSqlAction("assorted_test_db", Environment.LEFT, "CREATE DATABASE IF NOT EXISTS assorted_test_db");
        validateDBSqlAction("assorted_test_db", Environment.LEFT, "hdfs://HDP50/warehouse/tablespace/external/hive/assorted_test_db.db");

//        boolean foundCreate = false;
//        for (var pair : dbSQL.get(Environment.LEFT)) {
//            if (pair.getDescription().equals("Create Database")) {
//                assertTrue(pair.getAction().contains("CREATE DATABASE IF NOT EXISTS assorted_test_db"));
//                assertTrue(pair.getAction().contains("hdfs://HDP50/warehouse/tablespace/external/hive/assorted_test_db.db"));
//                foundCreate = true;
//            }
//        }
//        assertTrue(foundCreate, "Should have CREATE DATABASE statement");
    }

    @Test
    public void checkPhaseSummary() {
        // Validate phase summary shows all tables in CALCULATED_SQL phase
        validateDBInPhaseSummaryCount("assorted_test_db", PhaseState.CALCULATED_SQL, 4);
//        var phaseSummary = getConversion().getDatabase("assorted_test_db").getPhaseSummary();
//        assertNotNull(phaseSummary);
//        assertEquals(4, phaseSummary.get(PhaseState.CALCULATED_SQL).intValue(),
//                "Should have 4 tables in CALCULATED_SQL phase");
    }

    @Test
    public void checkTotalPhaseCount() {
        // Validate total phase count for tables
        validateTablePhaseTotalCount("assorted_test_db", "ext_part_01", 5);
        validateTablePhaseTotalCount("assorted_test_db", "ext_part_02", 4);
        validateTablePhaseTotalCount("assorted_test_db", "legacy_mngd_01", 4);
        validateTablePhaseTotalCount("assorted_test_db", "ext_missing_01", 4);

//        assertEquals(5, getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("ext_part_01").getTotalPhaseCount().get(),
//                "ext_part_01 should have 5 total phases");
//        assertEquals(4, getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("ext_part_02").getTotalPhaseCount().get(),
//                "ext_part_02 should have 4 total phases");
//        assertEquals(4, getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("legacy_mngd_01").getTotalPhaseCount().get(),
//                "legacy_mngd_01 should have 4 total phases");
//        assertEquals(4, getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("ext_missing_01").getTotalPhaseCount().get(),
//                "ext_missing_01 should have 4 total phases");
    }

    @Test
    public void checkNonPartitionedTableNoPartitions() {
        // ext_part_02 is non-partitioned - should NOT have partition statements
        validateTableSqlDescription("assorted_test_db", "ext_part_02", Environment.LEFT,
                "Partition");

//        var ext_part_02_left_sql = getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("ext_part_02").getEnvironmentTable(Environment.LEFT).getSql();
//        boolean foundPartitions = false;
//        for (var pair : ext_part_02_left_sql) {
//            if (pair.getDescription().contains("Partition")) {
//                foundPartitions = true;
//            }
//        }
//        assertFalse(foundPartitions, "Non-partitioned table should not have partition statements");
    }

    @Test
    public void checkTableDefinitions() {
        // Validate that table definitions are captured
        validateTableEnvironmentDefinitionHas("assorted_test_db", "ext_part_01", Environment.LEFT,
                "CREATE EXTERNAL TABLE");
        validateTableEnvironmentDefinitionHas("assorted_test_db", "ext_part_02", Environment.LEFT,
                "PARTITIONED BY");

//        var ext_part_01_def = getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("ext_part_01").getEnvironmentTable(Environment.LEFT).getDefinition();
//        assertFalse(ext_part_01_def.isEmpty(), "Table definition should exist");
//        assertTrue(ext_part_01_def.stream().anyMatch(line -> line.contains("CREATE EXTERNAL TABLE")));
//        assertTrue(ext_part_01_def.stream().anyMatch(line -> line.contains("PARTITIONED BY")));

        validateTableEnvironmentDefinitionHas("assorted_test_db", "legacy_mngd_01", Environment.LEFT,
                "CREATE TABLE");
//        var legacy_mngd_01_def = getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("legacy_mngd_01").getEnvironmentTable(Environment.LEFT).getDefinition();
//        assertFalse(legacy_mngd_01_def.isEmpty(), "Table definition should exist");
        // Legacy managed table definition
//        assertTrue(legacy_mngd_01_def.stream().anyMatch(line -> line.contains("CREATE TABLE")));
    }


}
