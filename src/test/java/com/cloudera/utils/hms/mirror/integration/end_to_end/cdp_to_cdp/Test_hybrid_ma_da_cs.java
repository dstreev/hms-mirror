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
import com.cloudera.utils.hms.mirror.domain.core.EnvironmentTable;
import com.cloudera.utils.hms.mirror.domain.core.TableMirror;
import com.cloudera.utils.hms.mirror.domain.support.DataStrategyEnum;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.integration.end_to_end.E2EBaseTest;
import com.cloudera.utils.hms.mirror.PhaseState;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static com.cloudera.utils.hms.mirror.domain.support.Environment.*;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = Mirror.class,
        args = {
                "--hms-mirror.config.data-strategy=HYBRID",
                "--hms-mirror.config.migrate-acid=true",
                "--hms-mirror.config.downgrade-acid=true",
                "--hms-mirror.config.target-namespace=s3a://my_cs_bucket",
                "--hms-mirror.conversion.test-filename=/test_data/assorted_tbls_01.yaml",
                "--hms-mirror.config.filename=/config/default.yaml.cdp-cdp",
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/e2e/cdp_cdp/hybrid_ma_da_cs"
        })
@Slf4j
public class Test_hybrid_ma_da_cs extends E2EBaseTest {
    //        String[] args = new String[]{"-d", "HYBRID",
//                "-ma", "-da", "-cs", TARGET_NAMESPACE,
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
    @Test
    public void returnCodeTest() {
        // Get Runtime Return Code.
        long rtn = getReturnCode();
        // Verify the return code
        assertEquals(0L, rtn, "Return Code Failure: " + rtn);
    }

    @Test
    public void checkTableCount() {
        validateTableCount("assorted_test_db", 7);
//        assertEquals(7, getConversion().getDatabase("assorted_test_db").getTableMirrors().size());
    }

    @Test
    public void checkACIDStrategy() {
        // ACID tables use different strategies based on partitioning
        validateTableStrategy("assorted_test_db", "acid_01", DataStrategyEnum.EXPORT_IMPORT);
        validateTableStrategy("assorted_test_db", "acid_02", DataStrategyEnum.EXPORT_IMPORT);
        validateTableStrategy("assorted_test_db", "acid_03", DataStrategyEnum.SQL);

//        assertEquals("EXPORT_IMPORT", getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("acid_01").getStrategy().toString());
//        assertEquals("EXPORT_IMPORT", getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("acid_02").getStrategy().toString());
//         acid_03 is partitioned so uses SQL strategy
//        assertEquals("SQL", getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("acid_03").getStrategy().toString());
    }

    @Test
    public void checkNonACIDStrategy() {
        // Non-ACID tables use different strategies based on partitioning  
        // ext_part_01 is partitioned so uses SQL strategy
        validateTableStrategy("assorted_test_db", "ext_part_01", DataStrategyEnum.SQL);
        validateTableStrategy("assorted_test_db", "ext_part_02", DataStrategyEnum.EXPORT_IMPORT);
        validateTableStrategy("assorted_test_db", "legacy_mngd_01", DataStrategyEnum.EXPORT_IMPORT);

//        assertEquals("SQL", getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("ext_part_01").getStrategy().toString());
        // Non-partitioned tables use EXPORT_IMPORT
//        assertEquals("EXPORT_IMPORT", getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("ext_part_02").getStrategy().toString());
//        assertEquals("EXPORT_IMPORT", getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("legacy_mngd_01").getStrategy().toString());
    }

    @Test
    public void validateTargetNamespace() {
        // Validate target namespace is applied to database location
        validateDBSqlAction("assorted_test_db", Environment.RIGHT, "s3a://my_cs_bucket");
//        var dbSql = getConversion().getDatabase("assorted_test_db").getSql(Environment.RIGHT);
//        boolean foundTargetNamespace = false;
//        for (var pair : dbSql) {
//            if (pair.getAction().contains("s3a://my_cs_bucket")) {
//                foundTargetNamespace = true;
//                break;
//            }
//        }
//        assertTrue(foundTargetNamespace, "Should use target namespace in database SQL");
    }

    @Test
    public void validateTransferTableCreation() {
        // ACID tables should have transfer tables on LEFT
        validateTableSqlAction("assorted_test_db", "acid_01", Environment.LEFT, "EXPORT Table acid_01 TO");
        validateTableSqlAction("assorted_test_db", "acid_02", Environment.LEFT, "s3a://my_cs_bucket/hms_mirror_working");

//        var acid01LeftSql = getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.LEFT).getSql();
//        boolean foundTransfer = false;
//        for (var pair : acid01LeftSql) {
//            if (pair.getDescription().equals("EXPORT Table")) {
//                assertTrue(pair.getAction().contains("EXPORT TABLE acid_01 TO"));
//                assertTrue(pair.getAction().contains("s3a://my_cs_bucket/hms_mirror_working"));
//                foundTransfer = true;
//            }
//        }
        // Just ensure SQL list exists
//        assertNotNull(acid01LeftSql);
    }

    @Test
    public void checkRightTableCreation() {
        // Validate RIGHT side has SQL statements for EXPORT_IMPORT tables
        validateTableSqlGenerated("assorted_test_db", "acid_01", Environment.RIGHT);

//        var acid01RightSql = getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.RIGHT).getSql();
//
//        assertNotNull(acid01RightSql);
//        assertFalse(acid01RightSql.isEmpty(), "Should have RIGHT SQL statements");
    }

    @Test
    public void checkACIDDowngrade() {
        // With EXPORT_IMPORT strategy, definition might be empty on RIGHT
        TableMirror tableMirror = getTableMirrorOrFail("assorted_test_db", "acid_01");
        // Check that the Right environment exists
        assertNotNull(tableMirror.getEnvironmentTable(Environment.RIGHT), "RIGHT environment should exist");

//        var acid01Right = getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.RIGHT);
        // EXPORT_IMPORT doesn't populate full definition, just verify it exists
//        assertNotNull(acid01Right);
    }

    @Test
    public void checkDataMovementForACID() {
        // Validate data movement SQL for ACID tables
        validateTableSqlAction("assorted_test_db", "acid_01", Environment.LEFT,
                "EXPORT Table acid_01 TO");
        validateTableSqlAction("assorted_test_db", "acid_01", Environment.LEFT,
                "s3a://my_cs_bucket/hms_mirror_working");

//        var acid01LeftSql = getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.LEFT).getSql();
//        boolean foundDataMove = false;
//        for (var pair : acid01LeftSql) {
//            if (pair.getDescription().equals("EXPORT Table")) {
//                assertTrue(pair.getAction().contains("EXPORT TABLE acid_01 TO"));
//                assertTrue(pair.getAction().contains("s3a://my_cs_bucket/hms_mirror_working"));
//                foundDataMove = true;
//            }
//        }
//        assertTrue(foundDataMove, "Should have EXPORT SQL for ACID table");
    }

    @Test
    public void checkPartitionHandlingForACID() {
        // acid_03 is partitioned and uses SQL strategy
        validateTableStrategy("assorted_test_db", "acid_03", DataStrategyEnum.SQL);
//        var acid03Strategy = getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("acid_03").getStrategy().toString();
//        assertEquals("SQL", acid03Strategy);
        
        // SQL strategy should have phases
        validateTablePhaseTotalCount("assorted_test_db", "acid_03", 3);
//        var acid03 = getConversion().getDatabase("assorted_test_db").getTableMirrors().get("acid_03");
//        assertTrue(acid03.getTotalPhaseCount().get() > 1, "SQL strategy should have multiple phases");
    }

    @Test
    public void checkMSCKRepairForShadowTables() {
        // Shadow tables should have MSCK REPAIR for partitioned tables
        validateTableSqlAction("assorted_test_db", "acid_03", Environment.RIGHT,
                "MSCK REPAIR TABLE");
        validateTableSqlAction("assorted_test_db", "acid_03", Environment.RIGHT,
                "hms_mirror_shadow_acid_03");

//        var acid03RightSql = getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("acid_03").getEnvironmentTable(Environment.RIGHT).getSql();
//        boolean foundMSCK = false;
//        for (var pair : acid03RightSql) {
//            if (pair.getAction().contains("MSCK REPAIR TABLE")) {
//                assertTrue(pair.getAction().contains("hms_mirror_shadow_acid_03"));
//                foundMSCK = true;
//            }
//        }
//        assertTrue(foundMSCK, "Should have MSCK REPAIR for partitioned shadow table");
    }

    @Test
    public void checkCleanupOperations() {
        // EXPORT_IMPORT strategy has empty cleanup
        validateTableCleanupSqlGenerated("assorted_test_db", "acid_01", Environment.LEFT);
        validateTableCleanupSqlGenerated("assorted_test_db", "acid_01", Environment.RIGHT);
//        var acid01LeftCleanup = getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.LEFT).getCleanUpSql();
//        var acid01RightCleanup = getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.RIGHT).getCleanUpSql();
        
        // Cleanup lists exist but may be empty for EXPORT_IMPORT
//        assertNotNull(acid01LeftCleanup);
//        assertNotNull(acid01RightCleanup);
    }

    @Test
    public void checkExportImportForNonACID() {
        // ext_part_02 uses EXPORT_IMPORT strategy
        validateTableStrategy("assorted_test_db", "ext_part_02", DataStrategyEnum.EXPORT_IMPORT);
//        var extPart02Strategy = getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("ext_part_02").getStrategy().toString();
//        assertEquals("EXPORT_IMPORT", extPart02Strategy);
        
        // Should have SQL on both sides
        validateTableSqlGenerated("assorted_test_db", "ext_part_02", Environment.LEFT);
        validateTableSqlGenerated("assorted_test_db", "ext_part_02", Environment.RIGHT);

//        var extPart02LeftSql = getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("ext_part_02").getEnvironmentTable(Environment.LEFT).getSql();
//        var extPart02RightSql = getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("ext_part_02").getEnvironmentTable(Environment.RIGHT).getSql();
        
//        assertNotNull(extPart02LeftSql);
//        assertNotNull(extPart02RightSql);
    }

    @Test
    public void checkBucketedTableProperties() {
        // Check that acid_02 has definition on LEFT side
//        var acid02Left = getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("acid_02").getEnvironmentTable(Environment.LEFT);
//        assertFalse(acid02Left.getDefinition().isEmpty(), "Should have table definition");
        validateTableEnvironmentDefinitionHas("assorted_test_db", "acid_02", LEFT,
                "CLUSTERED");
//        assertTrue(acid02Left.getDefinition().toString().contains("CLUSTERED"), "Should have clustered definition");
    }

    @Test
    public void checkStorageFormats() {
        // Check LEFT side definition for ORC format
        validateTableEnvironmentDefinitionHas("assorted_test_db", "acid_01", LEFT, "OrcSerde");

//        var acid01Left = getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.LEFT);
//        assertFalse(acid01Left.getDefinition().isEmpty(), "Should have table definition");
//        boolean foundOrcSerde = false;
//        for (var line : acid01Left.getDefinition()) {
//            if (line.contains("OrcSerde")) {
//                foundOrcSerde = true;
//                break;
//            }
//        }
//        assertTrue(foundOrcSerde, "Should use ORC format");
    }

    @Test
    public void checkDatabaseLocationChange() {
        // Database should have ALTER statement for target namespace
        validateDBSqlAction("assorted_test_db", Environment.RIGHT, "ALTER DATABASE assorted_test_db SET LOCATION");
        validateDBSqlAction("assorted_test_db", Environment.RIGHT, "s3a://my_cs_bucket");

//        var dbSql = getConversion().getDatabase("assorted_test_db").getSql(Environment.RIGHT);
//        boolean foundAlter = false;
//        for (var pair : dbSql) {
//            if (pair.getDescription().equals("Alter Database Location")) {
//                assertTrue(pair.getAction().contains("ALTER DATABASE assorted_test_db SET LOCATION"));
//                assertTrue(pair.getAction().contains("s3a://my_cs_bucket"));
//                foundAlter = true;
//            }
//        }
//        assertTrue(foundAlter, "Should have ALTER DATABASE for target namespace");
    }

    @Test
    public void checkPhaseStates() {
        // Validate phase states for ACID tables using validatePhase helper
        validatePhase("assorted_test_db", "acid_01", PhaseState.CALCULATED_SQL);
        validatePhase("assorted_test_db", "acid_02", PhaseState.CALCULATED_SQL);
        validatePhase("assorted_test_db", "acid_03", PhaseState.CALCULATED_SQL);
        
        // Non-ACID tables should also be in calculated state
        validatePhase("assorted_test_db", "ext_part_02", PhaseState.CALCULATED_SQL);
        validatePhase("assorted_test_db", "legacy_mngd_01", PhaseState.CALCULATED_SQL);
    }

    @Test
    public void checkHiveSettings() {
        // EXPORT_IMPORT doesn't generate SET statements in SQL
        // Check that acid_03 (SQL strategy) has settings in issues instead
        validateTableIssuesHave("assorted_test_db", "acid_03", Environment.RIGHT,
                "hive.stats.autogather");
        validateTableIssuesHave("assorted_test_db", "acid_03", Environment.RIGHT,
                "hive.stats.column.autogather");
//        var acid03Issues = getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("acid_03").getEnvironmentTable(Environment.RIGHT).getIssues();
//        boolean foundSettings = false;
//        for (var issue : acid03Issues) {
//            if (issue.contains("hive.stats.autogather") || issue.contains("hive.stats.column.autogather")) {
//                foundSettings = true;
//            }
//        }
//        assertTrue(foundSettings, "Should have Hive settings optimization notices");
    }

    @Test
    public void checkTableDefinition() {
        // EXPORT_IMPORT strategy doesn't populate definition on RIGHT side
        // Check that RIGHT environment exists but definition is empty
        validateTableEnvironmentHasDefinition("assorted_test_db", "acid_01", Environment.RIGHT);

//        var acid01Right = getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.RIGHT);
//        assertNotNull(acid01Right, "RIGHT environment should exist");
        // For EXPORT_IMPORT, RIGHT definition is typically empty
//        assertTrue(acid01Right.getDefinition().isEmpty(),
//                  "EXPORT_IMPORT strategy should have empty RIGHT definition");
    }

    @Test
    public void checkMissingTableHandling() {
        // ext_missing_01 should be handled appropriately
        validateTableIssues("assorted_test_db", "ext_missing_01", Environment.LEFT);
        validateTableSqlNotGenerated("assorted_test_db", "ext_missing_01", Environment.RIGHT);

//        var extMissing = getConversion().getDatabase("assorted_test_db").getTableMirrors().get("ext_missing_01");
//        if (extMissing != null) {
            // Table might be skipped or have special handling
//            assertTrue(extMissing.getEnvironmentTable(Environment.LEFT).getIssues().size() > 0 ||
//                      extMissing.getEnvironmentTable(Environment.RIGHT).getSql().isEmpty(),
//                      "Missing table should have issues or no SQL");
//        }
    }

    @Test
    public void checkDataMovementFromShadowToTarget() {
        // EXPORT_IMPORT strategy uses IMPORT statement instead of shadow tables
        validateTableSqlAction("assorted_test_db", "acid_01", Environment.RIGHT,
                "IMPORT EXTERNAL TABLE acid_01 FROM");
//
//        var acid01RightSql = getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.RIGHT).getSql();
//        boolean foundImport = false;
//        for (var pair : acid01RightSql) {
//            if (pair.getAction().contains("IMPORT EXTERNAL TABLE acid_01 FROM")) {
//                foundImport = true;
//            }
//        }
//        assertTrue(foundImport, "Should have IMPORT statement for EXPORT_IMPORT strategy");
    }

    @Test
    public void checkPartitionDiscovery() {
        // acid_03 uses SQL strategy and should have discover.partitions in RIGHT table properties
        validateTableEnvironment( "assorted_test_db", "acid_03", Environment.RIGHT);
//        var acid03Right = getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("acid_03").getEnvironmentTable(Environment.RIGHT);
//        assertNotNull(acid03Right, "SQL strategy should have RIGHT table");

        validateTableEnvironmentAddPropertiesHas("assorted_test_db", "acid_03", RIGHT,
                "discover.partitions");
        // Check in addProperties
//        boolean foundInProperties = acid03Right.getAddProperties().containsKey("discover.partitions") &&
//                "true".equals(acid03Right.getAddProperties().get("discover.partitions"));

        validateTableEnvironmentDefinitionHas("assorted_test_db", "acid_03", RIGHT,
                "'discover.partitions'='true'");
        // Check in definition
//        boolean foundInDefinition = false;
//        for (var line : acid03Right.getDefinition()) {
//            if (line.contains("'discover.partitions'='true'")) {
//                foundInDefinition = true;
//                break;
//            }
//        }
        
//        assertTrue(foundInProperties || foundInDefinition, "RIGHT table should have discover.partitions property");
    }

    @Test
    public void checkExportDirectory() {
        // EXPORT_IMPORT strategy doesn't use TRANSFER tables
        // TODO: Huh?
//        var transferTable = getConversion().getDatabase("assorted_test_db")
//                .getTableMirrors().get("acid_01").getEnvironmentTable(TRANSFER);
        // For EXPORT_IMPORT, TRANSFER table should be null or empty
//        assertTrue(transferTable == null || transferTable.getDefinition().isEmpty());
    }

    @Test
    public void validateAllTablesProcessed() {
        // Ensure all expected tables are present
        TableMirror tableMirror = getTableMirrorOrFail("assorted_test_db", "acid_01");
        TableMirror tableMirror2 = getTableMirrorOrFail("assorted_test_db", "acid_02");
        TableMirror tableMirror3 = getTableMirrorOrFail("assorted_test_db", "acid_03");
        TableMirror tableMirror4 = getTableMirrorOrFail("assorted_test_db", "ext_part_01");
        TableMirror tableMirror5 = getTableMirrorOrFail("assorted_test_db", "ext_part_02");
        TableMirror tableMirror6 = getTableMirrorOrFail("assorted_test_db", "legacy_mngd_01");
        TableMirror tableMirror7 = getTableMirrorOrFail("assorted_test_db", "ext_missing_01");

//        var db = getConversion().getDatabase("assorted_test_db");
//        assertNotNull(db.getTableMirrors().get("acid_01"));
//        assertNotNull(db.getTableMirrors().get("acid_02"));
//        assertNotNull(db.getTableMirrors().get("acid_03"));
//        assertNotNull(db.getTableMirrors().get("ext_part_01"));
//        assertNotNull(db.getTableMirrors().get("ext_part_02"));
//        assertNotNull(db.getTableMirrors().get("legacy_mngd_01"));
//        assertNotNull(db.getTableMirrors().get("ext_missing_01"));
    }

}
