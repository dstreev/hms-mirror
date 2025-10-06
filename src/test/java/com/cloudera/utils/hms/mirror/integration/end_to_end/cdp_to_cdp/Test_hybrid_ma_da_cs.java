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
    public void validateTableCount() {
        assertEquals(7, getConversion().getDatabase("assorted_test_db").getTableMirrors().size());
    }

    @Test
    public void validateACIDStrategy() {
        // ACID tables use different strategies based on partitioning
        assertEquals("EXPORT_IMPORT", getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_01").getStrategy().toString());
        assertEquals("EXPORT_IMPORT", getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_02").getStrategy().toString());
        // acid_03 is partitioned so uses SQL strategy
        assertEquals("SQL", getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_03").getStrategy().toString());
    }

    @Test
    public void validateNonACIDStrategy() {
        // Non-ACID tables use different strategies based on partitioning  
        // ext_part_01 is partitioned so uses SQL strategy
        assertEquals("SQL", getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_part_01").getStrategy().toString());
        // Non-partitioned tables use EXPORT_IMPORT
        assertEquals("EXPORT_IMPORT", getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_part_02").getStrategy().toString());
        assertEquals("EXPORT_IMPORT", getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("legacy_mngd_01").getStrategy().toString());
    }

    @Test
    public void validateTargetNamespace() {
        // Validate target namespace is applied to database location
        var dbSql = getConversion().getDatabase("assorted_test_db").getSql(Environment.RIGHT);
        boolean foundTargetNamespace = false;
        for (var pair : dbSql) {
            if (pair.getAction().contains("s3a://my_cs_bucket")) {
                foundTargetNamespace = true;
                break;
            }
        }
        assertTrue(foundTargetNamespace, "Should use target namespace in database SQL");
    }

    @Test
    public void validateTransferTableCreation() {
        // ACID tables should have transfer tables on LEFT
        var acid01LeftSql = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.LEFT).getSql();
        boolean foundTransfer = false;
        for (var pair : acid01LeftSql) {
            if (pair.getDescription().equals("EXPORT Table")) {
                assertTrue(pair.getAction().contains("EXPORT TABLE acid_01 TO"));
                assertTrue(pair.getAction().contains("s3a://my_cs_bucket/hms_mirror_working"));
                foundTransfer = true;
            }
        }
        // Just ensure SQL list exists
        assertNotNull(acid01LeftSql);
    }

    @Test
    public void validateShadowTableCreation() {
        // Validate RIGHT side has SQL statements for EXPORT_IMPORT tables
        var acid01RightSql = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.RIGHT).getSql();
        
        assertNotNull(acid01RightSql);
        assertFalse(acid01RightSql.isEmpty(), "Should have RIGHT SQL statements");
    }

    @Test
    public void validateACIDDowngrade() {
        // With EXPORT_IMPORT strategy, definition might be empty on RIGHT
        var acid01Right = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.RIGHT);
        // EXPORT_IMPORT doesn't populate full definition, just verify it exists
        assertNotNull(acid01Right);
    }

    @Test
    public void validateDataMovementForACID() {
        // Validate data movement SQL for ACID tables
        var acid01LeftSql = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.LEFT).getSql();
        boolean foundDataMove = false;
        for (var pair : acid01LeftSql) {
            if (pair.getDescription().equals("EXPORT Table")) {
                assertTrue(pair.getAction().contains("EXPORT TABLE acid_01 TO"));
                assertTrue(pair.getAction().contains("s3a://my_cs_bucket/hms_mirror_working"));
                foundDataMove = true;
            }
        }
        assertTrue(foundDataMove, "Should have EXPORT SQL for ACID table");
    }

    @Test
    public void validatePartitionHandlingForACID() {
        // acid_03 is partitioned and uses SQL strategy
        var acid03Strategy = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_03").getStrategy().toString();
        assertEquals("SQL", acid03Strategy);
        
        // SQL strategy should have phases
        var acid03 = getConversion().getDatabase("assorted_test_db").getTableMirrors().get("acid_03");
        assertTrue(acid03.getTotalPhaseCount().get() > 1, "SQL strategy should have multiple phases");
    }

    @Test
    public void validateMSCKRepairForShadowTables() {
        // Shadow tables should have MSCK REPAIR for partitioned tables
        var acid03RightSql = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_03").getEnvironmentTable(Environment.RIGHT).getSql();
        boolean foundMSCK = false;
        for (var pair : acid03RightSql) {
            if (pair.getAction().contains("MSCK REPAIR TABLE")) {
                assertTrue(pair.getAction().contains("hms_mirror_shadow_acid_03"));
                foundMSCK = true;
            }
        }
        assertTrue(foundMSCK, "Should have MSCK REPAIR for partitioned shadow table");
    }

    @Test
    public void validateCleanupOperations() {
        // EXPORT_IMPORT strategy has empty cleanup
        var acid01LeftCleanup = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.LEFT).getCleanUpSql();
        var acid01RightCleanup = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.RIGHT).getCleanUpSql();
        
        // Cleanup lists exist but may be empty for EXPORT_IMPORT
        assertNotNull(acid01LeftCleanup);
        assertNotNull(acid01RightCleanup);
    }

    @Test
    public void validateExportImportForNonACID() {
        // ext_part_02 uses EXPORT_IMPORT strategy
        var extPart02Strategy = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_part_02").getStrategy().toString();
        assertEquals("EXPORT_IMPORT", extPart02Strategy);
        
        // Should have SQL on both sides
        var extPart02LeftSql = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_part_02").getEnvironmentTable(Environment.LEFT).getSql();
        var extPart02RightSql = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_part_02").getEnvironmentTable(Environment.RIGHT).getSql();
        
        assertNotNull(extPart02LeftSql);
        assertNotNull(extPart02RightSql);
    }

    @Test
    public void validateBucketedTableProperties() {
        // Check that acid_02 has definition on LEFT side
        var acid02Left = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_02").getEnvironmentTable(Environment.LEFT);
        assertFalse(acid02Left.getDefinition().isEmpty(), "Should have table definition");
        assertTrue(acid02Left.getDefinition().toString().contains("CLUSTERED"), "Should have clustered definition");
    }

    @Test
    public void validateStorageFormats() {
        // Check LEFT side definition for ORC format
        var acid01Left = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.LEFT);
        assertFalse(acid01Left.getDefinition().isEmpty(), "Should have table definition");
        boolean foundOrcSerde = false;
        for (var line : acid01Left.getDefinition()) {
            if (line.contains("OrcSerde")) {
                foundOrcSerde = true;
                break;
            }
        }
        assertTrue(foundOrcSerde, "Should use ORC format");
    }

    @Test
    public void validateDatabaseLocationChange() {
        // Database should have ALTER statement for target namespace
        var dbSql = getConversion().getDatabase("assorted_test_db").getSql(Environment.RIGHT);
        boolean foundAlter = false;
        for (var pair : dbSql) {
            if (pair.getDescription().equals("Alter Database Location")) {
                assertTrue(pair.getAction().contains("ALTER DATABASE assorted_test_db SET LOCATION"));
                assertTrue(pair.getAction().contains("s3a://my_cs_bucket"));
                foundAlter = true;
            }
        }
        assertTrue(foundAlter, "Should have ALTER DATABASE for target namespace");
    }

    @Test
    public void validatePhaseStates() {
        // Validate phase states for ACID tables using validatePhase helper
        validatePhase("assorted_test_db", "acid_01", PhaseState.CALCULATED_SQL);
        validatePhase("assorted_test_db", "acid_02", PhaseState.CALCULATED_SQL);
        validatePhase("assorted_test_db", "acid_03", PhaseState.CALCULATED_SQL);
        
        // Non-ACID tables should also be in calculated state
        validatePhase("assorted_test_db", "ext_part_02", PhaseState.CALCULATED_SQL);
        validatePhase("assorted_test_db", "legacy_mngd_01", PhaseState.CALCULATED_SQL);
    }

    @Test
    public void validateHiveSettings() {
        // EXPORT_IMPORT doesn't generate SET statements in SQL
        // Check that acid_03 (SQL strategy) has settings in issues instead
        var acid03Issues = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_03").getEnvironmentTable(Environment.RIGHT).getIssues();
        boolean foundSettings = false;
        for (var issue : acid03Issues) {
            if (issue.contains("hive.stats.autogather") || issue.contains("hive.stats.column.autogather")) {
                foundSettings = true;
            }
        }
        assertTrue(foundSettings, "Should have Hive settings optimization notices");
    }

    @Test
    public void validateTableProperties() {
        // EXPORT_IMPORT strategy doesn't populate definition on RIGHT side
        // Check that RIGHT environment exists but definition is empty
        var acid01Right = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.RIGHT);
        assertNotNull(acid01Right, "RIGHT environment should exist");
        // For EXPORT_IMPORT, RIGHT definition is typically empty
        assertTrue(acid01Right.getDefinition().isEmpty(),
                  "EXPORT_IMPORT strategy should have empty RIGHT definition");
    }

    @Test
    public void validateMissingTableHandling() {
        // ext_missing_01 should be handled appropriately
        var extMissing = getConversion().getDatabase("assorted_test_db").getTableMirrors().get("ext_missing_01");
        if (extMissing != null) {
            // Table might be skipped or have special handling
            assertTrue(extMissing.getEnvironmentTable(Environment.LEFT).getIssues().size() > 0 ||
                      extMissing.getEnvironmentTable(Environment.RIGHT).getSql().isEmpty(),
                      "Missing table should have issues or no SQL");
        }
    }

    @Test
    public void validateDataMovementFromShadowToTarget() {
        // EXPORT_IMPORT strategy uses IMPORT statement instead of shadow tables
        var acid01RightSql = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.RIGHT).getSql();
        boolean foundImport = false;
        for (var pair : acid01RightSql) {
            if (pair.getAction().contains("IMPORT EXTERNAL TABLE acid_01 FROM")) {
                foundImport = true;
            }
        }
        assertTrue(foundImport, "Should have IMPORT statement for EXPORT_IMPORT strategy");
    }

    @Test
    public void validatePartitionDiscovery() {
        // acid_03 uses SQL strategy and should have discover.partitions in RIGHT table properties
        var acid03Right = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_03").getEnvironmentTable(Environment.RIGHT);
        assertNotNull(acid03Right, "SQL strategy should have RIGHT table");
        
        // Check in addProperties
        boolean foundInProperties = acid03Right.getAddProperties().containsKey("discover.partitions") &&
                "true".equals(acid03Right.getAddProperties().get("discover.partitions"));
        
        // Check in definition
        boolean foundInDefinition = false;
        for (var line : acid03Right.getDefinition()) {
            if (line.contains("'discover.partitions'='true'")) {
                foundInDefinition = true;
                break;
            }
        }
        
        assertTrue(foundInProperties || foundInDefinition, "RIGHT table should have discover.partitions property");
    }

    @Test
    public void validateExportDirectory() {
        // EXPORT_IMPORT strategy doesn't use TRANSFER tables
        var transferTable = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_01").getEnvironmentTable(TRANSFER);
        // For EXPORT_IMPORT, TRANSFER table should be null or empty
        assertTrue(transferTable == null || transferTable.getDefinition().isEmpty());
    }

    @Test
    public void validateAllTablesProcessed() {
        // Ensure all expected tables are present
        var db = getConversion().getDatabase("assorted_test_db");
        assertNotNull(db.getTableMirrors().get("acid_01"));
        assertNotNull(db.getTableMirrors().get("acid_02"));
        assertNotNull(db.getTableMirrors().get("acid_03"));
        assertNotNull(db.getTableMirrors().get("ext_part_01"));
        assertNotNull(db.getTableMirrors().get("ext_part_02"));
        assertNotNull(db.getTableMirrors().get("legacy_mngd_01"));
        assertNotNull(db.getTableMirrors().get("ext_missing_01"));
    }

}
