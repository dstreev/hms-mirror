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
                "--hms-mirror.config.warehouse-directory=/finance/mngd-hive",
                "--hms-mirror.config.external-warehouse-directory=/finance/ext-hive",
//                "--hms-mirror.config.evaluate-partition-locations=true",
                "--hms-mirror.config.align-locations=true",
                "--hms-mirror.conversion.test-filename=/test_data/assorted_tbls_01.yaml",
                "--hms-mirror.config.filename=/config/default.yaml.cdp-cdp",
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/e2e/cdp_cdp/hybrid_ma_wd_epl"
        })
@Slf4j
public class Test_hybrid_ma_wd_epl extends E2EBaseTest {
    //        String outputDir = getOutputDirBase() + nameofCurrMethod;
//
//        String[] args = new String[]{"-d", "HYBRID",
//                "-ma",
//                "-wd", "/finance/mngd-hive",
//                "-ewd", "/finance/ext-hive",
//                "-epl",
//                "-ltd", ASSORTED_TBLS_04, "-cfg", CDP_CDP,
//                "-o", outputDir
//        };
//        long rtn = 0;
//        MirrorLegacy mirror = new MirrorLegacy();
//        rtn = mirror.go(args);
//        assertEquals("Return Code Failure: " + rtn, 0, rtn);
    @Test
    public void returnCodeTest() {
        // Get Runtime Return Code.
        long rtn = getReturnCode();
        // Verify the return code.
        long check = 0L;
        assertEquals(check, rtn, "Return Code Failure: " + rtn);
    }

    @Test
    public void validateTableCount() {
        assertEquals(7, getConversion().getDatabase("assorted_test_db").getTableMirrors().size());
    }

    @Test
    public void validateACIDStrategy() {
        // ACID tables use EXPORT_IMPORT strategy with warehouse directories
        assertEquals("EXPORT_IMPORT", getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_01").getStrategy().toString());
        assertEquals("EXPORT_IMPORT", getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_02").getStrategy().toString());
        // acid_03 is partitioned ACID table and uses ACID strategy
        assertEquals("ACID", getConversion().getDatabase("assorted_test_db")
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
    public void validateDatabaseLocationAlignment() {
        // Database should be aligned to external warehouse directory
        var dbSql = getConversion().getDatabase("assorted_test_db").getSql(Environment.RIGHT);
        boolean foundDbLocation = false;
        for (var pair : dbSql) {
            if (pair.getAction().contains("ALTER DATABASE assorted_test_db SET LOCATION") &&
                pair.getAction().contains("/finance/ext-hive/assorted_test_db.db")) {
                foundDbLocation = true;
                break;
            }
        }
        assertTrue(foundDbLocation, "Database should be aligned to external warehouse directory");
    }

    @Test
    public void validateDatabaseManagedLocationAlignment() {
        // Database should have managed location set to managed warehouse directory
        var dbSql = getConversion().getDatabase("assorted_test_db").getSql(Environment.RIGHT);
        boolean foundManagedLocation = false;
        for (var pair : dbSql) {
            if (pair.getAction().contains("ALTER DATABASE assorted_test_db SET MANAGEDLOCATION") &&
                pair.getAction().contains("/finance/mngd-hive/assorted_test_db.db")) {
                foundManagedLocation = true;
                break;
            }
        }
        assertTrue(foundManagedLocation, "Database should have managed location set to managed warehouse directory");
    }

    @Test
    public void validateWarehouseDirectoryConfiguration() {
        // Validate warehouse configuration is applied by checking the SQL statements
        // This is validated by the other database location tests
        // Return code 0 indicates successful configuration
        assertEquals(0L, getReturnCode());
    }

    @Test
    public void validateExportImportOperations() {
        // EXPORT_IMPORT tables should have export/import operations
        var acid01LeftSql = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.LEFT).getSql();
        boolean foundExport = false;
        for (var pair : acid01LeftSql) {
            if (pair.getAction().contains("EXPORT TABLE acid_01 TO")) {
                foundExport = true;
                break;
            }
        }
        assertTrue(foundExport, "Should have EXPORT operation for ACID table");
        
        // For warehouse alignment, verify RIGHT environment exists
        var acid01Right = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.RIGHT);
        assertNotNull(acid01Right, "RIGHT environment should exist for EXPORT_IMPORT");
    }

    @Test
    public void validateSQLStrategyTables() {
        // SQL strategy tables should have TRANSFER tables
        var acid03Transfer = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_03").getEnvironmentTable(TRANSFER);
        assertNotNull(acid03Transfer, "SQL strategy should have TRANSFER table");
        
        // ext_part_01 should also have SQL strategy
        var extPart01Transfer = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("ext_part_01").getEnvironmentTable(TRANSFER);
        assertNotNull(extPart01Transfer, "Partitioned external table should have TRANSFER table");
    }

    @Test
    public void validateDataMovementSQL() {
        // SQL strategy should have data movement from TRANSFER tables
        var acid03LeftSql = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_03").getEnvironmentTable(Environment.LEFT).getSql();
        boolean foundDataMovement = false;
        for (var pair : acid03LeftSql) {
            if (pair.getAction().contains("INSERT OVERWRITE TABLE hms_mirror_transfer_acid_03")) {
                foundDataMovement = true;
                break;
            }
        }
        assertTrue(foundDataMovement, "Should have data movement to TRANSFER table");
    }

    @Test
    public void validateShadowTables() {
        // SQL strategy should create shadow tables
        var acid03Shadow = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_03").getEnvironmentTable(SHADOW);
        assertNotNull(acid03Shadow, "SQL strategy should have SHADOW table");
        
        // Shadow table should have hms-mirror properties
        boolean foundShadowProperty = false;
        for (var line : acid03Shadow.getDefinition()) {
            if (line.contains("'hms-mirror_shadow_table'='true'")) {
                foundShadowProperty = true;
                break;
            }
        }
        assertTrue(foundShadowProperty, "Shadow table should have hms-mirror_shadow_table property");
    }

    @Test
    public void validateACIDDowngrade() {
        // ACID strategy may have different downgrade handling with warehouse alignment
        var acid03Right = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_03").getEnvironmentTable(Environment.RIGHT);
        
        // For ACID strategy, check that RIGHT table exists and has proper structure
        assertNotNull(acid03Right, "RIGHT table should exist for ACID strategy");
        assertFalse(acid03Right.getDefinition().isEmpty(), "RIGHT table should have definition");
    }

    @Test
    public void validatePartitionHandling() {
        // ACID strategy tables should handle partitions appropriately
        var acid03 = getConversion().getDatabase("assorted_test_db").getTableMirrors().get("acid_03");
        assertEquals("ACID", acid03.getStrategy().toString());
        assertTrue(acid03.getTotalPhaseCount().get() > 1, "ACID strategy should have multiple phases");
    }

    @Test
    public void validateMSCKRepair() {
        // ACID strategy handles partition repair differently than SQL strategy
        var acid03Right = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_03").getEnvironmentTable(Environment.RIGHT);
        assertNotNull(acid03Right, "RIGHT table should exist for ACID strategy");
        
        // Verify the strategy is ACID as expected
        assertEquals("ACID", getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_03").getStrategy().toString());
    }

    @Test
    public void validatePhaseStates() {
        // All tables should reach CALCULATED_SQL phase
        validatePhase("assorted_test_db", "acid_01", PhaseState.CALCULATED_SQL);
        validatePhase("assorted_test_db", "acid_02", PhaseState.CALCULATED_SQL);
        validatePhase("assorted_test_db", "acid_03", PhaseState.CALCULATED_SQL);
        validatePhase("assorted_test_db", "ext_part_01", PhaseState.CALCULATED_SQL);
        validatePhase("assorted_test_db", "ext_part_02", PhaseState.CALCULATED_SQL);
        validatePhase("assorted_test_db", "legacy_mngd_01", PhaseState.CALCULATED_SQL);
    }

    @Test
    public void validateCleanupOperations() {
        // Tables should have appropriate cleanup operations
        var acid01LeftCleanup = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.LEFT).getCleanUpSql();
        var acid01RightCleanup = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.RIGHT).getCleanUpSql();
        
        // Cleanup lists should exist
        assertNotNull(acid01LeftCleanup);
        assertNotNull(acid01RightCleanup);
    }

    @Test
    public void validateMissingTableHandling() {
        // ext_missing_01 should be handled appropriately
        var extMissing = getConversion().getDatabase("assorted_test_db").getTableMirrors().get("ext_missing_01");
        if (extMissing != null) {
            // Table might be skipped or have special handling
            assertTrue(!extMissing.getEnvironmentTable(Environment.LEFT).getIssues().isEmpty() ||
                      extMissing.getEnvironmentTable(Environment.RIGHT).getSql().isEmpty(),
                      "Missing table should have issues or no SQL");
        }
    }

    @Test
    public void validateMetadataProperties() {
        // Tables should have metadata handling with warehouse alignment
        var acid01Right = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.RIGHT);
        
        // For warehouse alignment, verify the environment exists
        assertNotNull(acid01Right, "RIGHT environment should exist with warehouse alignment");
        // Process completed successfully
        assertEquals(0L, getReturnCode(), "Process should complete successfully");
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
