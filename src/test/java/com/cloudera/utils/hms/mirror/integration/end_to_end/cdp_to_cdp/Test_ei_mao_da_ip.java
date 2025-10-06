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

import com.cloudera.utils.hms.mirror.MessageCode;
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
                "--hms-mirror.config.downgrade-acid=true",
                "--hms-mirror.config.in-place=true",
                "--hms-mirror.conversion.test-filename=/test_data/assorted_tbls_01.yaml",
                "--hms-mirror.config.filename=/config/default.yaml.cdp-cdp",
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/e2e/cdp_cdp/ei_mao_da_ip"
        })
@Slf4j
public class Test_ei_mao_da_ip extends E2EBaseTest {

    //
//        String[] args = new String[]{"-d", "EXPORT_IMPORT",
//                "-mao", "-da", "-ip",
//                "-ltd", ASSORTED_TBLS_04,
//                "-cfg", CDP_CDP,
//                "-o", outputDir
//        };
//        rtn = mirror.go(args);
//        long check = MessageCode.VALID_ACID_DA_IP_STRATEGIES.getLong();
//        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check * -1, check * -1, rtn);

    @Test
    public void returnCodeTest() {
        // Get Runtime Return Code.
        long rtn = getReturnCode();
        // Verify the return code indicates validation error
        long check = getCheckCode(MessageCode.VALID_ACID_DA_IP_STRATEGIES);
        assertEquals(check, rtn, "Return Code Failure: " + rtn);
    }

    @Test
    public void validateConfigurationError() {
        // The validation error is confirmed by the return code
        // Return code should match VALID_ACID_DA_IP_STRATEGIES error code
        long rtn = getReturnCode();
        long expectedCode = getCheckCode(MessageCode.VALID_ACID_DA_IP_STRATEGIES);
        assertEquals(expectedCode, rtn, 
                "Return code should indicate VALID_ACID_DA_IP_STRATEGIES error");
    }

    @Test
    public void validateNoTablesProcessed() {
        // Since configuration is invalid, no tables should be processed
        assertNotNull(getConversion().getDatabase("assorted_test_db"), "Database should exist");
        assertEquals(3, 
                getConversion().getDatabase("assorted_test_db").getTableMirrors().size(),
                "Should have 3 ACID tables loaded but not processed");
    }

    @Test
    public void validateTablesInInitPhase() {
        // All tables should remain in INIT phase due to validation error
        validatePhase("assorted_test_db", "acid_01", PhaseState.INIT);
        validatePhase("assorted_test_db", "acid_02", PhaseState.INIT);
        validatePhase("assorted_test_db", "acid_03", PhaseState.INIT);
    }

    @Test
    public void validateNullStrategy() {
        // Strategy should be null for all tables since validation failed
        assertNull(getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_01").getStrategy(),
                "Strategy should be null for acid_01");
        assertNull(getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_02").getStrategy(),
                "Strategy should be null for acid_02");
        assertNull(getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_03").getStrategy(),
                "Strategy should be null for acid_03");
    }

    @Test
    public void validateNoSQLGenerated() {
        // No SQL should be generated due to validation error
        var acid01LeftSql = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.LEFT).getSql();
        assertTrue(acid01LeftSql.isEmpty(), "LEFT SQL should be empty for acid_01");
        
        var acid01RightSql = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.RIGHT).getSql();
        assertTrue(acid01RightSql.isEmpty(), "RIGHT SQL should be empty for acid_01");
    }

    @Test
    public void validateStageDuration() {
        // Tables should have 0 stage duration since they weren't processed
        assertEquals(0, getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_01").getStageDuration(),
                "Stage duration should be 0 for acid_01");
        assertEquals(0, getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_02").getStageDuration(),
                "Stage duration should be 0 for acid_02");
        assertEquals(0, getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_03").getStageDuration(),
                "Stage duration should be 0 for acid_03");
    }

    @Test
    public void validateNoProcessingOccurred() {
        // Since validation failed, tables should not have been processed
        // This is evidenced by null strategy and INIT phase state
        var tableMirrors = getConversion().getDatabase("assorted_test_db").getTableMirrors();
        for (var entry : tableMirrors.entrySet()) {
            assertNull(entry.getValue().getStrategy(), 
                    "Strategy should be null for " + entry.getKey());
            assertEquals(PhaseState.INIT, entry.getValue().getPhaseState(),
                    "Phase state should be INIT for " + entry.getKey());
        }
    }

    @Test
    public void validateACIDTablesExistOnLeft() {
        // Tables should still exist on LEFT (loaded from test data)
        assertTrue(getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.LEFT).isExists(),
                "acid_01 should exist on LEFT");
        assertTrue(getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_02").getEnvironmentTable(Environment.LEFT).isExists(),
                "acid_02 should exist on LEFT");
        assertTrue(getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_03").getEnvironmentTable(Environment.LEFT).isExists(),
                "acid_03 should exist on LEFT");
    }

    @Test
    public void validateTablesNotExistOnRight() {
        // Tables should not exist on RIGHT (not processed)
        assertFalse(getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.RIGHT).isExists(),
                "acid_01 should not exist on RIGHT");
        assertFalse(getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_02").getEnvironmentTable(Environment.RIGHT).isExists(),
                "acid_02 should not exist on RIGHT");
        assertFalse(getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_03").getEnvironmentTable(Environment.RIGHT).isExists(),
                "acid_03 should not exist on RIGHT");
    }

    @Test
    public void validatePhaseCount() {
        // Phase counts should be 0 since processing didn't start
        assertEquals(0, getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_01").getCurrentPhase().get(),
                "Current phase should be 0 for acid_01");
        assertEquals(0, getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_01").getTotalPhaseCount().get(),
                "Total phase count should be 0 for acid_01");
    }

    @Test
    public void validateACIDProperties() {
        // Validate that ACID properties are still present in LEFT table definitions
        validateTableIsACID("assorted_test_db", "acid_01", Environment.LEFT);
        validateTableIsACID("assorted_test_db", "acid_02", Environment.LEFT);
        validateTableIsACID("assorted_test_db", "acid_03", Environment.LEFT);
        
        validateTableProperty("assorted_test_db", "acid_01", Environment.LEFT,
                "transactional", "true");
        validateTableProperty("assorted_test_db", "acid_02", Environment.LEFT,
                "transactional", "true");
        validateTableProperty("assorted_test_db", "acid_03", Environment.LEFT,
                "transactional", "true");
    }

    @Test
    public void validateTableLocations() {
        // Validate original table locations are preserved
        validateTableLocation("assorted_test_db", "acid_01", Environment.LEFT,
                "hdfs://HDP50/warehouse/tablespace/managed/hive/assorted_test_db.db/acid_01");
        validateTableLocation("assorted_test_db", "acid_02", Environment.LEFT,
                "hdfs://HDP50/warehouse/tablespace/managed/hive/assorted_test_db.db/acid_02");
        validateTableLocation("assorted_test_db", "acid_03", Environment.LEFT,
                "hdfs://HDP50/warehouse/tablespace/managed/hive/assorted_test_db.db/acid_03");
    }

    @Test
    public void validateOnlyACIDTablesLoaded() {
        // Verify that only ACID tables are loaded (migrate-acid-only)
        var tableMirrors = getConversion().getDatabase("assorted_test_db").getTableMirrors();
        assertTrue(tableMirrors.containsKey("acid_01"), "acid_01 should be loaded");
        assertTrue(tableMirrors.containsKey("acid_02"), "acid_02 should be loaded");
        assertTrue(tableMirrors.containsKey("acid_03"), "acid_03 should be loaded");
        
        // Non-ACID tables should not be present
        assertFalse(tableMirrors.containsKey("ext_part_01"), "ext_part_01 should not be loaded");
        assertFalse(tableMirrors.containsKey("ext_part_02"), "ext_part_02 should not be loaded");
        assertFalse(tableMirrors.containsKey("legacy_mngd_01"), "legacy_mngd_01 should not be loaded");
    }

    @Test
    public void validateBucketingPresent() {
        // Validate that ACID tables still have bucketing defined
        var acid01LeftDef = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.LEFT).getDefinition();
        assertTrue(acid01LeftDef.stream().anyMatch(line -> line.contains("INTO 2 BUCKETS")),
                "acid_01 should have 2 buckets defined");
        
        var acid03LeftDef = getConversion().getDatabase("assorted_test_db")
                .getTableMirrors().get("acid_03").getEnvironmentTable(Environment.LEFT).getDefinition();
        assertTrue(acid03LeftDef.stream().anyMatch(line -> line.contains("INTO 6 BUCKETS")),
                "acid_03 should have 6 buckets defined");
    }

    @Test
    public void validateTableProperties() {
        // Validate that table properties are preserved
        validateTableProperty("assorted_test_db", "acid_01", Environment.LEFT,
                "bucketing_version", "2");
        validateTableProperty("assorted_test_db", "acid_01", Environment.LEFT,
                "transactional_properties", "default");
        
        validateTableProperty("assorted_test_db", "acid_02", Environment.LEFT,
                "bucketing_version", "2");
        validateTableProperty("assorted_test_db", "acid_02", Environment.LEFT,
                "transactional_properties", "default");
        
        validateTableProperty("assorted_test_db", "acid_03", Environment.LEFT,
                "bucketing_version", "2");
        validateTableProperty("assorted_test_db", "acid_03", Environment.LEFT,
                "transactional_properties", "default");
    }

    @Test
    public void validateDatabaseNotProcessed() {
        // Database should exist but have no SQL generated
        assertNotNull(getConversion().getDatabase("assorted_test_db"), "Database should exist");
        var dbRightSql = getConversion().getDatabase("assorted_test_db").getSql(Environment.RIGHT);
        assertTrue(dbRightSql.isEmpty(), "Database RIGHT SQL should be empty");
    }

    @Test
    public void validateNoIssuesOrErrors() {
        // Tables should have no issues or errors (they weren't processed)
        validateTableIssueCount("assorted_test_db", "acid_01", Environment.LEFT, 0);
        validateTableErrorCount("assorted_test_db", "acid_01", Environment.LEFT, 0);
        validateTableIssueCount("assorted_test_db", "acid_02", Environment.LEFT, 0);
        validateTableErrorCount("assorted_test_db", "acid_02", Environment.LEFT, 0);
        validateTableIssueCount("assorted_test_db", "acid_03", Environment.LEFT, 0);
        validateTableErrorCount("assorted_test_db", "acid_03", Environment.LEFT, 0);
    }

}
