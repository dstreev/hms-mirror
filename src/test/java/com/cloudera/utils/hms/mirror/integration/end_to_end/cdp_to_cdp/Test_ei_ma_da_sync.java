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
                "--hms-mirror.config.migrate-acid=true",
                "--hms-mirror.config.downgrade-acid=true",
                "--hms-mirror.config.sync=true",
                "--hms-mirror.conversion.test-filename=/test_data/exists_01.yaml",
                "--hms-mirror.config.filename=/config/default.yaml.cdp-cdp",
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/e2e/cdp_cdp/ei_ma_da_sync"
        })
@Slf4j
public class Test_ei_ma_da_sync extends E2EBaseTest {

    @Test
    public void returnCodeTest() {
        // Get Runtime Return Code.
        long rtn = getReturnCode();
        // Verify the return code.
        long check = 0L;
        assertEquals(check * -1, rtn, "Return Code Failure: " + rtn);
    }

    @Test
    public void databaseLocationTest() {
        // Validate database locations for merge_files_migrate
        validateDBLocation("merge_files_migrate", Environment.LEFT, 
                "hdfs://HDP50/apps/hive/warehouse/merge_files_migrate.db");
        validateDBLocation("merge_files_migrate", Environment.RIGHT,
                "hdfs://HOME90/apps/hive/warehouse/merge_files_migrate.db");
    }

    @Test
    public void acid_01_phaseTest() {
        // Validate phase state for acid_01 ACID table
        validatePhase("merge_files_migrate", "acid_01", PhaseState.CALCULATED_SQL);
    }

    @Test
    public void ext_01_phaseTest() {
        // Validate phase state for ext_01 external table
        validatePhase("merge_files_migrate", "ext_01", PhaseState.CALCULATED_SQL);
    }

    @Test
    public void validateTableCount() {
        // Validate that we have 2 tables in the database
        assertEquals(2, getConversion().getDatabase("merge_files_migrate")
                .getTableMirrors().size(), "Table count mismatch");
    }

    @Test
    public void validateExportImportStrategy() {
        // Validate that all tables use EXPORT_IMPORT strategy
        assertEquals("EXPORT_IMPORT", getConversion().getDatabase("merge_files_migrate")
                .getTableMirrors().get("acid_01").getStrategy().toString());
        assertEquals("EXPORT_IMPORT", getConversion().getDatabase("merge_files_migrate")
                .getTableMirrors().get("ext_01").getStrategy().toString());
    }

    @Test
    public void validateACIDTableIsTransactional() {
        // Validate that acid_01 is transactional on LEFT
        validateTableIsACID("merge_files_migrate", "acid_01", Environment.LEFT);
        
        // Validate transactional property
        validateTableProperty("merge_files_migrate", "acid_01", Environment.LEFT,
                "transactional", "true");
    }

    @Test
    public void validateACIDTableDowngrade() {
        // Since downgrade-acid is true, RIGHT side should also be transactional but with different properties
        validateTableProperty("merge_files_migrate", "acid_01", Environment.RIGHT,
                "transactional", "true");
        validateTableProperty("merge_files_migrate", "acid_01", Environment.RIGHT,
                "transactional_properties", "default");
    }

    @Test
    public void validateTableExistence() {
        // Validate that tables exist on both LEFT and RIGHT
        assertTrue(getConversion().getDatabase("merge_files_migrate")
                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.LEFT).isExists());
        assertTrue(getConversion().getDatabase("merge_files_migrate")
                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.RIGHT).isExists());
        assertTrue(getConversion().getDatabase("merge_files_migrate")
                .getTableMirrors().get("ext_01").getEnvironmentTable(Environment.LEFT).isExists());
        assertTrue(getConversion().getDatabase("merge_files_migrate")
                .getTableMirrors().get("ext_01").getEnvironmentTable(Environment.RIGHT).isExists());
    }

    @Test
    public void validateCreateStrategy() {
        // LEFT side should not be modified (NOTHING)
        assertEquals("NOTHING", getConversion().getDatabase("merge_files_migrate")
                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.LEFT).getCreateStrategy().toString());
        assertEquals("NOTHING", getConversion().getDatabase("merge_files_migrate")
                .getTableMirrors().get("ext_01").getEnvironmentTable(Environment.LEFT).getCreateStrategy().toString());
        
        // RIGHT side should have LEAVE strategy since tables exist and sync=true
        assertEquals("LEAVE", getConversion().getDatabase("merge_files_migrate")
                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.RIGHT).getCreateStrategy().toString());
        assertEquals("LEAVE", getConversion().getDatabase("merge_files_migrate")
                .getTableMirrors().get("ext_01").getEnvironmentTable(Environment.RIGHT).getCreateStrategy().toString());
    }

    @Test
    public void validateSyncIssues() {
        // When sync=true and table exists on RIGHT, it should have an issue
        validateTableIssueCount("merge_files_migrate", "acid_01", Environment.RIGHT, 1);
        validateTableIssueCount("merge_files_migrate", "ext_01", Environment.RIGHT, 1);
        
        // Check the specific issue message
        var acid01Issues = getConversion().getDatabase("merge_files_migrate")
                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.RIGHT).getIssues();
        assertTrue(acid01Issues.contains("Schema exists on the target, but not on the source."));
        
        var ext01Issues = getConversion().getDatabase("merge_files_migrate")
                .getTableMirrors().get("ext_01").getEnvironmentTable(Environment.RIGHT).getIssues();
        assertTrue(ext01Issues.contains("Schema exists on the target, but not on the source."));
    }

    @Test
    public void validateACIDTableBuckets() {
        // Validate acid_01 has 2 buckets on LEFT
        var acid01LeftDef = getConversion().getDatabase("merge_files_migrate")
                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.LEFT).getDefinition();
        assertTrue(acid01LeftDef.stream().anyMatch(line -> line.contains("INTO 2 BUCKETS")));
    }

    @Test
    public void validateTableLocations() {
        // Validate table locations
        validateTableLocation("merge_files_migrate", "acid_01", Environment.LEFT,
                "hdfs://HDP50/apps/hive/warehouse/merge_files_migrate.db/acid_01");
        validateTableLocation("merge_files_migrate", "acid_01", Environment.RIGHT,
                "hdfs://HOME90/warehouse/tablespace/managedDirectory/hive/merge_files_migrate.db/acid_01");
        validateTableLocation("merge_files_migrate", "ext_01", Environment.LEFT,
                "hdfs://HDP50/apps/hive/warehouse/merge_files_migrate.db/ext_01");
        validateTableLocation("merge_files_migrate", "ext_01", Environment.RIGHT,
                "hdfs://HOME90/apps/hive/warehouse/merge_files_migrate.db/ext_01");
    }

    @Test
    public void validateTableProperties() {
        // Validate bucketing_version for acid_01
        validateTableProperty("merge_files_migrate", "acid_01", Environment.LEFT,
                "bucketing_version", "2");
        validateTableProperty("merge_files_migrate", "acid_01", Environment.RIGHT,
                "bucketing_version", "2");
        
        // Validate that RIGHT has hms-mirror_Metadata_Stage1 property
        assertNotNull(getConversion().getDatabase("merge_files_migrate")
                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.RIGHT)
                .getDefinition().stream()
                .filter(line -> line.contains("hms-mirror_Metadata_Stage1"))
                .findFirst()
                .orElse(null), "hms-mirror_Metadata_Stage1 property not found");
    }

    @Test
    public void validateNoSQLGenerated() {
        // Since sync=true and tables exist, no SQL should be generated
        var leftSql = getConversion().getDatabase("merge_files_migrate")
                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.LEFT).getSql();
        assertTrue(leftSql.isEmpty(), "LEFT SQL should be empty");
        
        var rightSql = getConversion().getDatabase("merge_files_migrate")
                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.RIGHT).getSql();
        assertTrue(rightSql.isEmpty(), "RIGHT SQL should be empty");
        
        var leftExtSql = getConversion().getDatabase("merge_files_migrate")
                .getTableMirrors().get("ext_01").getEnvironmentTable(Environment.LEFT).getSql();
        assertTrue(leftExtSql.isEmpty(), "LEFT ext_01 SQL should be empty");
        
        var rightExtSql = getConversion().getDatabase("merge_files_migrate")
                .getTableMirrors().get("ext_01").getEnvironmentTable(Environment.RIGHT).getSql();
        assertTrue(rightExtSql.isEmpty(), "RIGHT ext_01 SQL should be empty");
    }

    @Test
    public void validatePhaseSummary() {
        // Validate phase summary shows 2 tables in CALCULATED_SQL phase
        var phaseSummary = getConversion().getDatabase("merge_files_migrate").getPhaseSummary();
        assertNotNull(phaseSummary);
        assertEquals(2, phaseSummary.get(PhaseState.CALCULATED_SQL).intValue(), 
                "Should have 2 tables in CALCULATED_SQL phase");
    }

}
