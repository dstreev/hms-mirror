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
                "--hms-mirror.conversion.test-filename=/test_data/exists_01.yaml",
                "--hms-mirror.config.filename=/config/default.yaml.cdp-cdp",
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/e2e/cdp_cdp/ei_ma_exists"
        })
@Slf4j
public class Test_ei_ma_exists extends E2EBaseTest {

    @Test
    public void returnCodeTest() {
        // Get Runtime Return Code.
        long rtn = getReturnCode();
        // Verify the return code should be 2 (errors on 2 tables because they exist already)
        long check = 2L;
        assertEquals(check, rtn, "Return Code Failure: " + rtn);
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
    public void acid_01_phaseErrorTest() {
        // Validate phase state for acid_01 is ERROR due to existing table
        validatePhase("merge_files_migrate", "acid_01", PhaseState.ERROR);
    }

    @Test
    public void ext_01_phaseErrorTest() {
        // Validate phase state for ext_01 is ERROR due to existing table
        validatePhase("merge_files_migrate", "ext_01", PhaseState.ERROR);
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
    public void validateExistingTableIssues() {
        // When tables exist on RIGHT without --sync flag, LEFT should have issues
        validateTableIssueCount("merge_files_migrate", "acid_01", Environment.LEFT, 1);
        validateTableIssueCount("merge_files_migrate", "ext_01", Environment.LEFT, 1);
        
        // Right side should have no issues
        validateTableIssueCount("merge_files_migrate", "acid_01", Environment.RIGHT, 0);
        validateTableIssueCount("merge_files_migrate", "ext_01", Environment.RIGHT, 0);
    }

    @Test
    public void validateExistingTableIssueMessage() {
        // Check the specific issue message for existing tables
        var acid01Issues = getConversion().getDatabase("merge_files_migrate")
                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.LEFT).getIssues();
        assertTrue(acid01Issues.contains("Schema exists already. Drop it and try again or add `--sync` to OVERWRITE current tables data."));
        
        var ext01Issues = getConversion().getDatabase("merge_files_migrate")
                .getTableMirrors().get("ext_01").getEnvironmentTable(Environment.LEFT).getIssues();
        assertTrue(ext01Issues.contains("Schema exists already. Drop it and try again or add `--sync` to OVERWRITE current tables data."));
    }

    @Test
    public void validateSkippedSQL() {
        // Validate that SQL contains "Skipped" message for existing tables
        var acid01SqlPairs = getConversion().getDatabase("merge_files_migrate")
                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.LEFT).getSql();
        assertEquals(1, acid01SqlPairs.size(), "Should have one SQL entry");
        assertEquals("Skipped", acid01SqlPairs.get(0).getDescription());
        assertTrue(acid01SqlPairs.get(0).getAction().contains("Schema exists already"));
        
        var ext01SqlPairs = getConversion().getDatabase("merge_files_migrate")
                .getTableMirrors().get("ext_01").getEnvironmentTable(Environment.LEFT).getSql();
        assertEquals(1, ext01SqlPairs.size(), "Should have one SQL entry");
        assertEquals("Skipped", ext01SqlPairs.get(0).getDescription());
        assertTrue(ext01SqlPairs.get(0).getAction().contains("Schema exists already"));
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
        // All environments should have NOTHING create strategy since tables exist
        assertEquals("NOTHING", getConversion().getDatabase("merge_files_migrate")
                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.LEFT).getCreateStrategy().toString());
        assertEquals("NOTHING", getConversion().getDatabase("merge_files_migrate")
                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.RIGHT).getCreateStrategy().toString());
        assertEquals("NOTHING", getConversion().getDatabase("merge_files_migrate")
                .getTableMirrors().get("ext_01").getEnvironmentTable(Environment.LEFT).getCreateStrategy().toString());
        assertEquals("NOTHING", getConversion().getDatabase("merge_files_migrate")
                .getTableMirrors().get("ext_01").getEnvironmentTable(Environment.RIGHT).getCreateStrategy().toString());
    }

    @Test
    public void validateACIDTableProperties() {
        // Validate that acid_01 is transactional
        validateTableIsACID("merge_files_migrate", "acid_01", Environment.LEFT);
        validateTableProperty("merge_files_migrate", "acid_01", Environment.LEFT,
                "transactional", "true");
        
        // RIGHT side also has transactional property
        validateTableProperty("merge_files_migrate", "acid_01", Environment.RIGHT,
                "transactional", "true");
        validateTableProperty("merge_files_migrate", "acid_01", Environment.RIGHT,
                "transactional_properties", "default");
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
    public void validateBucketingVersion() {
        // Validate bucketing_version property
        validateTableProperty("merge_files_migrate", "acid_01", Environment.LEFT,
                "bucketing_version", "2");
        validateTableProperty("merge_files_migrate", "acid_01", Environment.RIGHT,
                "bucketing_version", "2");
        validateTableProperty("merge_files_migrate", "ext_01", Environment.RIGHT,
                "bucketing_version", "2");
    }

    @Test
    public void validateNoRightSQL() {
        // RIGHT side should have no SQL since tables already exist
        var acid01RightSql = getConversion().getDatabase("merge_files_migrate")
                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.RIGHT).getSql();
        assertTrue(acid01RightSql.isEmpty(), "RIGHT acid_01 SQL should be empty");
        
        var ext01RightSql = getConversion().getDatabase("merge_files_migrate")
                .getTableMirrors().get("ext_01").getEnvironmentTable(Environment.RIGHT).getSql();
        assertTrue(ext01RightSql.isEmpty(), "RIGHT ext_01 SQL should be empty");
    }

    @Test
    public void validatePhaseSummary() {
        // Validate phase summary shows 2 tables in ERROR phase
        var phaseSummary = getConversion().getDatabase("merge_files_migrate").getPhaseSummary();
        assertNotNull(phaseSummary);
        assertEquals(2, phaseSummary.get(PhaseState.ERROR).intValue(), 
                "Should have 2 tables in ERROR phase");
    }

    @Test
    public void validateTotalPhaseCount() {
        // Validate total phase count for tables
        assertEquals(3, getConversion().getDatabase("merge_files_migrate")
                .getTableMirrors().get("acid_01").getTotalPhaseCount().get(), 
                "acid_01 should have 3 total phases");
        assertEquals(3, getConversion().getDatabase("merge_files_migrate")
                .getTableMirrors().get("ext_01").getTotalPhaseCount().get(), 
                "ext_01 should have 3 total phases");
    }

    @Test
    public void validateCurrentPhase() {
        // Validate current phase is 1 (stopped at phase 1 due to error)
        assertEquals(1, getConversion().getDatabase("merge_files_migrate")
                .getTableMirrors().get("acid_01").getCurrentPhase().get(), 
                "acid_01 should be at phase 1");
        assertEquals(1, getConversion().getDatabase("merge_files_migrate")
                .getTableMirrors().get("ext_01").getCurrentPhase().get(), 
                "ext_01 should be at phase 1");
    }

}
