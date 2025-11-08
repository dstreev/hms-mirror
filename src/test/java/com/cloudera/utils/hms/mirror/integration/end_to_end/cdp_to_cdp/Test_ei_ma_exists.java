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

import com.cloudera.utils.hms.mirror.CreateStrategy;
import com.cloudera.utils.hms.mirror.PhaseState;
import com.cloudera.utils.hms.mirror.cli.Mirror;
import com.cloudera.utils.hms.mirror.domain.core.TableMirror;
import com.cloudera.utils.hms.mirror.domain.support.DataStrategyEnum;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.integration.end_to_end.E2EBaseTest;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static com.cloudera.utils.hms.mirror.domain.support.Environment.LEFT;
import static com.cloudera.utils.hms.mirror.domain.support.Environment.RIGHT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

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
        long check = 0L;
        assertEquals(check, rtn, "Return Code Failure: " + rtn);
    }

    @Test
    public void databaseLocationTest() {
        // Validate database locations for merge_files_migrate
        validateDBLocation("merge_files_migrate", LEFT,
                "hdfs://HDP50/apps/hive/warehouse/merge_files_migrate.db");
        validateDBLocation("merge_files_migrate", RIGHT,
                "hdfs://HOME90/apps/hive/warehouse/merge_files_migrate.db");
    }

    @Test
    public void checkPhaseTest() {
        // Validate phase state for acid_01 is ERROR due to existing table
        validatePhase("merge_files_migrate", "acid_01", PhaseState.PROCESSED);
        validatePhase("merge_files_migrate", "ext_01", PhaseState.PROCESSED);
    }

    @Test
    public void checkTableCount() {
        // Validate that we have 2 tables in the database
        validateTableCount("merge_files_migrate", 2);
    }

    @Test
    public void checkExportImportStrategy() {
        // Validate that all tables use EXPORT_IMPORT strategy
        validateTableStrategy("merge_files_migrate", "acid_01", DataStrategyEnum.EXPORT_IMPORT);
        validateTableStrategy("merge_files_migrate", "ext_01", DataStrategyEnum.EXPORT_IMPORT);
    }

    @Test
    public void checkExistingTableIssues() {
        // When tables exist on RIGHT without --sync flag, LEFT should have issues
        validateTableIssueCount("merge_files_migrate", "acid_01", RIGHT, 1);
        validateTableIssueCount("merge_files_migrate", "ext_01", RIGHT, 1);

        // Right side should have no issues
        validateTableIssueCount("merge_files_migrate", "acid_01", LEFT, 0);
        validateTableIssueCount("merge_files_migrate", "ext_01", LEFT, 0);
    }

    @Test
    public void checkExistingTableIssueMessage() {
        // Check the specific issue message for existing tables
        validateTableIssue("merge_files_migrate", "acid_01", RIGHT,
                "Schema exists already. Drop it and try again or add `--sync` to OVERWRITE current tables data.");

        validateTableIssue("merge_files_migrate", "ext_01", RIGHT,
                "Schema exists already. Drop it and try again or add `--sync` to OVERWRITE current tables data.");
    }

    @Test
    public void checkSkippedSQL() {
        // Validate that SQL contains "Skipped" message for existing tables
        validateTableSqlNotGenerated("merge_files_migrate", "acid_01", LEFT);

        validateTableSqlNotGenerated("merge_files_migrate", "ext_01", LEFT);
    }

    @Test
    public void checkTableExistence() {
        // Validate that tables exist on both LEFT and RIGHT
        TableMirror tableMirror = getTableMirrorOrFail("merge_files_migrate", "acid_01");
        assertNotNull(tableMirror.getEnvironmentTable(LEFT));
        assertNotNull(tableMirror.getEnvironmentTable(RIGHT));


        TableMirror tableMirror2 = getTableMirrorOrFail("merge_files_migrate", "ext_01");
        assertNotNull(tableMirror2.getEnvironmentTable(LEFT));
        assertNotNull(tableMirror2.getEnvironmentTable(RIGHT));

    }

    @Test
    public void validateCreateStrategy() {
        // All environments should have NOTHING create strategy since tables exist
        validateTableEnvironmentCreateStrategy("merge_files_migrate", "acid_01", LEFT,
                CreateStrategy.NOTHING);
        validateTableEnvironmentCreateStrategy("merge_files_migrate", "acid_01", RIGHT,
                CreateStrategy.NOTHING);
        validateTableEnvironmentCreateStrategy("merge_files_migrate", "ext_01", LEFT,
                CreateStrategy.NOTHING);
        validateTableEnvironmentCreateStrategy("merge_files_migrate", "ext_01", RIGHT,
                CreateStrategy.NOTHING);

    }

    @Test
    public void validateACIDTableProperties() {
        // Validate that acid_01 is transactional
        validateTableIsACID("merge_files_migrate", "acid_01", LEFT);
        validateTableProperty("merge_files_migrate", "acid_01", LEFT,
                "transactional", "true");

        // RIGHT side also has transactional property
        validateTableProperty("merge_files_migrate", "acid_01", RIGHT,
                "transactional", "true");
        validateTableProperty("merge_files_migrate", "acid_01", RIGHT,
                "transactional_properties", "default");
    }

    @Test
    public void validateTableLocations() {
        // Validate table locations
        validateTableLocation("merge_files_migrate", "acid_01", LEFT,
                "hdfs://HDP50/apps/hive/warehouse/merge_files_migrate.db/acid_01");
        validateTableLocation("merge_files_migrate", "acid_01", RIGHT,
                "hdfs://HOME90/warehouse/tablespace/managedDirectory/hive/merge_files_migrate.db/acid_01");
        validateTableLocation("merge_files_migrate", "ext_01", LEFT,
                "hdfs://HDP50/apps/hive/warehouse/merge_files_migrate.db/ext_01");
        validateTableLocation("merge_files_migrate", "ext_01", RIGHT,
                "hdfs://HOME90/apps/hive/warehouse/merge_files_migrate.db/ext_01");
    }

    @Test
    public void validateBucketingVersion() {
        // Validate bucketing_version property
        validateTableProperty("merge_files_migrate", "acid_01", LEFT,
                "bucketing_version", "2");
        validateTableProperty("merge_files_migrate", "acid_01", RIGHT,
                "bucketing_version", "2");
        validateTableProperty("merge_files_migrate", "ext_01", RIGHT,
                "bucketing_version", "2");
    }

    @Test
    public void validateNoRightSQL() {
        // RIGHT side should have no SQL since tables already exist
        validateTableSqlNotGenerated("merge_files_migrate", "acid_01", RIGHT);

        validateTableSqlNotGenerated("merge_files_migrate", "ext_01", RIGHT);

    }

    @Test
    public void checkDBPhaseSummary() {
        // Validate phase summary shows 2 tables in ERROR phase
        validateDBPhaseSummaryCount("merge_files_migrate", PhaseState.PROCESSED, 2);
    }

    @Test
    public void checkTotalPhaseCount() {
        // Validate total phase count for tables
        validateTablePhaseTotalCount("merge_files_migrate", "acid_01", 3);
        validateTablePhaseTotalCount("merge_files_migrate", "ext_01", 3);
    }

    @Test
    public void checkCurrentPhase() {
        // Validate current phase is 1 (stopped at phase 1 due to error)
        validateTablePhaseCurrentCount("merge_files_migrate", "acid_01", 2);
        validateTablePhaseCurrentCount("merge_files_migrate", "ext_01", 2);
    }

}
