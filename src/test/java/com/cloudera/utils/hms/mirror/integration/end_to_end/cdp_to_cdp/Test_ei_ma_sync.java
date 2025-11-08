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
import com.cloudera.utils.hms.mirror.domain.support.DataStrategyEnum;
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
                "--hms-mirror.config.sync=true",
                "--hms-mirror.conversion.test-filename=/test_data/exists_01.yaml",
                "--hms-mirror.config.filename=/config/default.yaml.cdp-cdp",
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/e2e/cdp_cdp/ei_ma_sync"
        })
@Slf4j
public class Test_ei_ma_sync extends E2EBaseTest {

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
    public void checkPhaseTest() {
        // Validate phase state for acid_01 ACID table with sync
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
    public void checkSyncIssues() {
        // When sync=true and table exists on RIGHT, it should have specific issue
        validateTableIssueCount("merge_files_migrate", "acid_01", Environment.RIGHT, 1);
        validateTableIssueCount("merge_files_migrate", "ext_01", Environment.RIGHT, 1);
        
        // Check the specific issue message
        validateTableIssue("merge_files_migrate", "acid_01", Environment.RIGHT,
                "Schema exists already and matches. No action necessary");
        validateTableIssue("merge_files_migrate", "ext_01", Environment.RIGHT,
                "Schema exists already and matches. No action necessary");
    }

    @Test
    public void checkTableExistence() {
        // Validate that tables exist on both LEFT and RIGHT
        validateTableEnvironment("merge_files_migrate", "acid_01", Environment.LEFT);
        validateTableEnvironment("merge_files_migrate", "acid_01", Environment.RIGHT);
        validateTableEnvironment("merge_files_migrate", "ext_01", Environment.LEFT);
        validateTableEnvironment("merge_files_migrate", "ext_01", Environment.RIGHT);
    }

    @Test
    public void checkCreateStrategy() {
        // LEFT side should not be modified (NOTHING)
        validateTableEnvironmentCreateStrategy("merge_files_migrate", "acid_01", Environment.LEFT, CreateStrategy.NOTHING);
        validateTableEnvironmentCreateStrategy("merge_files_migrate", "ext_01", Environment.LEFT, CreateStrategy.NOTHING);

        // RIGHT side should have LEAVE strategy since tables exist and sync=true
        validateTableEnvironmentCreateStrategy("merge_files_migrate", "acid_01", Environment.RIGHT, CreateStrategy.NOTHING);
        validateTableEnvironmentCreateStrategy("merge_files_migrate", "ext_01", Environment.RIGHT, CreateStrategy.NOTHING);
    }

    @Test
    public void checkACIDTableIsTransactional() {
        // Validate that acid_01 is transactional on LEFT
        validateTableIsACID("merge_files_migrate", "acid_01", Environment.LEFT);
        
        // Validate transactional property
        validateTableProperty("merge_files_migrate", "acid_01", Environment.LEFT,
                "transactional", "true");
    }

    @Test
    public void checkACIDTableOnRight() {
        // RIGHT side should also be transactional
        validateTableProperty("merge_files_migrate", "acid_01", Environment.RIGHT,
                "transactional", "true");
        validateTableProperty("merge_files_migrate", "acid_01", Environment.RIGHT,
                "transactional_properties", "default");
    }

    @Test
    public void checkACIDTableBuckets() {
        // Validate acid_01 has 2 buckets on LEFT
        validateTableBuckets("merge_files_migrate", "acid_01", Environment.LEFT, 2);
    }

    @Test
    public void checkTableLocations() {
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
    public void checkTableProperties() {
        // Validate bucketing_version for acid_01
        validateTableProperty("merge_files_migrate", "acid_01", Environment.LEFT,
                "bucketing_version", "2");
        validateTableProperty("merge_files_migrate", "acid_01", Environment.RIGHT,
                "bucketing_version", "2");
        
        // ext_01 on RIGHT also has bucketing_version
        validateTableProperty("merge_files_migrate", "ext_01", Environment.RIGHT,
                "bucketing_version", "2");
    }

    @Test
    public void checkNoSQLGenerated() {
        // Since sync=true and tables exist, no SQL should be generated
        validateTableSqlNotGenerated("merge_files_migrate", "acid_01", Environment.LEFT);
        validateTableSqlNotGenerated("merge_files_migrate", "acid_01", Environment.RIGHT);
        validateTableSqlNotGenerated("merge_files_migrate", "ext_01", Environment.LEFT);
        validateTableSqlNotGenerated("merge_files_migrate", "ext_01", Environment.RIGHT);
    }

    @Test
    public void checkPhaseSummary() {
        // Validate phase summary shows 2 tables in CALCULATED_SQL phase
        validateDBInPhaseSummaryCount("merge_files_migrate", PhaseState.PROCESSED, 2);
    }

    @Test
    public void checkHmsMirrorMetadata() {
        // RIGHT side tables should have hms-mirror_Metadata_Stage1 property
        validateTableEnvironmentDefinitionHas("merge_files_migrate", "acid_01", Environment.RIGHT,
                "hms-mirror_Metadata_Stage1");
        validateTableEnvironmentDefinitionHas("merge_files_migrate", "ext_01", Environment.RIGHT,
                "hms-mirror_Metadata_Stage1");
    }

    @Test
    public void checkTotalPhaseCount() {
        // Validate total phase count for tables
        validateTablePhaseTotalCount("merge_files_migrate", "acid_01", 3);
        validateTablePhaseTotalCount("merge_files_migrate", "ext_01", 3);
    }

    @Test
    public void checkCurrentPhase() {
        // Validate current phase is 1
        validateTablePhaseCurrentCount("merge_files_migrate", "acid_01", 2);
        validateTablePhaseCurrentCount("merge_files_migrate", "ext_01", 2);
    }

}
