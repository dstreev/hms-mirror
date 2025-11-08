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

import static com.cloudera.utils.hms.mirror.MirrorConf.ALTER_DB_LOCATION_DESC;
import static com.cloudera.utils.hms.util.TableUtils.REPAIR_DESC;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = Mirror.class,
        args = {
//                "--hms-mirror.config.data-strategy=STORAGE_MIGRATION",
//                "--hms-mirror.storage-migration.namespace=s3a://my_cs_bucket",
//                "--hms-mirror.config.migrate-acid=4",
//                "--hms-mirror.config.warehouse-directory=/warehouse/managed",
//                "--hms-mirror.config.external-warehouse-directory=/warehouse/external",
                "--hms-mirror.conversion.test-filename=/test_data/assorted_tbls_01.yaml",
                "--hms-mirror.config.filename=/config/default.yaml.cdp-cdp",
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/e2e/cdp_cdp/so_01"
        })
@Slf4j
public class Test_so_01 extends E2EBaseTest {

    @Test
    public void returnCodeTest() {
        // Get Runtime Return Code.
        long rtn = getReturnCode();
        // Verify the return code.
        long check = 0L;
        assertEquals(check * -1, rtn, "Return Code Failure: " + rtn);
    }

    @Test
    public void sqlPairTest() {
        // Validate the SQL Pair.
        validateTableSqlPair("assorted_test_db", Environment.RIGHT, "ext_part_01", REPAIR_DESC,
                "MSCK REPAIR TABLE ext_part_01");
        validateDBSqlPair("assorted_test_db", Environment.RIGHT, ALTER_DB_LOCATION_DESC,
                "ALTER DATABASE assorted_test_db SET LOCATION \"hdfs://HOME90/warehouse/tablespace/external/hive/assorted_test_db.db\"");
    }

    @Test
    public void dbLocationTest() {
        validateDBLocation("assorted_test_db", Environment.RIGHT,
                "hdfs://HOME90/warehouse/tablespace/external/hive/assorted_test_db.db");
    }

    @Test
    public void tableLocationTest() {
        validateTableLocation("assorted_test_db", "ext_part_01", Environment.RIGHT,
                "hdfs://HOME90/warehouse/tablespace/external/hive/assorted_test_db.db/ext_part_01");
    }

    @Test
    public void phaseTest() {
        validatePhase("assorted_test_db", "ext_part_01", PhaseState.PROCESSED);
    }

    @Test
    public void issueTest() {
        validateTableIssueCount("assorted_test_db", "ext_part_01",
                Environment.RIGHT, 1);
    }

}
