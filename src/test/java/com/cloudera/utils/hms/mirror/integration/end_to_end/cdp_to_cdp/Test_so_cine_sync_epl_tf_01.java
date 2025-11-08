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

import static com.cloudera.utils.hms.mirror.MirrorConf.ALTER_TABLE_PARTITION_ADD_LOCATION_DESC;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = Mirror.class,
        args = {
                "--hms-mirror.config.create-if-not-exist=true",
//                "--hms-mirror.config.evaluate-partition-location=tru",
//                "--hms-mirror.config.align-locations=true",
                "--hms-mirror.config.table-filter=web_sales",
                "--hms-mirror.conversion.test-filename=/test_data/exists_parts_02.yaml",
                "--hms-mirror.config.filename=/config/default.yaml.cdp-cdp",
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/e2e/cdp_cdp/so_cine_sync_epl_tf_01"
        })
@Slf4j
public class Test_so_cine_sync_epl_tf_01 extends E2EBaseTest {

    @Test
    public void returnCodeTest() {
        // Get Runtime Return Code.
        long rtn = getReturnCode();
        // Verify the return code.
        long check = 0L;
        assertEquals(check, rtn, "Return Code Failure: " + rtn);
    }

    @Test
    public void sqlPairTest() {
        // Validate the SQL Pair.
        // msck off, so ALTER ADD PARTS
        validateTableSqlPair("ext_purge_odd_parts", Environment.RIGHT, "web_sales", ALTER_TABLE_PARTITION_ADD_LOCATION_DESC,
                "ALTER TABLE web_sales ADD IF NOT EXISTS");
    }

    @Test
    public void dbLocationTest() {
        validateDBLocation("ext_purge_odd_parts", Environment.RIGHT,
                "hdfs://HOME90/apps/hive/warehouse/ext_purge_odd_parts.db");
    }

    @Test
    public void tableLocationTest() {
        validateTableLocation("ext_purge_odd_parts", "web_sales", Environment.RIGHT,
                "hdfs://HOME90/warehouse/tablespace/external/hive/ext_purge_odd_parts.db/web_sales");
    }

    @Test
    public void phaseTest() {
        validatePhase("ext_purge_odd_parts", "web_sales", PhaseState.PROCESSED);
    }

    @Test
    public void issueTest() {
        validateTableIssueCount("ext_purge_odd_parts", "web_sales",
                Environment.RIGHT, 2);
    }

}
