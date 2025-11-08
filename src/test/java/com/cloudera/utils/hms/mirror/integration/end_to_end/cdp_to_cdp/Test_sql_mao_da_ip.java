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
import com.cloudera.utils.hms.util.TableUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static com.cloudera.utils.hms.mirror.MirrorConf.RENAME_TABLE_DESC;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = Mirror.class,
        args = {
                "--hms-mirror.config.data-strategy=SQL",
                "--hms-mirror.config.migrate-acid-only=true",
                "--hms-mirror.config.downgrade-acid=true",
                "--hms-mirror.config.in-place=true",
                "--hms-mirror.conversion.test-filename=/test_data/assorted_tbls_01.yaml",
                "--hms-mirror.config.filename=/config/default.yaml.cdp-cdp",
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/e2e/cdp_cdp/sql_mao_da_ip"
        })
@Slf4j
public class Test_sql_mao_da_ip extends E2EBaseTest {


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
        validateTableSqlPair("assorted_test_db", Environment.LEFT, "acid_01", TableUtils.STORAGE_MIGRATION_TRANSFER_DESC,
                "FROM acid_01_archive INSERT OVERWRITE TABLE acid_01 SELECT *");
        validateTableSqlPair("assorted_test_db", Environment.LEFT, "acid_01", RENAME_TABLE_DESC,
                "ALTER TABLE acid_01 RENAME TO acid_01_archive");
    }

    @Test
    public void tableLocationTest() {
        validateTableLocation("assorted_test_db", "acid_01", Environment.RIGHT,
                null);
    }

}
