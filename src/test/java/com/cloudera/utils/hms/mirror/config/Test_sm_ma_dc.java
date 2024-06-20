/*
 * Copyright (c) 2024. Cloudera, Inc. All Rights Reserved
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

package com.cloudera.utils.hms.mirror.config;

import com.cloudera.utils.hms.mirror.MessageCode;
import com.cloudera.utils.hms.mirror.cli.Mirror;
import com.cloudera.utils.hms.mirror.integration.end_to_end.E2EBaseTest;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = Mirror.class,
        args = {
                "--hms-mirror.config.data-strategy=STORAGE_MIGRATION",
                "--hms-mirror.config.migrate-acid=true",
//                "--hms-mirror.config.migrate-acid-only=true",
//                "--hms-mirror.config.warehouse-directory=/warehouse/managed",
//                "--hms-mirror.config.external-warehouse-directory=/warehouse/external",
//                "--hms-mirror.config.sort-dynamic-partition-inserts=true",
//                "--hms-mirror.config.downgrade-acid=true",
//                "--hms-mirror.config.read-only=true",
//                "--hms-mirror.config.sync=true",
                "--hms-mirror.config.evaluate-partition-location=true",
//                "--hms-mirror.config.intermediate-storage=s3a://my_is_bucket",
//                "--hms-mirror.config.common-storage=s3a://my_cs_bucket",
//                "--hms-mirror.config.reset-to-default-location=true",
                "--hms-mirror.config.distcp=true",
                "--hms-mirror.conversion.test-filename=/test_data/assorted_tbls_01.yaml",
                "--hms-mirror.config.filename=/config/default.yaml.hdp2-cdp",
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/config/sm_ma_dc"
        })

@Slf4j
public class Test_sm_ma_dc extends E2EBaseTest {
    //        String[] args = new String[]{"-d", "STORAGE_MIGRATION",
//                "-ma",
//                "-dc",
//                "-ltd", ASSORTED_TBLS_04,
//                "-cfg", HDP2_CDP,
//                "-o", outputDir
//        };
//        long rtn = 0;
//        MirrorLegacy mirror = new MirrorLegacy();
//        rtn = mirror.go(args);
//
//        long check = MessageCode.STORAGE_MIGRATION_REQUIRED_WAREHOUSE_OPTIONS.getLong();
//        check = check | MessageCode.STORAGE_MIGRATION_NAMESPACE_LEFT_MISSING_RDL_GLM.getLong();

    @Test
    public void returnCodeTest() {
        // Get Runtime Return Code.
        long actual = getReturnCode();
        // Verify the return code.
        long expected = getCheckCode(
                MessageCode.STORAGE_MIGRATION_REQUIRED_WAREHOUSE_OPTIONS,
                MessageCode.STORAGE_MIGRATION_NAMESPACE_LEFT_MISSING_RDL_GLM,
                MessageCode.METASTORE_DIRECT_CONFIG,
                MessageCode.STORAGE_MIGRATION_NOT_AVAILABLE_FOR_LEGACY);

        assertEquals("Return Code Failure: ", expected, actual);

    }

    @Test
    public void warningCodeTest() {
        // Get Runtime Return Code.
        long actual = getWarningCode();
        // Verify the return code.
        long expected = getCheckCode(
                MessageCode.STORAGE_MIGRATION_DISTCP_EXECUTE,
                MessageCode.STORAGE_MIGRATION_NAMESPACE_LEFT,
                MessageCode.DISTCP_WO_TABLE_FILTERS
        );

        assertEquals("Warning Code Failure: ", expected, actual);

    }


}
