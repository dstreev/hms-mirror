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

package com.cloudera.utils.hms.mirror.integration.end_to_end.cdp_to_cdp;

import com.cloudera.utils.hms.mirror.MessageCode;
import com.cloudera.utils.hms.mirror.cli.Mirror;
import com.cloudera.utils.hms.mirror.integration.end_to_end.E2EBaseTest;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.BitSet;

import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = Mirror.class,
        args = {
                "--hms-mirror.config.data-strategy=HYBRID",
                "--hms-mirror.config.migrate-acid-only=true",
                "--hms-mirror.config.downgrade-acid=true",
                "--hms-mirror.config.read-only=true",
                "--hms-mirror.config.target-namespace=s3a://my_cs_bucket",
                "--hms-mirror.conversion.test-filename=/test_data/assorted_tbls_01.yaml",
                "--hms-mirror.config.filename=/config/default.yaml.cdp-cdp",
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/e2e/cdp_cdp/hybrid_mao_da_ro_cs"
        })
@Slf4j
public class Test_hybrid_mao_da_ro_cs extends E2EBaseTest {
    //        String[] args = new String[]{"-d", "HYBRID",
//                "-mao", "-da", "-ro", "-cs", TARGET_NAMESPACE,
//                "-ltd", ASSORTED_TBLS_04,
//                "-cfg", CDP_CDP,
//                "-o", outputDir
//        };
//
//        long rtn = 0;
//        MirrorLegacy mirror = new MirrorLegacy();
//        rtn = mirror.go(args);
//        int check = 0;
//        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check * -1, check * -1, rtn);
    @Test
    public void returnCodeTest() {
        // Get Runtime Return Code.
        long actual = getReturnCode();
        // Verify the return code.
        BitSet bitSet = new BitSet(150);
        long expected = 0;
        bitSet.set(MessageCode.RO_VALID_STRATEGIES.getCode());
        long[] messageSet = bitSet.toLongArray();
        for (long messageBit : messageSet) {
            expected = expected | messageBit;
        }

        assertEquals("Return Code Failure: ", expected * -1, actual);
    }

//    @Test
//    public void phaseTest() {
//        validatePhase("ext_purge_odd_parts", "web_sales", PhaseState.SUCCESS);
//    }
//
//    @Test
//    public void issueTest() {
//        validateTableIssueCount("ext_purge_odd_parts", "web_sales",
//                Environment.LEFT, 17);
//    }

}
