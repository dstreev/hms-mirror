/*
 * Copyright (c) 2022-2025. Cloudera, Inc. All Rights Reserved
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

package com.cloudera.utils.hms.mirror.utils;

import com.cloudera.utils.hms.mirror.domain.core.DBMirror;
import com.cloudera.utils.hms.mirror.domain.core.TableMirror;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;

@ExtendWith(MockitoExtension.class)
@Slf4j
public class TestTranslator01 extends TranslatorTestBase {
    private static final String LEFT_HDFS = "hdfs://LEFT";
    private static final String RIGHT_HDFS = "hdfs://RIGHT";
    private static final String TEST_DB_NAME = "tpcds_10";
    private static final String TEST_TABLE_NAME = "call_center";
    private static final String TRANSLATOR_CONFIG = "/translator/testcase_01.yaml";
    private static final String DEFAULT_CONFIG = "/config/default_01.yaml";

    /*
    TODO: Fix
     */
    @BeforeEach
    public void setup1() throws IOException {
        log.info("Setting up TestTranslator01");
    }

    private TableMirror createTestTableMirror() {
        DBMirror dbMirror = new DBMirror();
        dbMirror.setName(TEST_DB_NAME);
        
        TableMirror tableMirror = new TableMirror();
        tableMirror.setName(TEST_TABLE_NAME);
//        tableMirror.setParent(dbMirror);
        tableMirror.getTableDefinition(Environment.RIGHT).add("CREATE EXTERNAL TABLE");
        return tableMirror;
    }

/**
     * Tests for URL manipulation utility functions
     */

    /**
     * Tests for basic table location translation
     */

    /**
     * Test for standard HDFS namespace translation
     */

    /**
     * Test for table location translation consolidation
     */

    /**
     * Test for multiple table location translation patterns
     */
//    @Test
//    public void testMultipleTableLocationPatterns() throws IOException {
//        // Setup common test objects
//        DBMirror dbMirror = new DBMirror();
//        dbMirror.setName(TEST_DB_NAME);
//        TableMirror callCenterTable = new TableMirror();
//        callCenterTable.setName("call_center");
////        callCenterTable.setParent(dbMirror);
//        callCenterTable.getTableDefinition(Environment.RIGHT).add("CREATE EXTERNAL TABLE");
//
//        assertTrue("Couldn't validate translator configuration", translator.validate());
//
//        // Test warehouse path pattern with web_sales
//        assertLocationTranslation(
//            "hdfs://LEFT/tpcds_base_dir5/web_sales",
//            "hdfs://RIGHT/warehouse/tablespace/external/hive/tpcds_10.db/web_sales",
//            dbMirror,
//            callCenterTable
//        );
//
//        // Test base directory 4 with call_center2
//        assertLocationTranslation(
//            "hdfs://LEFT/tpcds_base_dir4/call_center2",
//            "hdfs://RIGHT/warehouse/tablespace/external/hive/tpcds_10.db/call_center2",
//                dbMirror,
//                callCenterTable
//        );
//
//        // Test nested paths with web_returns
//        TableMirror webReturnsTable = new TableMirror();
//        webReturnsTable.setName("web_returns");
////        webReturnsTable.setParent(dbMirror);
//        webReturnsTable.getTableDefinition(Environment.RIGHT).add("CREATE EXTERNAL TABLE");
//
//        assertLocationTranslation(
//            "hdfs://LEFT/tpcds_base_dir4/web/web_returns",
//            "hdfs://RIGHT/warehouse/tablespace/external/hive/tpcds_10.db/web_returns",
//                dbMirror,
//                webReturnsTable
//        );
//
//        // Test web returns path with user directory
//        assertLocationTranslation(
//            "hdfs://LEFT/tpcds_base_dir/web/web_returns2",
//            "hdfs://RIGHT/user/dstreev/datasets/tpcds_11.db/web_returns",
//                dbMirror,
//                webReturnsTable
//        );
//
//        // Test call_center path with different database
//        assertLocationTranslation(
//            "hdfs://LEFT/tpcds_base_dir/web/call_center",
//            "hdfs://RIGHT/warehouse/tablespace/external/hive/tpcds_11.db/call_center",
//                dbMirror,
//                callCenterTable
//        );
//    }
}