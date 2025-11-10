/*
 * Copyright (c) 2025. Cloudera, Inc. All Rights Reserved
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

package com.cloudera.utils.hms.utils;

import com.cloudera.utils.hms.mirror.domain.legacy.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.service.DatabaseService;
import com.cloudera.utils.hms.util.DatabaseUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DatabaseUtilsTest {

    private static final Map<String, String> parameters = new HashMap<>();

    @BeforeEach
    public void setup() {
        parameters.put("dont.move.me", "1");
        parameters.put("repl.test1", "2");
        parameters.put("repl.test2", "3");
        parameters.put("skip3", "4");

    }

    @Test
    public void dbPropertiesSkipTest_01() {
        HmsMirrorConfig config = new HmsMirrorConfig();
        config.getFilter().addDbPropertySkipItem("dont\\.move\\.me");
        config.getFilter().addDbPropertySkipItem("repl\\.test.*");
        Map<String, String> results = DatabaseUtils.getParameters(parameters, DatabaseService.skipList, config.getFilter().getDbPropertySkipListPattern());
        // We should clear out all but the 'skip3' entry
        assertEquals(1, results.size());
        assertEquals("4", results.get("skip3"));
    }

    @Test
    public void dbPropertiesSkipTest_02() {
        HmsMirrorConfig config = new HmsMirrorConfig();
        config.getFilter().getDbPropertySkipList().add("dont\\.move\\.me");
        config.getFilter().getDbPropertySkipList().add("repl\\.test.*");
        Map<String, String> results = DatabaseUtils.getParameters(parameters, DatabaseService.skipList, config.getFilter().getDbPropertySkipListPattern());
        // We should clear out all but the 'skip3' entry
        assertEquals(1, results.size());
        assertEquals("4", results.get("skip3"));
    }

}
