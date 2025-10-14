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

import com.cloudera.utils.hms.mirror.domain.core.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.domain.support.SideType;
import com.cloudera.utils.hms.util.ConfigUtils;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConfigUtilsTest {

    @Test
    public void testGetPropertyOverridesFor() {
        // Test case 1
        // Given
        Environment environment = Environment.LEFT;
        HmsMirrorConfig config = new HmsMirrorConfig();
        config.getOptimization().getOverrides().getProperties().put("tez.queue.name", new HashMap<SideType, String>() {{
            put(SideType.LEFT, "queue1");
            put(SideType.RIGHT, "queue2");
        }});
        config.getOptimization().getOverrides().getProperties().put("something.null", new HashMap<SideType, String>() {{
            put(SideType.LEFT, null);
            put(SideType.RIGHT, null);
        }});
        List<String> result = ConfigUtils.getPropertyOverridesFor(environment, config);

        //        assertEquals("SET tez.queue.name=queue1", result);

        // Test case 2
        // Given
        environment = Environment.RIGHT;
        config = new HmsMirrorConfig();
        config.getOptimization().getOverrides().getProperties().put("mapred.job.queue.name", new HashMap<SideType, String>() {{
            put(SideType.LEFT, "queue1");
            put(SideType.RIGHT, "queue2");
        }});
        // When
        result = ConfigUtils.getPropertyOverridesFor(environment, config);
        // Then
//        assertEquals("SET mapred.job.queue.name=queue2", result);

        // Test case 3
        // Given
        environment = Environment.LEFT;
        config = new HmsMirrorConfig();
        config.getOptimization().getOverrides().getProperties().put("tez.queue.name", new HashMap<SideType, String>() {{
            put(SideType.BOTH, "queue1");
        }});
        // When
        result = ConfigUtils.getPropertyOverridesFor(environment, config);
        // Then
//        assertEquals("SET tez.queue.name=queue1", result);

        // Test case 4
        // Given
        environment = Environment.RIGHT;
        config = new HmsMirrorConfig();
        config.getOptimization().getOverrides().getProperties().put("mapred.job.queue.name", new HashMap<SideType, String>() {{
            put(SideType.BOTH, "queue1");
        }});
        config.getOptimization().getOverrides().getProperties().put("something.something", new HashMap<SideType, String>() {{
            put(SideType.BOTH, "BI");
        }});
        config.getOptimization().getOverrides().getProperties().put("another.setting", new HashMap<SideType, String>() {{
            put(SideType.RIGHT, "check");
        }});
        // When
        result = ConfigUtils.getPropertyOverridesFor(environment, config);
        // Then
//        assertEquals("SET mapred.job.queue.name=queue1", result);

        // Test case 5
        // Given
//        environment = Environment.LEFT;
//        config = new HmsMirrorConfig();
//        config.getOptimization().getOverrides().getProperties().put("tez.queue

    }

    @Test
    public void testGetQueuePropertyOverride() {
        // Test case 1
        // Given
        Environment environment = Environment.LEFT;
        HmsMirrorConfig config = new HmsMirrorConfig();
        config.getOptimization().getOverrides().getProperties().put("tez.queue.name", new HashMap<SideType, String>() {{
            put(SideType.LEFT, "queue1");
            put(SideType.RIGHT, "queue2");
        }});
        // When
        String result = ConfigUtils.getQueuePropertyOverride(environment, config);
        // Then
        assertEquals("SET tez.queue.name=queue1", result);

        // Test case 2
        // Given
        environment = Environment.RIGHT;
        config = new HmsMirrorConfig();
        config.getOptimization().getOverrides().getProperties().put("mapred.job.queue.name", new HashMap<SideType, String>() {{
            put(SideType.LEFT, "queue1");
            put(SideType.RIGHT, "queue2");
        }});
        // When
        result = ConfigUtils.getQueuePropertyOverride(environment, config);
        // Then
        assertEquals("SET mapred.job.queue.name=queue2", result);

        // Test case 3
        // Given
        environment = Environment.LEFT;
        config = new HmsMirrorConfig();
        config.getOptimization().getOverrides().getProperties().put("tez.queue.name", new HashMap<SideType, String>() {{
            put(SideType.BOTH, "queue1");
        }});
        // When
        result = ConfigUtils.getQueuePropertyOverride(environment, config);
        // Then
        assertEquals("SET tez.queue.name=queue1", result);

        // Test case 4
        // Given
        environment = Environment.RIGHT;
        config = new HmsMirrorConfig();
        config.getOptimization().getOverrides().getProperties().put("mapred.job.queue.name", new HashMap<SideType, String>() {{
            put(SideType.BOTH, "queue1");
        }});
        config.getOptimization().getOverrides().getProperties().put("something.something", new HashMap<SideType, String>() {{
            put(SideType.BOTH, "BI");
        }});
        // When
        result = ConfigUtils.getQueuePropertyOverride(environment, config);
        // Then
        assertEquals("SET mapred.job.queue.name=queue1", result);

        // Test case 5
        // Given
//        environment = Environment.LEFT;
//        config = new HmsMirrorConfig();
//        config.getOptimization().getOverrides().getProperties().put("tez.queue

    }
}
