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
import com.cloudera.utils.hms.mirror.domain.support.ConversionResult;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.domain.support.SideType;
import com.cloudera.utils.hms.mirror.testutils.ConversionResultTestFactory;
import com.cloudera.utils.hms.util.ConfigUtils;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConfigUtilsTest {

    @Test
    public void testGetPropertyOverridesFor() {
        // Test case 1
        // Given
        ConversionResult conversionResult = ConversionResultTestFactory.createSimpleSchemaOnlyConversion();


        List<String> result = ConfigUtils.getPropertyOverridesFor(Environment.LEFT, conversionResult.getConfig().getOptimization().getOverrides());
        assertTrue(result.contains("SET tez.queue.name=marketing"), "Couldn't locate expected property override");
        result = ConfigUtils.getPropertyOverridesFor(Environment.RIGHT, conversionResult.getConfig().getOptimization().getOverrides());
        assertTrue(result.contains("SET tez.queue.name=finance"), "Couldn't locate expected property override");
        result = ConfigUtils.getPropertyOverridesFor(Environment.LEFT, conversionResult.getConfig().getOptimization().getOverrides());
        assertTrue(result.contains("SET key1=value1"), "Couldn't locate expected property override");
        result = ConfigUtils.getPropertyOverridesFor(Environment.RIGHT, conversionResult.getConfig().getOptimization().getOverrides());
        assertTrue(result.contains("SET key1=value1"), "Couldn't locate expected property override");

        // Test case 2

    }

}
