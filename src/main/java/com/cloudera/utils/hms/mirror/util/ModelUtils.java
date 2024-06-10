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

package com.cloudera.utils.hms.mirror.util;

import com.cloudera.utils.hms.mirror.domain.support.ConnectionPoolType;
import org.springframework.ui.Model;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class ModelUtils {

    public static void allEnumsForModel(Model model) {
        enumForModel(com.cloudera.utils.hms.mirror.domain.support.SerdeType.class, model);
        enumForModel(com.cloudera.utils.hms.mirror.domain.support.TableType.class, model);
        enumForModel(com.cloudera.utils.hms.mirror.domain.support.StageEnum.class, model);
        enumForModel(com.cloudera.utils.hms.mirror.domain.support.CollectionEnum.class, model);
        enumForModel(com.cloudera.utils.hms.mirror.domain.support.DataStrategyEnum.class, model);
        enumForModel(com.cloudera.utils.hms.mirror.domain.support.DataMovementStrategyEnum.class, model);
        configEnvironmentForModel(model);
        enumForModel(ConnectionPoolType.class, model);
        booleanForModel(model);
    }

    public static void configEnvironmentForModel(Model model) {
        // Add LEFT and RIGHT to the model
        model.addAttribute("environments", new String[]{"LEFT", "RIGHT"});
    }

    public static void enumForModel(Class clazz, Model model) {
        if (clazz.isEnum()) {
            Method method = null;
            try {
                method = clazz.getMethod("values");
                Enum<?>[] enums = (Enum<?>[])method.invoke(null);
                String[] enumNames = new String[enums.length];
                for (int i = 0; i < enums.length; i++) {
                    enumNames[i] = enums[i].name();
                }
                model.addAttribute(clazz.getSimpleName().toLowerCase() + "s", enumNames);
            } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException(e);
            }

        }
    }

    public static void booleanForModel(Model model) {
        String[] bools = new String[2];
        bools[0] = "true";
        bools[1] = "false";
        model.addAttribute("booleans", bools);
    }
}
