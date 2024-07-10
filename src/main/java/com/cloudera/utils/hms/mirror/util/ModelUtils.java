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

import com.cloudera.utils.hive.config.DBStore;
import com.cloudera.utils.hms.mirror.domain.support.ConnectionPoolType;
import com.cloudera.utils.hms.mirror.domain.support.DataStrategyEnum;
import com.cloudera.utils.hms.mirror.web.controller.ControllerReferences;
import org.springframework.ui.Model;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class ModelUtils implements ControllerReferences {

    public static void allEnumsForModel(Model model) {
        enumForModel(com.cloudera.utils.hms.mirror.domain.support.SerdeType.class, model);
        enumForModel(com.cloudera.utils.hms.mirror.domain.support.TableType.class, model);
        enumForModel(com.cloudera.utils.hms.mirror.domain.support.StageEnum.class, model);
        enumForModel(com.cloudera.utils.hms.mirror.domain.support.CollectionEnum.class, model);
        enumForModel(com.cloudera.utils.hms.mirror.domain.support.DataStrategyEnum.class, model);
        enumForModel(com.cloudera.utils.hms.mirror.domain.support.DataMovementStrategyEnum.class, model);
        enumForModel(com.cloudera.utils.hms.mirror.domain.support.DistcpFlowEnum.class, model);
        enumForModel(DBStore.DB_TYPE.class, model);
        configEnvironmentForModel(model);
        configSupportDataStrategyForModel(model);
        configSupportedHiveDriverClassesForModel(model);
        enumForModel(ConnectionPoolType.class, model);
        model.addAttribute("FALSE", "false");
        model.addAttribute("TRUE", "true");
        booleanForModel(model);
    }

    public static void configEnvironmentForModel(Model model) {
        // Add LEFT and RIGHT to the model
        model.addAttribute(ENVIRONMENTS, new String[]{"LEFT", "RIGHT"});
    }

    public static List<DataStrategyEnum> getSupportedDataStrategies() {
        List<DataStrategyEnum> supportedDataStrategies = new ArrayList<>();
        supportedDataStrategies.add(DataStrategyEnum.STORAGE_MIGRATION);
        supportedDataStrategies.add(DataStrategyEnum.DUMP);
        supportedDataStrategies.add(DataStrategyEnum.SCHEMA_ONLY);
        supportedDataStrategies.add(DataStrategyEnum.SQL);
        supportedDataStrategies.add(DataStrategyEnum.EXPORT_IMPORT);
        supportedDataStrategies.add(DataStrategyEnum.HYBRID);
        supportedDataStrategies.add(DataStrategyEnum.COMMON);

        return supportedDataStrategies;
    }

    public static void configSupportDataStrategyForModel(Model model) {
        // Add SUPPORTED and UNSUPPORTED to the model
//        List<String> supportedDataStrategies = new ArrayList<>();
//        for (DataStrategyEnum dataStrategy : getSupportedDataStrategies()) {
//            supportedDataStrategies.add(dataStrategy.name());
//        }
        model.addAttribute(SUPPORTED_DATA_STRATEGIES, getSupportedDataStrategies().toArray(new DataStrategyEnum[0]));
    }

    public static void configSupportedHiveDriverClassesForModel(Model model) {
        // Add SUPPORTED and UNSUPPORTED to the model
        model.addAttribute(SUPPORTED_HIVE_DRIVER_CLASSES,
                new String[]{"org.apache.hive.jdbc.HiveDriver", "com.cloudera.hive.jdbc.HS2Driver"});
    }

    public static void enumForModel(Class clazz, Model model) {
        if (clazz.isEnum()) {
            Method method = null;
            try {
                method = clazz.getMethod("values");
                Enum<?>[] enums = (Enum<?>[]) method.invoke(null);
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
        bools[0] = "false";
        bools[1] = "true";
        model.addAttribute(BOOLEANS, bools);
    }
}
