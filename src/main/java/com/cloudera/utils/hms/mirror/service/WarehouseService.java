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
 */
package com.cloudera.utils.hms.mirror.service;

import com.cloudera.utils.hms.mirror.domain.core.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.core.Warehouse;
import com.cloudera.utils.hms.mirror.domain.core.WarehouseMapBuilder;
import com.cloudera.utils.hms.mirror.domain.dto.ConfigLiteDto;
import com.cloudera.utils.hms.mirror.domain.dto.JobDto;
import com.cloudera.utils.hms.mirror.domain.support.ConversionResult;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.domain.support.ExecuteSession;
import com.cloudera.utils.hms.mirror.domain.support.RunStatus;
import com.cloudera.utils.hms.mirror.exceptions.MissingDataPointException;
import com.cloudera.utils.hms.mirror.exceptions.RequiredConfigurationException;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;

import static com.cloudera.utils.hms.mirror.MessageCode.WAREHOUSE_DIRECTORIES_NOT_DEFINED;
import static com.cloudera.utils.hms.mirror.MessageCode.WAREHOUSE_DIRECTORIES_RETRIEVED_FROM_HIVE_ENV;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * Service for managing warehouse plans associated with Hive databases.
 * Provides methods for adding, removing, retrieving, and clearing warehouse plans.
 */
@Service
@Slf4j
@Getter
@RequiredArgsConstructor
public class WarehouseService {

    private static final String EXTERNAL_AND_MANAGED_LOCATIONS_REQUIRED =
            "External and Managed Warehouse Locations must be defined.";
    private static final String EXTERNAL_AND_MANAGED_LOCATIONS_DIFFERENT =
            "External and Managed Warehouse Locations must be different.";
    private static final String WAREHOUSE_PLAN_NOT_FOUND_MSG =
            "Warehouse Plan for Database: %s not found and couldn't be built from (Warehouse Plans, General Warehouse Configs or Hive ENV.";
    @NonNull
    private final ExecutionContextService executionContextService;

    /**
     * Adds a warehouse plan for a given database.
     *
     * @param database the name of the database.
     * @param external the external warehouse location.
     * @param managed  the managed warehouse location.
     * @return the created {@link Warehouse} plan.
     * @throws RequiredConfigurationException if external or managed locations are blank or identical.
    public Warehouse addWarehousePlan(String database, String external, String managed)
            throws RequiredConfigurationException {
        if (isBlank(external) || isBlank(managed)) {
            throw new RequiredConfigurationException(EXTERNAL_AND_MANAGED_LOCATIONS_REQUIRED);
        }
        if (external.equals(managed)) {
            throw new RequiredConfigurationException(EXTERNAL_AND_MANAGED_LOCATIONS_DIFFERENT);
        }
        ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
                new IllegalStateException("ConversionResult not set in the current thread context."));
        ConfigLiteDto config = conversionResult.getConfig();
        JobDto job = conversionResult.getJob();
        RunStatus runStatus = conversionResult.getRunStatus();

//        HmsMirrorConfig config = executeSessionService.getSession().getConfig();
        WarehouseMapBuilder mapBuilder = getWarehouseMapBuilder(conversionResult);
        mapBuilder.addWarehousePlan(database, external, managed);

        // TODO: Need to fix IF this is still relevant.
//        config.getDatabases().add(database);
        return mapBuilder.addWarehousePlan(database, external, managed);
    }
     */

    /**
     * Removes a warehouse plan for a given database.
     *
     * @param database the name of the database whose warehouse plan will be removed.
     * @return the removed {@link Warehouse} plan, or null if not found.
    public Warehouse removeWarehousePlan(String database) {
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();
        getWarehouseMapBuilder(config).removeWarehousePlan(database);
        config.getDatabases().remove(database);
        return getWarehouseMapBuilder(config).removeWarehousePlan(database);
    }
     */

    /**
     * Retrieves a warehouse plan for the given database, with fallbacks to configuration or hive environment.
     *
     * @param database the name of the database.
     * @return the {@link Warehouse} plan for this database.
     * @throws MissingDataPointException if a plan cannot be found or built using available configuration.
     */
    public Warehouse getWarehousePlan(String database) throws MissingDataPointException {
        ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
                new IllegalStateException("ConversionResult not set in the current thread context."));
        ConfigLiteDto config = conversionResult.getConfig();
        JobDto job = conversionResult.getJob();
        RunStatus runStatus = conversionResult.getRunStatus();

//        HmsMirrorConfig config = executeSessionService.getSession().getConfig();
        WarehouseMapBuilder warehouseMapBuilder = getWarehouseMapBuilder(conversionResult);
        Warehouse warehouse = warehouseMapBuilder.getWarehousePlans().get(database);

        if (isNull(warehouse)) {
//            ExecuteSession session = executeSessionService.getSession();
            if (nonNull(config.getTransfer().getWarehouse())) {
                warehouse = config.getTransfer().getWarehouse();
            }
            if (nonNull(warehouse) &&
                    (isBlank(warehouse.getExternalDirectory()) || isBlank(warehouse.getManagedDirectory()))) {
                warehouse = null;
            }
            if (isNull(warehouse)) {
                switch (job.getStrategy()) {
                    case DUMP:
                        return null;
                    case SCHEMA_ONLY:
                    case EXPORT_IMPORT:
                    case HYBRID:
                    case SQL:
                    case COMMON:
                    case LINKED:
                        warehouse = conversionResult.getConnection(Environment.RIGHT).getWarehouse(); // TODO: Check this. getEnvironmentWarehouse();
                        if (nonNull(warehouse)) {
                            runStatus.addWarning(WAREHOUSE_DIRECTORIES_RETRIEVED_FROM_HIVE_ENV);
                        } else {
                            runStatus.addWarning(WAREHOUSE_DIRECTORIES_NOT_DEFINED);
                        }
                        break;
                    default:
                        runStatus.addWarning(WAREHOUSE_DIRECTORIES_NOT_DEFINED);
                }
            }
        }

        if (isNull(warehouse)) {
            throw new MissingDataPointException(String.format(WAREHOUSE_PLAN_NOT_FOUND_MSG, database));
        }
        return warehouse;
    }

    /**
     * Gets all defined warehouse plans.
     *
     * @return a map of database names to {@link Warehouse} plans.
     */
    public Map<String, Warehouse> getWarehousePlans() {
        ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
                new IllegalStateException("ConversionResult not set."));
        return getWarehouseMapBuilder(conversionResult).getWarehousePlans();
    }

    /**
     * Removes all defined warehouse plans.
    public void clearWarehousePlans() {
//        HmsMirrorConfig config = executeSessionService.getSession().getConfig();
        ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
                new IllegalStateException("ConversionResult not set."));
        getWarehouseMapBuilder(conversionResult).clearWarehousePlan();
    }
     */

    /**
     * Retrieves the {@link WarehouseMapBuilder} instance from the configuration.
     *
     * @param config the HMS mirror configuration.
     * @return the {@link WarehouseMapBuilder} for the current configuration.
     */
    private WarehouseMapBuilder getWarehouseMapBuilder(ConversionResult conversionResult) {
        return conversionResult.getTranslator().getWarehouseMapBuilder();
    }
}