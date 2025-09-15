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

package com.cloudera.utils.hms.mirror.core.model;

import com.cloudera.utils.hms.mirror.domain.support.DataStrategyEnum;
import com.cloudera.utils.hms.mirror.domain.support.Environment;

import java.util.List;

/**
 * Request object for DistCp plan generation.
 */
public class DistCpPlanRequest {
    private final String database;
    private final List<String> tables;
    private final Environment sourceEnvironment;
    private final Environment targetEnvironment;
    private final DataStrategyEnum dataStrategy;
    private final boolean consolidateTables;
    private final int consolidationLevel;
    private final String outputDirectory;
    private final boolean generateScripts;
    private final boolean generateWorkbook;

    public DistCpPlanRequest(String database, List<String> tables,
                           Environment sourceEnvironment, Environment targetEnvironment,
                           DataStrategyEnum dataStrategy, boolean consolidateTables,
                           int consolidationLevel, String outputDirectory,
                           boolean generateScripts, boolean generateWorkbook) {
        this.database = database;
        this.tables = tables;
        this.sourceEnvironment = sourceEnvironment;
        this.targetEnvironment = targetEnvironment;
        this.dataStrategy = dataStrategy;
        this.consolidateTables = consolidateTables;
        this.consolidationLevel = consolidationLevel;
        this.outputDirectory = outputDirectory;
        this.generateScripts = generateScripts;
        this.generateWorkbook = generateWorkbook;
    }

    public String getDatabase() { return database; }
    public List<String> getTables() { return tables; }
    public Environment getSourceEnvironment() { return sourceEnvironment; }
    public Environment getTargetEnvironment() { return targetEnvironment; }
    public DataStrategyEnum getDataStrategy() { return dataStrategy; }
    public boolean isConsolidateTables() { return consolidateTables; }
    public int getConsolidationLevel() { return consolidationLevel; }
    public String getOutputDirectory() { return outputDirectory; }
    public boolean isGenerateScripts() { return generateScripts; }
    public boolean isGenerateWorkbook() { return generateWorkbook; }
}