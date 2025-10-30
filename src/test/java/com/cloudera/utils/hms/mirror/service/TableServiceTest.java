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

package com.cloudera.utils.hms.mirror.service;

import com.cloudera.utils.hadoop.cli.CliEnvironment;
import com.cloudera.utils.hms.mirror.Marker;
import com.cloudera.utils.hms.mirror.core.api.TableOperations;
import com.cloudera.utils.hms.mirror.domain.core.*;
import com.cloudera.utils.hms.mirror.domain.dto.ConfigLiteDto;
import com.cloudera.utils.hms.mirror.domain.dto.ConnectionDto;
import com.cloudera.utils.hms.mirror.domain.dto.DatasetDto;
import com.cloudera.utils.hms.mirror.domain.dto.JobDto;
import com.cloudera.utils.hms.mirror.domain.support.*;
import com.cloudera.utils.hms.mirror.repository.TableMirrorRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * Test class for TableService.checkTableFilter() method.
 *
 * Tests validation logic for different table types and migration scenarios:
 * - ACID table validation (with migrateACID on/off/only)
 * - VIEW validation (with migrateVIEW flag)
 * - Non-native table validation
 * - Storage migration flag checking
 * - Table size limit validation
 */
@ExtendWith(MockitoExtension.class)
public class TableServiceTest {

    @Mock
    private ConfigService configService;

    @Mock
    private ExecutionContextService executionContextService;

    @Mock
    private ConversionResultService conversionResultService;

    @Mock
    private ConnectionPoolService connectionPoolService;

    @Mock
    private QueryDefinitionsService queryDefinitionsService;

    @Mock
    private TranslatorService translatorService;

    @Mock
    private StatsCalculatorService statsCalculatorService;

    @Mock
    private TableOperations tableOperations;

    @Mock
    private CliEnvironment cliEnvironment;

    @Mock
    private TableMirrorRepository tableMirrorRepository;

    @InjectMocks
    private TableService tableService;

    private ConversionResult conversionResult;
    private ConfigLiteDto config;
    private JobDto job;
    private RunStatus runStatus;
    private DatasetDto dataset;
    private DBMirror dbMirror;
    private TableMirror tableMirror;
    private EnvironmentTable environmentTable;

    @BeforeEach
    void setUp() {
        // Create test fixtures
        conversionResult = new ConversionResult();
        conversionResult.setKey("test-conversion-key");

        config = new ConfigLiteDto();
        config.setMigrateACID(new MigrateACID());
        config.setMigrateVIEW(new MigrateVIEW());
        config.setMigrateNonNative(false);
        config.setOptimization(new Optimization());

        job = new JobDto();
        job.setName("test-job");
        job.setStrategy(DataStrategyEnum.SCHEMA_ONLY);

        runStatus = new RunStatus();

        dataset = new DatasetDto();
        dataset.setName("test-dataset");
        dataset.setDatabases(new ArrayList<>());

        // Create database spec
        DatasetDto.DatabaseSpec dbSpec = new DatasetDto.DatabaseSpec();
        dbSpec.setDatabaseName("test_db");
        DatasetDto.TableFilter filter = new DatasetDto.TableFilter();
        filter.setMaxSizeMb(0);
        filter.setMaxPartitions(0);
        dbSpec.setFilter(filter);
        dataset.getDatabases().add(dbSpec);

        // Set up ConversionResult
        conversionResult.setConfig(config);
        conversionResult.setJob(job);
        conversionResult.setRunStatus(runStatus);
        conversionResult.setDataset(dataset);

        // Mock connections
        ConnectionDto leftConn = new ConnectionDto();
        leftConn.setName("left-conn");
        conversionResult.setConnection(Environment.LEFT, leftConn);

        ConnectionDto rightConn = new ConnectionDto();
        rightConn.setName("right-conn");
        conversionResult.setConnection(Environment.RIGHT, rightConn);

        // Create DBMirror and TableMirror
        dbMirror = new DBMirror();
        dbMirror.setName("test_db");

        tableMirror = new TableMirror();
        tableMirror.setName("test_table");

        environmentTable = new EnvironmentTable();
        environmentTable.setName("test_table");
        environmentTable.setDefinition(createTableDefinition(false, false, false));
        tableMirror.getEnvironments().put(Environment.LEFT, environmentTable);

        // Mock ExecutionContextService to return ConversionResult
        when(executionContextService.getConversionResult()).thenReturn(Optional.of(conversionResult));
    }

    /**
     * Test that a valid ACID table passes validation when migrateACID is enabled.
     * The TRANSACTIONAL step should be added to the table mirror.
     */
    @Test
    void testCheckTableFilter_ValidACIDTable() {
        // Given: ACID managed table with migrateACID enabled
        config.getMigrateACID().setOn(true);
        environmentTable.setDefinition(createTableDefinition(true, true, false));

        // When: checkTableFilter is called
        tableService.checkTableFilter(dbMirror, tableMirror, Environment.LEFT);

        // Then: table should not be marked for removal and TRANSACTIONAL step added
        assertThat(tableMirror.isRemove()).isFalse();
        assertThat(tableMirror.getRemoveReason()).isNull();
        assertThat(hasStep(tableMirror, "TRANSACTIONAL")).isTrue();
    }

    /**
     * Test that an ACID table is marked for removal when migrateACID is disabled.
     */
    @Test
    void testCheckTableFilter_ACIDTableWithoutMigration() {
        // Given: ACID managed table with migrateACID disabled
        config.getMigrateACID().setOn(false);
        environmentTable.setDefinition(createTableDefinition(true, true, false));

        // When: checkTableFilter is called
        tableService.checkTableFilter(dbMirror, tableMirror, Environment.LEFT);

        // Then: table should be marked for removal
        assertThat(tableMirror.isRemove()).isTrue();
        assertThat(tableMirror.getRemoveReason())
                .isEqualTo("ACID table and ACID processing not selected (-ma|-mao)");
    }

    /**
     * Test that a non-ACID table is marked for removal when ACID-only mode is enabled.
     */
    @Test
    void testCheckTableFilter_NonACIDTableWithACIDOnlyMode() {
        // Given: Non-ACID managed table with ACID-only mode
        config.getMigrateACID().setOnly(true);
        environmentTable.setDefinition(createTableDefinition(true, false, false));

        // When: checkTableFilter is called
        tableService.checkTableFilter(dbMirror, tableMirror, Environment.LEFT);

        // Then: table should be marked for removal
        assertThat(tableMirror.isRemove()).isTrue();
        assertThat(tableMirror.getRemoveReason())
                .isEqualTo("Non-ACID table and ACID only processing selected `-mao`");
    }

    /**
     * Test that a valid VIEW passes validation when migrateVIEW is enabled.
     */
    @Test
    void testCheckTableFilter_ValidView() {
        // Given: VIEW with migrateVIEW enabled and non-DUMP strategy
        config.getMigrateVIEW().setOn(true);
        job.setStrategy(DataStrategyEnum.SCHEMA_ONLY);
        environmentTable.setDefinition(createTableDefinition(false, false, true));

        // When: checkTableFilter is called
        tableService.checkTableFilter(dbMirror, tableMirror, Environment.LEFT);

        // Then: table should not be marked for removal
        assertThat(tableMirror.isRemove()).isFalse();
        assertThat(tableMirror.getRemoveReason()).isNull();
    }

    /**
     * Test that a VIEW is marked for removal when migrateVIEW is enabled but table is not a VIEW.
     */
    @Test
    void testCheckTableFilter_ViewOnlyModeWithNonView() {
        // Given: Non-VIEW table with migrateVIEW enabled
        config.getMigrateVIEW().setOn(true);
        job.setStrategy(DataStrategyEnum.SCHEMA_ONLY);
        environmentTable.setDefinition(createTableDefinition(false, false, false));

        // When: checkTableFilter is called
        tableService.checkTableFilter(dbMirror, tableMirror, Environment.LEFT);

        // Then: table should be marked for removal
        assertThat(tableMirror.isRemove()).isTrue();
        assertThat(tableMirror.getRemoveReason())
                .isEqualTo("VIEW's only processing selected, but table is not a view");
    }

    /**
     * Test that a VIEW without migrateVIEW is marked for removal (non-DUMP strategy).
     */
    @Test
    void testCheckTableFilter_ViewWithoutMigration() {
        // Given: VIEW without migrateVIEW enabled
        config.getMigrateVIEW().setOn(false);
        job.setStrategy(DataStrategyEnum.SCHEMA_ONLY);
        environmentTable.setDefinition(createTableDefinition(false, false, true));

        // When: checkTableFilter is called
        tableService.checkTableFilter(dbMirror, tableMirror, Environment.LEFT);

        // Then: table should be marked for removal
        assertThat(tableMirror.isRemove()).isTrue();
        assertThat(tableMirror.getRemoveReason())
                .isEqualTo("This is a VIEW and VIEW processing wasn't selected");
    }

    /**
     * Test that an external ACID-only mode marks non-ACID external tables for removal.
     */
    @Test
    void testCheckTableFilter_ExternalTableWithACIDOnlyMode() {
        // Given: External non-ACID table with ACID-only mode
        config.getMigrateACID().setOnly(true);
        environmentTable.setDefinition(createTableDefinition(false, false, false));

        // When: checkTableFilter is called
        tableService.checkTableFilter(dbMirror, tableMirror, Environment.LEFT);

        // Then: table should be marked for removal
        assertThat(tableMirror.isRemove()).isTrue();
        assertThat(tableMirror.getRemoveReason())
                .isEqualTo("Non-ACID table and ACID only processing selected `-mao`");
    }

    /**
     * Test that a non-native table is marked for removal when migrateNonNative is disabled.
     */
    @Test
    void testCheckTableFilter_NonNativeTableWithoutMigration() {
        // Given: Non-native table without migrateNonNative
        config.setMigrateNonNative(false);
        List<String> nonNativeDefinition = Arrays.asList(
                "CREATE EXTERNAL TABLE test_table (",
                "  col1 STRING",
                ")",
                "STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'"
        );
        environmentTable.setDefinition(nonNativeDefinition);

        // When: checkTableFilter is called
        tableService.checkTableFilter(dbMirror, tableMirror, Environment.LEFT);

        // Then: table should be marked for removal
        assertThat(tableMirror.isRemove()).isTrue();
        assertThat(tableMirror.getRemoveReason())
                .isEqualTo("This is a Non-Native hive table and non-native process wasn't selected");
    }

    /**
     * Test that a non-native table passes validation when migrateNonNative is enabled.
     */
    @Test
    void testCheckTableFilter_NonNativeTableWithMigration() {
        // Given: Non-native table with migrateNonNative enabled
        config.setMigrateNonNative(true);
        List<String> nonNativeDefinition = Arrays.asList(
                "CREATE EXTERNAL TABLE test_table (",
                "  col1 STRING",
                ")",
                "STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'"
        );
        environmentTable.setDefinition(nonNativeDefinition);

        // When: checkTableFilter is called
        tableService.checkTableFilter(dbMirror, tableMirror, Environment.LEFT);

        // Then: table should not be marked for removal
        assertThat(tableMirror.isRemove()).isFalse();
        assertThat(tableMirror.getRemoveReason()).isNull();
    }

    /**
     * Test that a table with storage migration flag is marked for removal in STORAGE_MIGRATION strategy.
     */
    @Test
    void testCheckTableFilter_StorageMigrationFlagAlreadySet() {
        // Given: Table with storage migration flag and STORAGE_MIGRATION strategy
        job.setStrategy(DataStrategyEnum.STORAGE_MIGRATION);
        List<String> definition = Arrays.asList(
                "CREATE TABLE test_table (",
                "  col1 STRING",
                ")",
                "TBLPROPERTIES (",
                "  'hmsMirror_Storage_Migration_Flag'='2025-01-15 10:30:00'",
                ")"
        );
        environmentTable.setDefinition(definition);

        // When: checkTableFilter is called
        tableService.checkTableFilter(dbMirror, tableMirror, Environment.LEFT);

        // Then: table should be marked for removal
        assertThat(tableMirror.isRemove()).isTrue();
        assertThat(tableMirror.getRemoveReason())
                .contains("has already gone through the STORAGE_MIGRATION process");
    }

    /**
     * Test that a table exceeding size limit is marked for removal.
     */
    @Test
    void testCheckTableFilter_TableExceedsSizeLimit() {
        // Given: Table with data size exceeding filter limit
        DatasetDto.DatabaseSpec dbSpec = dataset.getDatabases().get(0);
        dbSpec.getFilter().setMaxSizeMb(100); // 100 MB limit

        // Set table data size to 200 MB
        environmentTable.getStatistics().put("data_size", 200L * 1024 * 1024);

        // When: checkTableFilter is called
        tableService.checkTableFilter(dbMirror, tableMirror, Environment.LEFT);

        // Then: table should be marked for removal
        assertThat(tableMirror.isRemove()).isTrue();
        assertThat(tableMirror.getRemoveReason())
                .contains("table dataset size exceeds the specified table filter size limit");
    }

    /**
     * Test that a table within size limit is not marked for removal.
     */
    @Test
    void testCheckTableFilter_TableWithinSizeLimit() {
        // Given: Table with data size within filter limit
        DatasetDto.DatabaseSpec dbSpec = dataset.getDatabases().get(0);
        dbSpec.getFilter().setMaxSizeMb(100); // 100 MB limit

        // Set table data size to 50 MB
        environmentTable.getStatistics().put("data_size", 50L * 1024 * 1024);

        // When: checkTableFilter is called
        tableService.checkTableFilter(dbMirror, tableMirror, Environment.LEFT);

        // Then: table should not be marked for removal
        assertThat(tableMirror.isRemove()).isFalse();
        assertThat(tableMirror.getRemoveReason()).isNull();
    }

    /**
     * Test that an empty EnvironmentTable passes validation.
     */
    @Test
    void testCheckTableFilter_EmptyEnvironmentTable() {
        // Given: Empty EnvironmentTable
        environmentTable.setDefinition(new ArrayList<>());

        // When: checkTableFilter is called
        tableService.checkTableFilter(dbMirror, tableMirror, Environment.LEFT);

        // Then: table should not be marked for removal (nothing to validate)
        assertThat(tableMirror.isRemove()).isFalse();
        assertThat(tableMirror.getRemoveReason()).isNull();
    }

    /**
     * Test that TRANSACTIONAL step is only added for ACID managed tables.
     */
    @Test
    void testCheckTableFilter_TransactionalStepOnlyForACIDTables() {
        // Given: Non-ACID external table
        config.getMigrateACID().setOn(true);
        environmentTable.setDefinition(createTableDefinition(false, false, false));

        // When: checkTableFilter is called
        tableService.checkTableFilter(dbMirror, tableMirror, Environment.LEFT);

        // Then: TRANSACTIONAL step should NOT be added
        assertThat(tableMirror.isRemove()).isFalse();
        assertThat(hasStep(tableMirror, "TRANSACTIONAL")).isFalse();
    }

    /**
     * Test VIEW in DUMP strategy (should not be marked for removal).
     */
    @Test
    void testCheckTableFilter_ViewInDumpStrategy() {
        // Given: VIEW with DUMP strategy (migrateVIEW can be off)
        config.getMigrateVIEW().setOn(false);
        job.setStrategy(DataStrategyEnum.DUMP);
        environmentTable.setDefinition(createTableDefinition(false, false, true));

        // When: checkTableFilter is called
        tableService.checkTableFilter(dbMirror, tableMirror, Environment.LEFT);

        // Then: table should not be marked for removal (DUMP allows VIEWs)
        assertThat(tableMirror.isRemove()).isFalse();
        assertThat(tableMirror.getRemoveReason()).isNull();
    }

    // Helper method to check if a table has a specific step
    private boolean hasStep(TableMirror tableMirror, String stepDescription) {
        return tableMirror.getSteps().stream()
                .anyMatch(marker -> marker.getDescription().equals(stepDescription));
    }

    // Helper method to create table definitions
    private List<String> createTableDefinition(boolean managed, boolean acid, boolean view) {
        List<String> definition = new ArrayList<>();

        if (view) {
            definition.add("CREATE VIEW test_table AS");
            definition.add("SELECT * FROM base_table");
        } else if (managed) {
            definition.add("CREATE TABLE test_table (");
            definition.add("  col1 STRING,");
            definition.add("  col2 INT");
            definition.add(")");
            if (acid) {
                definition.add("TBLPROPERTIES (");
                definition.add("  'transactional'='true'");
                definition.add(")");
            }
        } else {
            definition.add("CREATE EXTERNAL TABLE test_table (");
            definition.add("  col1 STRING,");
            definition.add("  col2 INT");
            definition.add(")");
            definition.add("LOCATION 'hdfs://namenode:8020/data/test_table'");
        }

        return definition;
    }
}
