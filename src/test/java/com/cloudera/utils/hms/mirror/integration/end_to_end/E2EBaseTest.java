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

package com.cloudera.utils.hms.mirror.integration.end_to_end;

import com.cloudera.utils.hms.mirror.CreateStrategy;
import com.cloudera.utils.hms.mirror.MessageCode;
import com.cloudera.utils.hms.mirror.Pair;
import com.cloudera.utils.hms.mirror.PhaseState;
import com.cloudera.utils.hms.mirror.domain.core.DBMirror;
import com.cloudera.utils.hms.mirror.domain.core.EnvironmentTable;
import com.cloudera.utils.hms.mirror.domain.core.TableMirror;
import com.cloudera.utils.hms.mirror.domain.dto.ConfigLiteDto;
import com.cloudera.utils.hms.mirror.domain.support.ConversionResult;
import com.cloudera.utils.hms.mirror.domain.support.DataStrategyEnum;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.exceptions.RepositoryException;
import com.cloudera.utils.hms.mirror.repository.DBMirrorRepository;
import com.cloudera.utils.hms.mirror.repository.TableMirrorRepository;
import com.cloudera.utils.hms.mirror.service.DomainService;
import com.cloudera.utils.hms.mirror.service.ExecutionContextService;
import com.cloudera.utils.hms.mirror.service.HMSMirrorAppService;
import com.cloudera.utils.hms.util.TableUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ActiveProfiles;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.cloudera.utils.hms.mirror.MessageCode.SCHEMA_EXISTS_TARGET_MISMATCH;
import static com.cloudera.utils.hms.mirror.MirrorConf.DB_LOCATION;
import static com.cloudera.utils.hms.mirror.MirrorConf.DB_MANAGED_LOCATION;
import static org.junit.jupiter.api.Assertions.*;

@Slf4j
@Getter
@ActiveProfiles({"no-cli", "test"})
public class E2EBaseTest {

    protected DomainService domainService;
    protected HMSMirrorAppService hmsMirrorAppService;
    protected ExecutionContextService executionContextService;
    protected DBMirrorRepository dbMirrorRepository;
    protected TableMirrorRepository tableMirrorRepository;
//    protected ExecuteSessionService executeSessionService;

    @Autowired
    public void setDomainService(DomainService domainService) {
        this.domainService = domainService;
    }

    @Autowired
    public void setExecutionContextService(ExecutionContextService executionContextService) {
        this.executionContextService = executionContextService;
    }

    @Autowired
    public void setHmsMirrorAppService(HMSMirrorAppService hmsMirrorAppService) {
        this.hmsMirrorAppService = hmsMirrorAppService;
    }

    @Autowired
    public void setDbMirrorRepository(DBMirrorRepository dbMirrorRepository) {
        this.dbMirrorRepository = dbMirrorRepository;
    }

    @Autowired
    public void setTableMirrorRepository(TableMirrorRepository tableMirrorRepository) {
        this.tableMirrorRepository = tableMirrorRepository;
    }

    @Test
    public void checkTableLocationsForNull() {
        if (getReturnCode() < 0) {
            log.info("LOCATION CHECK Skipping - return code is less than 0");
            return;
        }

        ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
                new RuntimeException("Conversion result is not available"));

        Map<String, DBMirror> dbMirrorMap = null;
        try {
            dbMirrorMap = getDbMirrorRepository().findByConversionKey(conversionResult.getKey());
        } catch (RepositoryException e) {
            throw new RuntimeException(e);
        }

        dbMirrorMap.forEach((dbName, dbMirror) -> {
            dbMirror.getProperties().forEach((environment, properties) -> {
                // Find the LOCATION property for each environment and check if there is a 'null' string in it.
                if (properties.containsKey(DB_LOCATION)) {
                    String location = properties.get(DB_LOCATION);
                    if (location != null) {
                        log.warn("LOCATION CHECK for DB Location: {}:{} - {}", environment, dbName, location);
                        if (location.contains("null")) {
                            fail("Location contains a null value for database: " + dbName +
                                    " location: " + location);
                        }
                    }
                }
                // Find the MANAGED LOCATION property for each environment and check if there is a 'null' string in it.
                if (properties.containsKey(DB_MANAGED_LOCATION)) {
                    String managedLocation = properties.get(DB_MANAGED_LOCATION);
                    if (managedLocation != null) {
                        log.warn("LOCATION CHECK for DB Managed Location for: {}:{} - {}", environment, dbName, managedLocation);
                        if (managedLocation.contains("null")) {
                            fail("Managed location contains a null value for database: " + dbName +
                                    " managed location: " + managedLocation);
                        }
                    }
                }
            });

            // Check table locations for 'null' string values
            Map<String, TableMirror> tableMirrorMap = null;
            try {
                tableMirrorMap = getTableMirrorRepository().findByDatabase(conversionResult.getKey(), dbName);
            } catch (RepositoryException e) {
                throw new RuntimeException(e);
            }
            tableMirrorMap.forEach((tableName, tableMirror) -> {
                tableMirror.getEnvironments().forEach((environment, environmentTable) -> {
                    String location = TableUtils.getLocation(tableName, environmentTable.getDefinition());
                    if (location != null) {
                        log.warn("LOCATION CHECK for Table Location for: {}:{}.{} - {}", environment, dbName, tableName, location);
                        if (location.contains("null")) {
                            fail(String.format("Table location contains a null value for %s.%s in %s. Location: %s",
                                    dbName, tableName, environment, location));
                        }
                    }
                });
            });
        });
    }

    /**
     * Check when the job strategy is STORAGE_MIGRATION.
     * Using the conversionResult.getNamespace()...
     * Check that the RIGHT DB Environment DB_LOCATION and DB_MANAGED_LOCATION start with the namespace above.
     * Get a list of Tables for the Database and check each table with this criteria.
     * The table environment schema are NOT adjusted, as they represent the original schemas pulled.  And the RIGHT environment
     * is a copy of the left and used as a place holder.  But the location adjustments will be in the LEFT environments
     * sql pairs.  Loop through all the pair.action elements and look for "LOCATION".  If you find it, parse out the location
     * value that follows it and ensure it starts with the namespace above.
     */
    @Test
    public void checkStorageMigrationSqlLocationTranslations() {
        if (getReturnCode() < 0) {
            log.info("STORAGE MIGRATION SQL TEST: Skipping - return code is less than 0");
            return;
        }

        ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
                new RuntimeException("Conversion result is not available"));

        DataStrategyEnum strategy = conversionResult.getJob().getStrategy();

        // Only run for STORAGE_MIGRATION strategy
        if (strategy != DataStrategyEnum.STORAGE_MIGRATION) {
            log.info("STORAGE MIGRATION SQL TEST: Skipping - strategy is {}, not STORAGE_MIGRATION", strategy);
            return;
        }

        String targetNamespace = conversionResult.getTargetNamespace();
        if (targetNamespace == null || targetNamespace.isEmpty()) {
            log.info("STORAGE MIGRATION SQL TEST: No target namespace defined. Skipping.");
            return;
        }

        log.info("STORAGE MIGRATION SQL TEST: Checking SQL location translations for STORAGE_MIGRATION with target namespace: {}", targetNamespace);

        // Get all DBMirrors
        Map<String, DBMirror> dbMirrorMap;
        try {
            dbMirrorMap = getDbMirrorRepository().findByConversionKey(conversionResult.getKey());
        } catch (RepositoryException e) {
            throw new RuntimeException("STORAGE MIGRATION SQL TEST: Failed to retrieve DBMirrors", e);
        }

        // Pattern to match LOCATION 'value' or LOCATION "value" in SQL
        Pattern locationPattern = Pattern.compile("LOCATION\\s+['\"]([^'\"]+)['\"]", Pattern.CASE_INSENSITIVE);

        // Check database and table locations
        dbMirrorMap.forEach((dbName, dbMirror) -> {
            log.info("STORAGE MIGRATION SQL TEST: Checking database '{}'", dbName);

            // Check RIGHT DB environment locations (definitions are adjusted in RIGHT)
            if (dbMirror.getProperties().containsKey(Environment.RIGHT)) {
                Map<String, String> rightProps = dbMirror.getEnvironmentProperties(Environment.RIGHT);

                // Check DB_LOCATION
                if (rightProps.containsKey(DB_LOCATION)) {
                    String location = rightProps.get(DB_LOCATION);
                    if (location != null) {
                        log.info("STORAGE MIGRATION SQL TEST: DB_LOCATION for '{}' in RIGHT - Expected prefix: '{}', Actual: '{}'",
                                dbName, targetNamespace, location);
                        assertTrue(location.startsWith(targetNamespace),
                                String.format("DB Location for database '%s' in RIGHT should start with target namespace '%s', but was: %s",
                                        dbName, targetNamespace, location));
                    }
                }

                // Check DB_MANAGED_LOCATION
                if (rightProps.containsKey(DB_MANAGED_LOCATION)) {
                    String managedLocation = rightProps.get(DB_MANAGED_LOCATION);
                    if (managedLocation != null) {
                        log.info("STORAGE MIGRATION SQL TEST: DB_MANAGED_LOCATION for '{}' in RIGHT - Expected prefix: '{}', Actual: '{}'",
                                dbName, targetNamespace, managedLocation);
                        assertTrue(managedLocation.startsWith(targetNamespace),
                                String.format("DB Managed Location for database '%s' in RIGHT should start with target namespace '%s', but was: %s",
                                        dbName, targetNamespace, managedLocation));
                    }
                }
            }

            // Check table SQL locations (SQL adjustments are in LEFT environment)
            try {
                Map<String, TableMirror> tableMirrorMap = getTableMirrorRepository().findByDatabase(conversionResult.getKey(), dbName);
                log.info("STORAGE MIGRATION SQL TEST: Checking {} tables in database '{}'", tableMirrorMap.size(), dbName);

                tableMirrorMap.forEach((tableName, tableMirror) -> {
                    if (tableMirror.getEnvironments().containsKey(Environment.LEFT)) {
                        EnvironmentTable leftEnvTable = tableMirror.getEnvironmentTable(Environment.LEFT);
                        List<Pair> sqlPairs = leftEnvTable.getSql();

                        if (sqlPairs != null && !sqlPairs.isEmpty()) {
                            log.debug("STORAGE MIGRATION SQL TEST: Checking {} SQL pairs for table '{}.{}'",
                                    sqlPairs.size(), dbName, tableName);

                            for (Pair pair : sqlPairs) {
                                String action = pair.getAction();
                                if (action != null && action.toUpperCase().contains("LOCATION")) {
                                    // Parse out LOCATION values from SQL
                                    java.util.regex.Matcher matcher = locationPattern.matcher(action);
                                    while (matcher.find()) {
                                        String locationValue = matcher.group(1);
                                        log.info("STORAGE MIGRATION SQL TEST: Found LOCATION in SQL for '{}.{}' - Expected prefix: '{}', Actual: '{}'",
                                                dbName, tableName, targetNamespace, locationValue);
                                        assertTrue(locationValue.startsWith(targetNamespace),
                                                String.format("Location in SQL for %s.%s should start with target namespace '%s', but was: %s. SQL: %s",
                                                        dbName, tableName, targetNamespace, locationValue, action));
                                    }
                                }
                            }
                        } else {
                            log.debug("STORAGE MIGRATION SQL TEST: No SQL pairs for table '{}.{}'", dbName, tableName);
                        }
                    } else {
                        log.debug("STORAGE MIGRATION SQL TEST: No LEFT environment for table '{}.{}'", dbName, tableName);
                    }
                });
            } catch (RepositoryException e) {
                log.error("STORAGE MIGRATION SQL TEST: Failed to retrieve tables for database '{}'", dbName, e);
                throw new RuntimeException("Failed to retrieve tables for database: " + dbName, e);
            }
        });

        log.info("STORAGE MIGRATION SQL TEST: Completed successfully");
    }

    @Test
    /**
     * Test all schema adjustments for transfers. Do not include STORAGE_MIGRATION because we don't
     * adjust the locations in the definitions, we change them in the SQL issued.
     */
    public void checkSchemaLocationTranslations() {
        if (getReturnCode() < 0) {
            log.info("LOCATION TRANSLATION TEST Skipping - return code is less than 0");
            return;
        }


        ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
                new RuntimeException("Conversion result is not available"));

        DataStrategyEnum strategy = conversionResult.getJob().getStrategy();

        if (getConfig().getMigrateACID().isInplace() && getConfig().getMigrateACID().isOn() && getConfig().getMigrateACID().isDowngrade()) {
            log.info("LOCATION TRANSLATION TEST: Skipping location translation for strategy: {}. In-place migration is enabled and downgrade is enabled.",
                    strategy);
            return;
        }

        // Only check location translations for strategies that should translate locations
        if (strategy != DataStrategyEnum.HYBRID &&
                strategy != DataStrategyEnum.SQL &&
                strategy != DataStrategyEnum.EXPORT_IMPORT &&
                strategy != DataStrategyEnum.SCHEMA_ONLY) {
            log.info("LOCATION TRANSLATION TEST: Skipping location comparison for strategy: {}. No translation expected.", strategy);
            return;
        }

        String targetNamespace = conversionResult.getTargetNamespace();
        if (targetNamespace == null || targetNamespace.isEmpty()) {
            log.info("LOCATION TRANSLATION TEST: No target namespace defined. Skipping location comparison.");
            return;
        }

        // Determine which environment to check based on strategy
        // STORAGE_MIGRATION: Check LEFT (in-place migration)
        // Other strategies: Check RIGHT (migrate to new cluster)
        Environment tblEnvironmentToCheck = Environment.RIGHT;

        // For the DB, even the STORAGE_MIGRATION, the definition we fix is on the RIGHT.  For STORAGE_MIGRATION
        //   we pull from that to build the LEFT SQL.
        Environment dbEnvironmentToCheck = Environment.RIGHT;

        log.info("LOCATION TRANSLATION TEST: Checking location translations for strategy: {} with target namespace: {} in {} environment",
                strategy, targetNamespace, tblEnvironmentToCheck);

        // Get all DBMirrors
        Map<String, DBMirror> dbMirrorMap;
        try {
            dbMirrorMap = getDbMirrorRepository().findByConversionKey(conversionResult.getKey());
        } catch (RepositoryException e) {
            throw new RuntimeException("LOCATION TRANSLATION TEST: Failed to retrieve DBMirrors", e);
        }

        // Check database locations
        dbMirrorMap.forEach((dbName, dbMirror) -> {
            log.info("LOCATION TRANSLATION TEST: Checking database '{}' in {} environment", dbName, dbEnvironmentToCheck);

            // Check DB_LOCATION
            if (dbMirror.getProperties().containsKey(dbEnvironmentToCheck)) {
                Map<String, String> envProps = dbMirror.getEnvironmentProperties(dbEnvironmentToCheck);
                if (envProps.containsKey(DB_LOCATION)) {
                    String location = envProps.get(DB_LOCATION);
                    if (location != null) {
                        log.info("LOCATION TRANSLATION TEST: DB_LOCATION for '{}' in {} - Expected prefix: '{}', Actual: '{}'",
                                dbName, dbEnvironmentToCheck, targetNamespace, location);
                        assertTrue(location.startsWith(targetNamespace),
                                String.format("DB Location for database '%s' in %s should start with target namespace '%s', but was: %s",
                                        dbName, dbEnvironmentToCheck, targetNamespace, location));
                    } else {
                        log.debug("LOCATION TRANSLATION TEST: DB_LOCATION is null for database '{}' in {}", dbName, dbEnvironmentToCheck);
                    }
                } else {
                    log.debug("LOCATION TRANSLATION TEST: DB_LOCATION not found for database '{}' in {}", dbName, dbEnvironmentToCheck);
                }

                // Check DB_MANAGED_LOCATION
                if (envProps.containsKey(DB_MANAGED_LOCATION)) {
                    String managedLocation = envProps.get(DB_MANAGED_LOCATION);
                    if (managedLocation != null) {
                        log.info("LOCATION TRANSLATION TEST: DB_MANAGED_LOCATION for '{}' in {} - Expected prefix: '{}', Actual: '{}'",
                                dbName, dbEnvironmentToCheck, targetNamespace, managedLocation);
                        assertTrue(managedLocation.startsWith(targetNamespace),
                                String.format("DB Managed Location for database '%s' in %s should start with target namespace '%s', but was: %s",
                                        dbName, dbEnvironmentToCheck, targetNamespace, managedLocation));
                    } else {
                        log.debug("LOCATION TRANSLATION TEST: DB_MANAGED_LOCATION is null for database '{}' in {}", dbName, dbEnvironmentToCheck);
                    }
                } else {
                    log.debug("LOCATION TRANSLATION TEST: DB_MANAGED_LOCATION not found for database '{}' in {}", dbName, dbEnvironmentToCheck);
                }
            } else {
                log.debug("LOCATION TRANSLATION TEST: No {} environment properties for database '{}'", dbEnvironmentToCheck, dbName);
            }

            // Check table locations
            try {
                Map<String, TableMirror> tableMirrorMap = getTableMirrorRepository().findByDatabase(conversionResult.getKey(), dbName);
                log.info("LOCATION TRANSLATION TEST: Checking {} tables in database '{}' for {} environment",
                        tableMirrorMap.size(), dbName, tblEnvironmentToCheck);

                tableMirrorMap.forEach((tableName, tableMirror) -> {
                    if (tableMirror.getEnvironmentTable(Environment.RIGHT).getIssues().contains(SCHEMA_EXISTS_TARGET_MISMATCH.getDesc())) {
                        log.info("LOCATION TRANSLATION TEST: Skipping table '{}' in {} environment - SCHEMA_EXISTS_TARGET_MISMATCH",
                                tableName, dbEnvironmentToCheck);
                    } else {
                        if (tableMirror.getEnvironments().containsKey(tblEnvironmentToCheck)) {
                            EnvironmentTable envTable = tableMirror.getEnvironmentTable(tblEnvironmentToCheck);
                            String tableLocation = TableUtils.getLocation(tableName, envTable.getDefinition());

                            if (tableLocation != null) {
                                log.info("LOCATION TRANSLATION TEST: Table location for '{}.{}' in {} - Expected prefix: '{}', Actual: '{}'",
                                        dbName, tableName, tblEnvironmentToCheck, targetNamespace, tableLocation);
                                assertTrue(tableLocation.startsWith(targetNamespace),
                                        String.format("Table location for %s.%s in %s should start with target namespace '%s', but was: %s",
                                                dbName, tableName, tblEnvironmentToCheck, targetNamespace, tableLocation));
                            } else {
                                log.debug("LOCATION TRANSLATION TEST: Table location is null for '{}.{}' in {}", dbName, tableName, tblEnvironmentToCheck);
                            }
                        } else {
                            log.debug("LOCATION TRANSLATION TEST: No {} environment for table '{}.{}'", tblEnvironmentToCheck, dbName, tableName);
                        }
                    }
                });
            } catch (RepositoryException e) {
                log.error("LOCATION TRANSLATION TEST: Failed to retrieve tables for database '{}'", dbName, e);
                throw new RuntimeException("Failed to retrieve tables for database: " + dbName, e);
            }
        });
    }

    protected ConfigLiteDto getConfig() {
        return getConversion().getConfig();
    }

    protected ConversionResult getConversion() {
        return getExecutionContextService().getConversionResult().orElseThrow(() ->
                new RuntimeException("Conversion result is not available"));
    }

    protected String[] getDatabasesFromTestDataFile(String testDataSet) {
        System.out.println("Test data file: " + testDataSet);
        ConversionResult conversionResult = domainService.deserializeConversion(testDataSet);
        String[] databases = null;

        databases = conversionResult.getDatabases().keySet().toArray(new String[0]);

        return databases;
    }

    protected String getDistcpLine(String outputDir, DBMirror[] dbMirrors, int dbInstance, Environment side,
                                   int distcpSource, int lineNum) {
        String rtn = null;
        String dbName = dbMirrors[dbInstance].getName();

        // Open the distcp file for the RIGHT.
        String distcpFile = outputDir + "/" + dbName + "_" + side.toString() + "_" + distcpSource + "_distcp_source.txt";
        File file = new File(distcpFile);
        if (!file.exists()) {
            fail("Distcp file doesn't exist: " + distcpFile);
        } else {
            try {
                List<String> lines = Files.readAllLines(file.toPath());
                rtn = lines.get(lineNum);
            } catch (IOException e) {
                log.error(e.getMessage(), e);
                fail("Error reading distcp file: " + distcpFile);
            }
        }
        return rtn;
    }

    protected long getCheckCode(MessageCode... messageCodes) {
        int check = 0;
        BitSet bitSet = new BitSet(150);
        long expected = 0;
        for (MessageCode messageCode : messageCodes) {
            bitSet.set(messageCode.ordinal());
        }
        long[] messageSet = bitSet.toLongArray();
        for (long messageBit : messageSet) {
            expected = expected | messageBit;
        }
        // Errors should be negative return code.
        return expected * -1;
    }

    protected DBMirror[] getResults(String outputDirBase, String sourceTestDataSet) {
        List<DBMirror> dbMirrorList = new ArrayList<>();
        System.out.println("Source Dataset: " + sourceTestDataSet);
        String[] databases = getDatabasesFromTestDataFile(sourceTestDataSet);
        for (String database : databases) {
            try {
                String resultsFileStr = outputDirBase + "/" + database + "_hms-mirror.yaml";
                URL resultURL = null;
                File resultsFile = new File(resultsFileStr);
                if (!resultsFile.exists()) {
                    log.error("Couldn't locate results file: " + resultsFileStr);
                    continue;
                }
                resultURL = resultsFile.toURI().toURL();
                ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
                mapper.enable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

                String yamlCfgFile = IOUtils.toString(resultURL, StandardCharsets.UTF_8);
                DBMirror dbMirror = mapper.readerFor(DBMirror.class).readValue(yamlCfgFile);
                dbMirrorList.add(dbMirror);
            } catch (UnrecognizedPropertyException upe) {
                log.error("\nThere may have been a breaking change in the configuration since the previous " +
                        "release. Review the note below and remove the 'Unrecognized field' from the configuration and try " +
                        "again.\n\n", upe);
            } catch (Throwable t) {
                // Look for yaml update errors.
                if (t.toString().contains("MismatchedInputException")) {
                    log.error("The format of the 'config' yaml file MAY HAVE CHANGED from the last release.  Please make a copy and run " +
                            "'-su|--setup' again to recreate in the new format", t);
                } else {
//                log.error(t);
                    log.error("A configuration element is no longer valid, progress.  Please remove the element from the configuration yaml and try again.", t);
                }
            }
        }
        return dbMirrorList.toArray(new DBMirror[0]);
    }

    protected Long getReturnCode() {
        return hmsMirrorAppService.getReturnCode();
//        return executeSessionService.getSession().getRunStatus().getErrors().getReturnCode();
    }

    protected Long getWarningCode() {
        return hmsMirrorAppService.getWarningCode();
    }

    /**
     * Gets a DBMirror by name, failing the test if not found.
     *
     * @param database the database name
     * @return the DBMirror instance
     * @throws AssertionError if the database is not found or a repository exception occurs
     */
    protected DBMirror getDBMirrorOrFail(String database) {
        try {
            return dbMirrorRepository.findByName(getConversion().getKey(), database)
                    .orElseThrow(() -> new AssertionError("Database not found: " + database));
        } catch (RepositoryException e) {
            throw new AssertionError("Failed to retrieve database: " + database, e);
        }
    }

    /**
     * Gets a TableMirror by name, failing the test if not found.
     *
     * @param database  the database name
     * @param tableName the table name
     * @return the TableMirror instance
     * @throws AssertionError if the table is not found or a repository exception occurs
     */
    protected TableMirror getTableMirrorOrFail(String database, String tableName) {
        try {
            return tableMirrorRepository.findByName(getConversion().getKey(), database, tableName)
                    .orElseThrow(() -> new AssertionError("Table not found: " + database + "." + tableName));
        } catch (RepositoryException e) {
            throw new AssertionError("Failed to retrieve table: " + database + "." + tableName, e);
        }
    }

    /**
     * Gets the number of tables in a database.
     *
     * @param database the database name
     * @return the number of tables in the database
     * @throws AssertionError if a repository exception occurs
     */
    protected int getTableCount(String database) {
        try {
            Map<String, TableMirror> tables = getTableMirrorRepository().findByDatabase(getConversion().getKey(), database);
            return tables != null ? tables.size() : 0;
        } catch (RepositoryException e) {
            throw new AssertionError("Failed to get table count for database: " + database, e);
        }
    }

    public void validateTableCount(String database, int expectedTableCount) {
        // Validate that we have 2 tables in the database
        int tableCount = getTableCount(database);
        assertEquals(expectedTableCount, tableCount, "Table count mismatch");
    }

    protected void validateTableIsACID(String database, String tableName, Environment environment) {
        TableMirror tableMirror = getTableMirrorOrFail(database, tableName);
        EnvironmentTable envTable = tableMirror.getEnvironmentTable(environment);

        assertNotNull(envTable,
                String.format("Environment %s doesn't exist for table %s.%s",
                        environment, database, tableName));
        assertTrue(TableUtils.isACID(envTable),
                String.format("Table %s.%s in %s is NOT ACID",
                        database, tableName, environment));
    }

    protected void validateTableInDatabase(String database, String tableName) {
        Map<String, TableMirror> tableMap = null;
        try {
            tableMap = getTableMirrorRepository().findByDatabase(getConversion().getKey(), database);
        } catch (RepositoryException e) {
            throw new RuntimeException(e);
        }
        // Check if tableName is a key in tableMap.
        if (!tableMap.containsKey(tableName)) {
            fail(String.format("Table %s.%s do NOT exists in database %s",
                    database, tableName, database));
        }
    }

    protected void validateTableNotInDatabase(String database, String tableName) {
        Map<String, TableMirror> tableMap = null;
        try {
            tableMap = getTableMirrorRepository().findByDatabase(getConversion().getKey(), database);
        } catch (RepositoryException e) {
            throw new RuntimeException(e);
        }
        // Check if tableName is a key in tableMap.
        if (tableMap.containsKey(tableName)) {
            fail(String.format("Table %s.%s exists in database %s",
                    database, tableName, database));
        }
    }

    protected void validateTableIsNotACID(String database, String tableName, Environment environment) {
        TableMirror tableMirror = getTableMirrorOrFail(database, tableName);
        EnvironmentTable envTable = tableMirror.getEnvironmentTable(environment);

        assertNotNull(envTable,
                String.format("Environment %s doesn't exist for table %s.%s",
                        environment, database, tableName));
        assertFalse(TableUtils.isACID(envTable),
                String.format("Table %s.%s in %s should NOT be ACID",
                        database, tableName, environment));
    }

    public void validateTableBuckets(String database, String tableName, Environment environment, int buckets) {
        // Validate acid_01 has 2 buckets on LEFT
        TableMirror tableMirror = getTableMirrorOrFail(database, tableName);
        EnvironmentTable envTable = tableMirror.getEnvironmentTable(environment);
        assertNotNull(envTable, String.format("Environment table is null for %s.%s in %s", database, tableName, environment));
        // In the envTable definition, we should have a line like this: INTO 'x' BUCKETS
        //  Parse this to find the number of buckets and compare to the expected value.
        Pattern pattern = Pattern.compile("INTO\\s+(\\d+)\\s+BUCKETS");
        boolean found = false;
        int actualBuckets = 0;

        for (String line : envTable.getDefinition()) {
            Matcher matcher = pattern.matcher(line);
            if (matcher.find()) {
                actualBuckets = Integer.parseInt(matcher.group(1));
                found = true;
                break;
            }
        }

        assertTrue(found,
                String.format("Could not find bucket specification in table definition for %s.%s in %s",
                        database, tableName, environment));
        assertEquals(buckets, actualBuckets,
                String.format("Bucket count doesn't match for table %s.%s in %s. Expected: %d, Actual: %d",
                        database, tableName, environment, buckets, actualBuckets));
    }

    protected void validateDBLocation(String database, Environment environment, String expectedLocation) {
        DBMirror dbMirror = getDBMirrorOrFail(database);

        assertTrue(dbMirror.getProperties().containsKey(environment),
                String.format("Environment %s doesn't exist for database %s", environment, database));
        assertEquals(expectedLocation, dbMirror.getEnvironmentProperties(environment).get(DB_LOCATION),
                String.format("Location doesn't match for database %s in %s", database, environment));
    }

    protected void validateDBManagedLocation(String database, Environment environment, String expectedLocation) {
        DBMirror dbMirror = getDBMirrorOrFail(database);

        assertTrue(dbMirror.getProperties().containsKey(environment),
                String.format("Environment %s doesn't exist for database %s", environment, database));
        assertEquals(expectedLocation, dbMirror.getEnvironmentProperty(environment, DB_MANAGED_LOCATION),
                String.format("Managed location doesn't match for database %s in %s", database, environment));
    }

    protected void validatePartitioned(String database, String tableName, Environment environment) {
        TableMirror tableMirror = getTableMirrorOrFail(database, tableName);

        assertTrue(tableMirror.getEnvironments().containsKey(environment),
                String.format("Environment %s doesn't exist for table %s.%s", environment, database, tableName));
        // Validate table is partitioned.
        assertTrue(TableUtils.isPartitioned(tableMirror.getEnvironmentTable(environment)),
                "The table should be partitioned, but isn't. Database: " + database + ", Table: " + tableName + ", Environment: " + environment);
//        assertEquals(expectedIssueCount, tableMirror.getEnvironmentTable(environment).getPartitions().size(),
//                String.format("Partition count doesn't match for table %s.%s in %s", database, tableName, environment));
    }

    protected void validatePartitionCount(String database, String tableName, Environment environment, int expectedIssueCount) {
        TableMirror tableMirror = getTableMirrorOrFail(database, tableName);

        assertTrue(tableMirror.getEnvironments().containsKey(environment),
                String.format("Environment %s doesn't exist for table %s.%s", environment, database, tableName));
        assertEquals(expectedIssueCount, tableMirror.getEnvironmentTable(environment).getPartitions().size(),
                String.format("Partition count doesn't match for table %s.%s in %s", database, tableName, environment));
    }

    protected void validatePartitionLocation(String database, String tableName, Environment environment, String partitionSpec, String partitionLocation) {
        TableMirror tableMirror = getTableMirrorOrFail(database, tableName);
        EnvironmentTable envTable = tableMirror.getEnvironmentTable(environment);

        assertTrue(tableMirror.getEnvironments().containsKey(environment),
                String.format("Environment %s doesn't exist for table %s.%s", environment, database, tableName));
        assertNotNull(envTable, String.format("Environment table is null for %s.%s in %s", database, tableName, environment));
        assertTrue(envTable.getPartitions().containsKey(partitionSpec),
                String.format("Partition %s doesn't exist for table %s.%s", partitionSpec, database, tableName));
        assertEquals(partitionLocation, envTable.getPartitions().get(partitionSpec),
                String.format("Partition location doesn't match for %s in %s.%s", partitionSpec, database, tableName));
    }

    protected void validatePhase(String database, String tableName, PhaseState expectedPhaseState) {
        TableMirror tableMirror = getTableMirrorOrFail(database, tableName);

        assertEquals(expectedPhaseState, tableMirror.getPhaseState(),
                String.format("Phase state doesn't match for table %s.%s", database, tableName));
    }

    protected void validateDBInPhaseSummaryCount(String database, PhaseState phase, int expectedPhaseSummaryCount) {
        // Validate phase summary shows all tables in CALCULATED_SQL phase
        DBMirror dbMirror = getDBMirrorOrFail(database);

        Map<String, TableMirror> tableMirrorMap = null;
        try {
            tableMirrorMap = getTableMirrorRepository().findByDatabase(getConversion().getKey(), database);
        } catch (RepositoryException e) {
            throw new RuntimeException(e);
        }
        int count = 0;
        for (TableMirror tableMirror : tableMirrorMap.values()) {
            if (tableMirror.getPhaseState() == phase) {
                count++;
            }
        }
        assertEquals(expectedPhaseSummaryCount, count,
                String.format("Phase summary count doesn't match for database %s in %s", database, phase));

    }

    protected void validateTableInPhaseState(String database, String tableName, PhaseState phaseState) {
        TableMirror tableMirror = getTableMirrorOrFail(database, tableName);
        assertEquals(phaseState, tableMirror.getPhaseState(),
                String.format("Phase state doesn't match for table %s.%s", database, tableName));
    }

    protected void validateDBPhaseSummaryCount(String database, PhaseState phaseState, int expectedPhaseSummaryCount) {
        // Validate phase summary shows all tables in CALCULATED_SQL phase
        ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
                new IllegalStateException("Conversion result is null."));
        DBMirror dbMirror = getDBMirrorOrFail(database);
        Map<String, TableMirror> tableMirrorMap = null;
        try {
            tableMirrorMap = getTableMirrorRepository().findByDatabase(conversionResult.getKey(),
                    database);
        } catch (RepositoryException e) {
            throw new RuntimeException(e);
        }
        AtomicInteger count = new AtomicInteger(0);
        tableMirrorMap.values().forEach(tableMirror -> {
            if (tableMirror.getPhaseState() == phaseState) {
                count.incrementAndGet();
            }
        });
        assertEquals(expectedPhaseSummaryCount, count.get(),
                String.format("Phase summary count doesn't match for database %s in %s", database, phaseState));
    }

    protected void validateTablePhaseTotalCount(String database, String table, int expectedPhaseTotalCount) {
        // Validate phase summary shows all tables in CALCULATED_SQL phase
        DBMirror dbMirror = getDBMirrorOrFail(database);

        TableMirror tableMirror = getTableMirrorOrFail(database, table);
        assertEquals(expectedPhaseTotalCount, tableMirror.getTotalPhaseCount().get());
    }

    protected void validateTablePhaseCurrentCount(String database, String table, int expectedPhaseCurrentCount) {
        // Validate phase summary shows all tables in CALCULATED_SQL phase
        DBMirror dbMirror = getDBMirrorOrFail(database);

        TableMirror tableMirror = getTableMirrorOrFail(database, table);
        assertEquals(expectedPhaseCurrentCount, tableMirror.getCurrentPhase().get());
    }


    protected void validateSyncIssue(String database, String table, Environment environment, String expectedIssue) {
        TableMirror tableMirror = getTableMirrorOrFail(database, table);
        EnvironmentTable envTable = tableMirror.getEnvironmentTable(environment);
        assertNotNull(envTable, String.format("Environment table is null for %s.%s in %s", database, table, environment));

        // Debug logging to diagnose string comparison issues
        log.debug("Validating sync issue for {}.{} in {}", database, table, environment);
        log.debug("Expected issue: [{}] (length: {})", expectedIssue, expectedIssue.length());
        log.debug("Expected issue bytes: {}", java.util.Arrays.toString(expectedIssue.getBytes()));
        log.debug("Actual issues in list (count: {}):", envTable.getIssues().size());
        boolean found = false;
        for (int i = 0; i < envTable.getIssues().size(); i++) {
            String actualIssue = envTable.getIssues().get(i);
            log.debug("  [{}]: [{}] (length: {})", i, actualIssue, actualIssue.length());
            log.debug("  [{}] bytes: {}", i, java.util.Arrays.toString(actualIssue.getBytes()));
            log.debug("  [{}] equals: {}, equalsIgnoreCase: {}, contains: {}",
                    i, actualIssue.equals(expectedIssue),
                    actualIssue.equalsIgnoreCase(expectedIssue),
                    envTable.getIssues().contains(expectedIssue));

            // Check trimmed version
            if (actualIssue.trim().equals(expectedIssue.trim())) {
                log.debug("  [{}] MATCH found after trimming!", i);
                found = true;
            }
        }

        assertTrue(found,
                String.format("Environment %s.%s in %s doesn't contain issue: %s\nActual issues: %s",
                        database, table, environment, expectedIssue, envTable.getIssues()));
    }

    protected void validateDBSqlPair(String database, Environment environment, String description, String actionTest) {
        DBMirror dbMirror = getDBMirrorOrFail(database);
        boolean found = Boolean.FALSE;

        for (Pair pair : dbMirror.getSql(environment)) {
            if (pair.getDescription().trim().equals(description)) {
                assertEquals(actionTest.trim(), pair.getAction().trim(),
                        String.format("DB SQL doesn't match for %s in %s", description, database));
                found = Boolean.TRUE;
            }
        }
        assertTrue(found, String.format("DB SQL pair not found for %s in %s", description, database));
    }

    protected void validateDBSqlAction(String database, Environment environment, String actionTest) {
        DBMirror dbMirror = getDBMirrorOrFail(database);
        boolean found = Boolean.FALSE;

        for (Pair pair : dbMirror.getSql(environment)) {
            if (pair.getAction().trim().contains(actionTest)) {
                found = Boolean.TRUE;
                break;
            }
        }
        assertTrue(found, String.format("DB SQL action not found for %s in %s", actionTest, database));
    }

    protected void validateDBSqlGenerated(String database, Environment environment) {
        DBMirror dbMirror = getDBMirrorOrFail(database);
        // Assert that dbMirror.getSql(environment) isn't empty.
        assertFalse(dbMirror.getSql(environment).isEmpty(), "The DB SQL should have been generated, but it isn't.");
    }

    protected void validateDBSqlNotGenerated(String database, Environment environment) {
        DBMirror dbMirror = getDBMirrorOrFail(database);
        // Assert that dbMirror.getSql(environment) is empty.
        assertTrue(dbMirror.getSql(environment).isEmpty(), "The DB SQL should NOT have been generated, but it was.");
    }


    protected void validateTableSqlAction(String database, String tableName, Environment environment, String actionTest) {
        TableMirror tableMirror = getTableMirrorOrFail(database, tableName);
        boolean found = Boolean.FALSE;

        for (Pair pair : tableMirror.getSql(environment)) {
            if (pair.getAction().trim().contains(actionTest.trim())) {
                found = Boolean.TRUE;
                break;
            }
        }
        assertTrue(found, String.format("Table SQL action not found for %s in %s", actionTest, database));
    }

    protected void validateTableSqlDescription(String database, String tableName, Environment environment, String descriptionTest) {
        TableMirror tableMirror = getTableMirrorOrFail(database, tableName);
        boolean found = Boolean.FALSE;

        for (Pair pair : tableMirror.getSql(environment)) {
            if (pair.getDescription().trim().contains(descriptionTest)) {
                found = Boolean.TRUE;
                break;
            }
        }
        assertTrue(found, String.format("Table SQL description not found for %s in %s", descriptionTest, database));
    }


    protected void validateTableSqlGenerated(String database, String tableName, Environment environment) {
        TableMirror tableMirror = getTableMirrorOrFail(database, tableName);
        EnvironmentTable envTable = tableMirror.getEnvironmentTable(environment);
        assertNotNull(envTable, String.format("Environment table is null for %s.%s in %s", database, tableName, environment));
        // Check that the envTable sql isn't empty.
        assertFalse(envTable.getSql().isEmpty(), "The table should have SQL, but doesn't. Database: " + database + ", Table: " + tableName + ", Environment: " + environment);
    }

    protected void validateTableSqlNotGenerated(String database, String tableName, Environment environment) {
        TableMirror tableMirror = getTableMirrorOrFail(database, tableName);
        EnvironmentTable envTable = tableMirror.getEnvironmentTable(environment);
        assertNotNull(envTable, String.format("Environment table is null for %s.%s in %s", database, tableName, environment));
        // Check that the envTable sql isn't empty.
        assertTrue(envTable.getSql().isEmpty(), "The table should NOT have SQL, but does. Database: " + database + ", Table: " + tableName + ", Environment: " + environment);
    }


    protected void validateTableCleanupSqlGenerated(String database, String tableName, Environment environment) {
        TableMirror tableMirror = getTableMirrorOrFail(database, tableName);
        EnvironmentTable envTable = tableMirror.getEnvironmentTable(environment);
        assertNotNull(envTable, String.format("Environment table is null for %s.%s in %s", database, tableName, environment));
        // Check that the envTable sql isn't empty.
        assertFalse(envTable.getCleanUpSql().isEmpty(), "The table should have Cleanup SQL, but doesn't. Database: " + database + ", Table: " + tableName + ", Environment: " + environment);
    }

    protected void validateTableCleanupSqlPair(String database, String tableName, Environment environment, String description, String actionTest) {
        TableMirror tableMirror = getTableMirrorOrFail(database, tableName);
        boolean found = Boolean.FALSE;

        for (Pair pair : tableMirror.getEnvironmentTable(environment).getCleanUpSql()) {
            if (pair.getDescription().trim().equals(description)) {
                assertEquals(actionTest, pair.getAction(),
                        String.format("Table SQL doesn't match for %s in %s.%s", description, database, tableName));
                found = Boolean.TRUE;
            }
        }
        assertTrue(found, String.format("Table SQL pair not found for %s in %s.%s", description, database, tableName));
    }


    protected void validateTableSqlPair(String database, Environment environment, String tableName, String description, String actionTest) {
        TableMirror tableMirror = getTableMirrorOrFail(database, tableName);
        boolean found = Boolean.FALSE;

        for (Pair pair : tableMirror.getEnvironmentTable(environment).getSql()) {
            if (pair.getDescription().trim().equals(description)) {
//                pair.getAction().trim().contains(actionTest);
                assertTrue(pair.getAction().trim().contains(actionTest),
                        String.format("Table SQL doesn't match for %s in %s.%s", description, database, tableName));
                found = Boolean.TRUE;
            }
        }
        assertTrue(found, String.format("Table SQL pair not found for %s in %s.%s", description, database, tableName));
    }

    protected void validateTableIssue(String database, String tableName, Environment environment, String expectedIssue) {
        TableMirror tableMirror = getTableMirrorOrFail(database, tableName);
        boolean found = Boolean.FALSE;

        for (String issue : tableMirror.getIssues(environment)) {
            if (issue.trim().contains(expectedIssue.trim())) {
                found = Boolean.TRUE;
                break;
            }
        }
        assertTrue(found, String.format("Table Issue not found for '%s' in %s.%s", expectedIssue, database, tableName));

    }

    protected void validateTableIssueCount(String database, String tableName, Environment environment, int expectedIssueCount) {
        TableMirror tableMirror = getTableMirrorOrFail(database, tableName);

        assertTrue(tableMirror.getEnvironments().containsKey(environment),
                String.format("Environment %s doesn't exist for table %s.%s", environment, database, tableName));
        assertEquals(expectedIssueCount, tableMirror.getEnvironmentTable(environment).getIssues().size(),
                String.format("Issue count doesn't match for table %s.%s in %s", database, tableName, environment));
    }

    protected void validateTableIssues(String database, String tableName, Environment environment) {
        TableMirror tableMirror = getTableMirrorOrFail(database, tableName);

        assertTrue(tableMirror.getEnvironments().containsKey(environment),
                String.format("Environment %s doesn't exist for table %s.%s", environment, database, tableName));
        // Validate table issues is not empty.
        assertFalse(tableMirror.getEnvironmentTable(environment).getIssues().isEmpty(),
                "The table should have issues, but doesn't. Database: " + database + ", Table: " + tableName + ", Environment: " + environment);
//        assertEquals(expectedIssueCount, tableMirror.getEnvironmentTable(environment).getIssues().size(),
//                String.format("Issue count doesn't match for table %s.%s in %s", database, tableName, environment));
    }

    protected void validateTableIssuesHave(String database, String tableName, Environment environment, String expectedIssue) {
        TableMirror tableMirror = getTableMirrorOrFail(database, tableName);
        boolean found = Boolean.FALSE;
        for (String issue : tableMirror.getEnvironmentTable(environment).getIssues()) {
            if (issue.contains(expectedIssue)) {
                found = Boolean.TRUE;
                break;
            }
        }
        assertTrue(found, String.format("Table Issue not found for '%s' in %s.%s", expectedIssue, database, tableName));
    }

    protected void validateTableError(String database, String tableName, Environment environment, String expectedError) {
        TableMirror tableMirror = getTableMirrorOrFail(database, tableName);
        boolean found = Boolean.FALSE;

        for (String issue : tableMirror.getErrors(environment)) {
            if (issue.contains(expectedError)) {
                found = Boolean.TRUE;
                break;
            }
        }
        assertTrue(found, String.format("Table Error not found for '%s' in %s.%s", expectedError, database, tableName));

    }

    protected void validateTableErrors(String database, String tableName, Environment environment) {
        TableMirror tableMirror = getTableMirrorOrFail(database, tableName);

        assertTrue(tableMirror.getEnvironments().containsKey(environment),
                String.format("Environment %s doesn't exist for table %s.%s", environment, database, tableName));
        // Validate table issues is not empty.
        assertFalse(tableMirror.getEnvironmentTable(environment).getErrors().isEmpty(),
                "The table should have errors, but doesn't. Database: " + database + ", Table: " + tableName + ", Environment: " + environment);
//        assertEquals(expectedIssueCount, tableMirror.getEnvironmentTable(environment).getIssues().size(),
//                String.format("Issue count doesn't match for table %s.%s in %s", database, tableName, environment));
    }

    protected void validateTableErrorsHave(String database, String tableName, Environment environment, String expectedError) {
        TableMirror tableMirror = getTableMirrorOrFail(database, tableName);
        boolean found = Boolean.FALSE;
        for (String issue : tableMirror.getEnvironmentTable(environment).getErrors()) {
            if (issue.contains(expectedError)) {
                found = Boolean.TRUE;
                break;
            }
        }
        assertTrue(found, String.format("Table Error not found for '%s' in %s.%s", expectedError, database, tableName));
    }

    protected void validateTableErrorCount(String database, String tableName, Environment environment, int expectedErrorCount) {
        TableMirror tableMirror = getTableMirrorOrFail(database, tableName);

        assertTrue(tableMirror.getEnvironments().containsKey(environment),
                String.format("Environment %s doesn't exist for table %s.%s", environment, database, tableName));
        assertEquals(expectedErrorCount, tableMirror.getEnvironmentTable(environment).getErrors().size(),
                String.format("Error count doesn't match for table %s.%s in %s", database, tableName, environment));
    }

    protected void validateTableEnvironment(String database, String tableName, Environment expectedEnvironment) {
        TableMirror tableMirror = getTableMirrorOrFail(database, tableName);
        EnvironmentTable envTable = tableMirror.getEnvironmentTable(expectedEnvironment);
        // TODO: isExists (i think) is about pre-existence of table.  So not sure this is the right check.  TBD
        assertTrue(envTable.isExists(), String.format("Environment table is null for %s.%s in %s", database, tableName, expectedEnvironment));
    }

    protected void validateTableEnvironmentNotExist(String database, String tableName, Environment expectedEnvironment) {
        TableMirror tableMirror = getTableMirrorOrFail(database, tableName);
        EnvironmentTable envTable = tableMirror.getEnvironmentTable(expectedEnvironment);
        assertFalse(envTable.isExists(), String.format("Environment table is null for %s.%s in %s", database, tableName, expectedEnvironment));
    }

    protected void validateTableEnvironmentHasDefinition(String database, String tableName, Environment expectedEnvironment) {
        TableMirror tableMirror = getTableMirrorOrFail(database, tableName);
        EnvironmentTable envTable = tableMirror.getEnvironmentTable(expectedEnvironment);
        assertNotNull(envTable, String.format("Environment table is null for %s.%s in %s", database, tableName, expectedEnvironment));
        // Validate envTable definition is not empty.
        assertFalse(envTable.getDefinition().isEmpty(),
                "The table should have a definition, but doesn't. Database: " + database + ", Table: " + tableName + ", Environment: " + expectedEnvironment);
//        assertEquals(expectedIssueCount, tableMirror.getEnvironmentTable(environment).getIssues().size(),
//                String.format("Issue count doesn't match for table %s.%s in %s", database, tableName, environment));
    }

    protected void validateTableEnvironmentDefinitionHas(String database, String tableName, Environment expectedEnvironment, String expectedDefinitionLine) {
        TableMirror tableMirror = getTableMirrorOrFail(database, tableName);
        EnvironmentTable envTable = tableMirror.getEnvironmentTable(expectedEnvironment);
        assertNotNull(envTable, String.format("Environment table is null for %s.%s in %s", database, tableName, expectedEnvironment));
        // Validate envTable definition is not empty.
        assertFalse(envTable.getDefinition().isEmpty(),
                "The table should have a definition, but doesn't. Database: " + database + ", Table: " + tableName + ", Environment: " + expectedEnvironment);
        boolean found = Boolean.FALSE;
        for (String line : envTable.getDefinition()) {
            if (line.contains(expectedDefinitionLine)) {
                found = Boolean.TRUE;
                break;
            }
        }
        assertTrue(found, String.format("Definition line '%s' not found in table %s.%s in %s", expectedDefinitionLine, database, tableName, expectedEnvironment));
    }

    protected void validateTableEnvironmentAddPropertiesHas(String database, String tableName, Environment expectedEnvironment, String expectedAddPropertyKey) {
        TableMirror tableMirror = getTableMirrorOrFail(database, tableName);
        EnvironmentTable envTable = tableMirror.getEnvironmentTable(expectedEnvironment);
        assertNotNull(envTable, String.format("Environment table is null for %s.%s in %s", database, tableName, expectedEnvironment));
        // Validate envTable definition is not empty.
        boolean found = Boolean.FALSE;
        for (String line : envTable.getAddProperties().keySet()) {
            if (line.contains(expectedAddPropertyKey)) {
                found = Boolean.TRUE;
                break;
            }
        }
        assertTrue(found, String.format("Add Property Key '%s' not found in table %s.%s in %s", expectedAddPropertyKey, database, tableName, expectedEnvironment));
    }


    protected void validateTableEnvironmentCreateStrategy(String database, String tableName, Environment expectedEnvironment, CreateStrategy expectedCreateStrategy) {
        TableMirror tableMirror = getTableMirrorOrFail(database, tableName);
        EnvironmentTable envTable = tableMirror.getEnvironmentTable(expectedEnvironment);
        assertNotNull(envTable, String.format("Environment table is null for %s.%s in %s", database, tableName, expectedEnvironment));
        assertEquals(expectedCreateStrategy, envTable.getCreateStrategy(),
                String.format("Create strategy doesn't match for %s.%s in %s", database, tableName, expectedEnvironment));
    }

    protected void validateTableEnvironmentHasProperty(String database, String tableName, Environment environment,
                                                       String property) {
        // Get the environment table for the specified environment.
        TableMirror tableMirror = getTableMirrorOrFail(database, tableName);
        EnvironmentTable envTable = tableMirror.getEnvironmentTable(environment);
        assertNotNull(envTable, String.format("Environment table is null for %s.%s in %s", database, tableName, environment));
        String tblProperty = TableUtils.getTblProperty(property, envTable);
        assertNotNull(tblProperty, String.format("Property %s doesn't exist for %s.%s in %s", property, database, tableName, environment));
//        assertNotNull(getConversion().getDatabase("merge_files_migrate")
//                .getTableMirrors().get("acid_01").getEnvironmentTable(Environment.RIGHT)
//                .getDefinition().stream()
//                .filter(line -> line.contains("hms-mirror_Metadata_Stage1"))
//                .findFirst()
//                .orElse(null), "hms-mirror_Metadata_Stage1 property not found");
    }

    protected void validateTableProperty(String database, String tableName, Environment environment, String key, String value) {
        TableMirror tableMirror = getTableMirrorOrFail(database, tableName);

        assertTrue(tableMirror.getEnvironments().containsKey(environment),
                String.format("Environment %s doesn't exist for table %s.%s", environment, database, tableName));
        String tblValue = TableUtils.getTblProperty(key, tableMirror.getEnvironmentTable(environment));
        assertEquals(value, tblValue,
                String.format("Property %s doesn't match for table %s.%s in %s", key, database, tableName, environment));
    }

    protected void validateTablePropertyMissing(String database, String tableName, Environment environment, String key) {
        TableMirror tableMirror = getTableMirrorOrFail(database, tableName);

        assertTrue(tableMirror.getEnvironments().containsKey(environment),
                String.format("Environment %s doesn't exist for table %s.%s", environment, database, tableName));
        assertNull(TableUtils.getTblProperty(key, tableMirror.getEnvironmentTable(environment)),
                String.format("Property %s should be missing for table %s.%s in %s", key, database, tableName, environment));
    }

    protected void validateTableLocation(String database, String tableName, Environment environment, String expectedLocation) {
        TableMirror tableMirror = getTableMirrorOrFail(database, tableName);

        assertTrue(tableMirror.getEnvironments().containsKey(environment),
                String.format("Environment %s doesn't exist for table %s.%s", environment, database, tableName));
        if (expectedLocation == null) {
            validateTableEnvironment(database, tableName, environment);
            assertNull(TableUtils.getLocation(tableName, tableMirror.getEnvironmentTable(environment).getDefinition()),
                    String.format("Location doesn't match for table %s.%s in %s", database, tableName, environment));
        } else {
            assertEquals(expectedLocation,
                    TableUtils.getLocation(tableName, tableMirror.getEnvironmentTable(environment).getDefinition()),
                    String.format("Location doesn't match for table %s.%s in %s", database, tableName, environment));
        }
    }

    protected void validateTableMissing(String database, String tableName) {
        try {
            Optional<TableMirror> tableMirror = tableMirrorRepository.findByName(getConversion().getKey(), database, tableName);
            if (tableMirror.isPresent()) {
                fail(String.format("Table %s.%s should not exist", database, tableName));
            }
        } catch (RepositoryException e) {
            throw new RuntimeException(e);
        }
    }

    protected void validateWorkingTableLocation(String database, String tableName, String workingTableName, Environment environment, String expectedLocation) {
        Pattern pattern = Pattern.compile(expectedLocation);
        TableMirror tableMirror = getTableMirrorOrFail(database, tableName);

        EnvironmentTable environmentTable = tableMirror.getEnvironmentTable(environment);
        assertNotNull(environmentTable,
                String.format("Working table doesn't exist for %s.%s in %s", database, tableName, environment));

        assertEquals(workingTableName, environmentTable.getName(),
                String.format("Working table name doesn't match for %s.%s in %s", database, tableName, environment));

        String location = TableUtils.getLocation(workingTableName, environmentTable.getDefinition());
        Matcher matcher = pattern.matcher(location);

        assertTrue(matcher.matches(),
                String.format("Working location doesn't match pattern for %s.%s in %s. Expected pattern: %s, Actual: %s",
                        database, tableName, environment, expectedLocation, location));
    }

    protected void validateTableStrategy(String database, String tableName, DataStrategyEnum strategy) {
        TableMirror tableMirror = getTableMirrorOrFail(database, tableName);

        assertEquals(strategy, tableMirror.getStrategy(), "Table strategy doesn't match");

    }
}