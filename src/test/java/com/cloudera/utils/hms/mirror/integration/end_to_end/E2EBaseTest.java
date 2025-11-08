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
     * @param database the database name
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
        assertFalse(dbMirror.getSql(environment).isEmpty(),"The DB SQL should have been generated, but it isn't.");
    }

    protected void validateDBSqlNotGenerated(String database, Environment environment) {
        DBMirror dbMirror = getDBMirrorOrFail(database);
        // Assert that dbMirror.getSql(environment) is empty.
        assertTrue(dbMirror.getSql(environment).isEmpty(),"The DB SQL should NOT have been generated, but it was.");
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
        assertFalse(envTable.getSql().isEmpty(),"The table should have SQL, but doesn't. Database: " + database + ", Table: " + tableName + ", Environment: " + environment);
    }

    protected void validateTableSqlNotGenerated(String database, String tableName, Environment environment) {
        TableMirror tableMirror = getTableMirrorOrFail(database, tableName);
        EnvironmentTable envTable = tableMirror.getEnvironmentTable(environment);
        assertNotNull(envTable, String.format("Environment table is null for %s.%s in %s", database, tableName, environment));
        // Check that the envTable sql isn't empty.
        assertTrue(envTable.getSql().isEmpty(),"The table should NOT have SQL, but does. Database: " + database + ", Table: " + tableName + ", Environment: " + environment);
    }


    protected void validateTableCleanupSqlGenerated(String database, String tableName, Environment environment) {
        TableMirror tableMirror = getTableMirrorOrFail(database, tableName);
        EnvironmentTable envTable = tableMirror.getEnvironmentTable(environment);
        assertNotNull(envTable, String.format("Environment table is null for %s.%s in %s", database, tableName, environment));
        // Check that the envTable sql isn't empty.
        assertFalse(envTable.getCleanUpSql().isEmpty(),"The table should have Cleanup SQL, but doesn't. Database: " + database + ", Table: " + tableName + ", Environment: " + environment);
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