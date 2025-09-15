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


import com.cloudera.utils.hms.mirror.core.api.TableOperations;
import com.cloudera.utils.hms.mirror.domain.EnvironmentTable;
import com.cloudera.utils.hms.mirror.domain.TableMirror;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.service.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

class TableServiceTest extends ServiceTestBase {

//    private ConfigService configService;
//    private ExecuteSessionService executeSessionService;
//    private ConnectionPoolService connectionPoolService;
    private QueryDefinitionsService queryDefinitionsService;
//    private TranslatorService translatorService;
    private StatsCalculatorService statsCalculatorService;
    private TableOperations tableOperations;  // NEW: Mock for core business logic

    private TableService tableService;

    @BeforeEach
    void setUp1() {
        queryDefinitionsService = new QueryDefinitionsService(executeSessionService);

        statsCalculatorService = new StatsCalculatorService(executeSessionService);
        
        // NEW: Mock the core business logic
        tableOperations = mock(TableOperations.class);

        tableService = new TableService(
                configService,
                executeSessionService,
                connectionPoolService,
                queryDefinitionsService,
                translatorService,
                statsCalculatorService,
                tableOperations  // NEW: Include the mock
        );
    }

    @Test
    void testGetCreateStatement() {
        // Mock the TableMirror and Environment dependencies
        TableMirror mockTableMirror = mock(TableMirror.class);
        List<String> rightDef = new ArrayList<>();
        rightDef.add("CREATE TABLE myTest");
        rightDef.add("(a string)");
        when(mockTableMirror.getTableDefinition(Environment.RIGHT)).thenReturn(rightDef);
//        EnvironmentTable mockEnvironmentTable = mock(EnvironmentTable.class);
//        when(mockEnvironmentTable.getDefinition()).thenReturn(rightDef);
//        when(mockTableMirror.getEnvironmentTable(Environment.RIGHT)).thenReturn(mockEnvironmentTable);
//        mockTableMirror.setTableDefinition(Environment.RIGHT, rightDef);
//        Environment mockEnvironment = mock(Environment.class);

        // Optionally stub interactions if the method under test calls methods on these mocks

        // For demonstration, we assume getCreateStatement returns a non-null value
        String expected = "CREATE TABLE ...";
        // If the method under test uses collaborator methods, further stubbing would be necessary

        // You might need to stub TableMirror or Environment, e.g.:
        // when(mockTableMirror.someMethod()).thenReturn(someValue);

        // Call the method under test
        String result = tableService.getCreateStatement(mockTableMirror, Environment.RIGHT);

        // Do assertions (adapt expected to your implementation)
        assertNotNull(result);
        // assertEquals(expected, result); // adapt as needed
    }
}