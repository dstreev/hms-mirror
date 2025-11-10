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

import com.cloudera.utils.hive.config.DBStore;
import com.cloudera.utils.hive.config.QueryDefinitions;
import com.cloudera.utils.hms.mirror.domain.legacy.Cluster;
import com.cloudera.utils.hms.mirror.domain.legacy.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.domain.support.ExecuteSession;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.net.URL;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class QueryDefinitionsServiceTest {

    /**
     * Test case when the environment exists in the queryDefinitionsMap cache.
     */

    /**
     * Test case when the environment refers to a cluster without a metastoreDirect configuration.
     */
    @Test
    void testGetQueryDefinitions_ClusterWithoutMetastore() {
        DBStore.DB_TYPE type = DBStore.DB_TYPE.MYSQL;
        ExecuteSessionService mockExecuteSessionService = mock(ExecuteSessionService.class);
        HmsMirrorConfig mockHmsMirrorConfig = mock(HmsMirrorConfig.class);
        Cluster mockCluster = mock(Cluster.class);

        when(mockHmsMirrorConfig.getCluster(Environment.LEFT)).thenReturn(mockCluster);
        ExecuteSession mockExecuteSession = mock(ExecuteSession.class);

        when(mockExecuteSession.getConfig()).thenReturn(mockHmsMirrorConfig);
        when(mockExecuteSessionService.getSession()).thenReturn(mockExecuteSession);
        when(mockHmsMirrorConfig.getCluster(Environment.LEFT)).thenReturn(mockCluster);
        // mockCluster.getMetastoreDirect() returns null by default, which is what we want for this test

        QueryDefinitionsService queryDefinitionsService = new QueryDefinitionsService(mockExecuteSessionService);

        QueryDefinitions result = queryDefinitionsService.getQueryDefinitions(Environment.LEFT);

        assertNull(result, "QueryDefinitions should be null as the cluster lacks metastoreDirect configuration.");
    }

    /**
     * Test case when the YAML configuration for the metastore can be loaded successfully.
     */
    @Test
    void testGetQueryDefinitions_ValidYAMLMYSQLConfig() throws Exception {
        DBStore.DB_TYPE type = DBStore.DB_TYPE.MYSQL;
        ExecuteSessionService mockExecuteSessionService = mock(ExecuteSessionService.class);
        HmsMirrorConfig mockHmsMirrorConfig = mock(HmsMirrorConfig.class);
        Cluster mockCluster = mock(Cluster.class);

        when(mockHmsMirrorConfig.getCluster(Environment.LEFT)).thenReturn(mockCluster);
        DBStore mockDBStore = mock(DBStore.class);
        ExecuteSession mockExecuteSession = mock(ExecuteSession.class);

        when(mockExecuteSession.getConfig()).thenReturn(mockHmsMirrorConfig);
        when(mockExecuteSessionService.getSession()).thenReturn(mockExecuteSession);
        when(mockHmsMirrorConfig.getCluster(Environment.LEFT)).thenReturn(mockCluster);
        when(mockCluster.getMetastoreDirect()).thenReturn(mockDBStore);
        when(mockDBStore.getType()).thenReturn(type);

        QueryDefinitionsService queryDefinitionsService = new QueryDefinitionsService(mockExecuteSessionService);

        URL mockURL = this.getClass().getResource("/"+type.toString()+"/metastoreDirect.yaml");
        assertNotNull(mockURL, "Resource file for "+type.toString()+"/metastoreDirect.yaml must exist");

        String yamlContent = IOUtils.toString(mockURL, StandardCharsets.UTF_8);

        QueryDefinitions result = queryDefinitionsService.getQueryDefinitions(Environment.LEFT);

        assertNotNull(result, "QueryDefinitions must be loaded successfully from YAML.");
        assertNotNull(result.getQueryDefinition("part_locations"), "Loaded YAML content must have valid query for: 'part_locations'");
        assertNotNull(result.getQueryDefinition("database_partition_locations"), "Loaded YAML content must have valid query for: 'database_partition_locations'");
        assertNotNull(result.getQueryDefinition("database_table_locations"), "Loaded YAML content must have valid query for: 'database_table_locations'");
    }

    @Test
    void testGetQueryDefinitions_ValidYAMLPostgreSConfig() throws Exception {
        DBStore.DB_TYPE type = DBStore.DB_TYPE.POSTGRES;
        ExecuteSessionService mockExecuteSessionService = mock(ExecuteSessionService.class);
        HmsMirrorConfig mockHmsMirrorConfig = mock(HmsMirrorConfig.class);
        Cluster mockCluster = mock(Cluster.class);

        when(mockHmsMirrorConfig.getCluster(Environment.LEFT)).thenReturn(mockCluster);
        DBStore mockDBStore = mock(DBStore.class);
        ExecuteSession mockExecuteSession = mock(ExecuteSession.class);

        when(mockExecuteSession.getConfig()).thenReturn(mockHmsMirrorConfig);
        when(mockExecuteSessionService.getSession()).thenReturn(mockExecuteSession);
        when(mockHmsMirrorConfig.getCluster(Environment.LEFT)).thenReturn(mockCluster);
        when(mockCluster.getMetastoreDirect()).thenReturn(mockDBStore);
        when(mockDBStore.getType()).thenReturn(type);

        QueryDefinitionsService queryDefinitionsService = new QueryDefinitionsService(mockExecuteSessionService);

        URL mockURL = this.getClass().getResource("/"+type.toString()+"/metastoreDirect.yaml");
        assertNotNull(mockURL, "Resource file for "+type.toString()+"/metastoreDirect.yaml must exist");

        String yamlContent = IOUtils.toString(mockURL, StandardCharsets.UTF_8);

        QueryDefinitions result = queryDefinitionsService.getQueryDefinitions(Environment.LEFT);

        assertNotNull(result, "QueryDefinitions must be loaded successfully from YAML.");
        assertNotNull(result.getQueryDefinition("part_locations"), "Loaded YAML content must have valid query for: 'part_locations'");
        assertNotNull(result.getQueryDefinition("database_partition_locations"), "Loaded YAML content must have valid query for: 'database_partition_locations'");
        assertNotNull(result.getQueryDefinition("database_table_locations"), "Loaded YAML content must have valid query for: 'database_table_locations'");
    }

    @Test
    void testGetQueryDefinitions_ValidYAMLOracleConfig() throws Exception {
        DBStore.DB_TYPE type = DBStore.DB_TYPE.ORACLE;
        ExecuteSessionService mockExecuteSessionService = mock(ExecuteSessionService.class);
        HmsMirrorConfig mockHmsMirrorConfig = mock(HmsMirrorConfig.class);
        Cluster mockCluster = mock(Cluster.class);

        when(mockHmsMirrorConfig.getCluster(Environment.LEFT)).thenReturn(mockCluster);
        DBStore mockDBStore = mock(DBStore.class);
        ExecuteSession mockExecuteSession = mock(ExecuteSession.class);

        when(mockExecuteSession.getConfig()).thenReturn(mockHmsMirrorConfig);
        when(mockExecuteSessionService.getSession()).thenReturn(mockExecuteSession);
        when(mockHmsMirrorConfig.getCluster(Environment.LEFT)).thenReturn(mockCluster);
        when(mockCluster.getMetastoreDirect()).thenReturn(mockDBStore);
        when(mockDBStore.getType()).thenReturn(type);

        QueryDefinitionsService queryDefinitionsService = new QueryDefinitionsService(mockExecuteSessionService);

        URL mockURL = this.getClass().getResource("/"+type.toString()+"/metastoreDirect.yaml");
        assertNotNull(mockURL, "Resource file for "+type.toString()+"/metastoreDirect.yaml must exist");

        String yamlContent = IOUtils.toString(mockURL, StandardCharsets.UTF_8);

        QueryDefinitions result = queryDefinitionsService.getQueryDefinitions(Environment.LEFT);

        assertNotNull(result, "QueryDefinitions must be loaded successfully from YAML.");
        assertNotNull(result.getQueryDefinition("part_locations"), "Loaded YAML content must have valid query for: 'part_locations'");
        assertNotNull(result.getQueryDefinition("database_partition_locations"), "Loaded YAML content must have valid query for: 'database_partition_locations'");
        assertNotNull(result.getQueryDefinition("database_table_locations"), "Loaded YAML content must have valid query for: 'database_table_locations'");
    }

    /**
     * Test case when the YAML configuration file is missing.
     */
}