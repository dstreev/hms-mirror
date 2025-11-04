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

package com.cloudera.utils.hms.mirror.config;

import com.cloudera.utils.hms.mirror.infrastructure.configuration.ConfigurationProvider;
import com.cloudera.utils.hms.mirror.infrastructure.configuration.SpringConfigurationProviderAdapter;
import com.cloudera.utils.hms.mirror.infrastructure.connection.ConnectionProvider;
import com.cloudera.utils.hms.mirror.infrastructure.connection.SpringConnectionProviderAdapter;
import com.cloudera.utils.hms.mirror.service.ConnectionPoolService;
import com.cloudera.utils.hms.mirror.service.ExecuteSessionService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring configuration for the new core business logic layer.
 * This configuration wires up the new layered architecture with the existing Spring services.
 */
@Configuration
public class CoreBusinessConfig {

    /**
     * Creates a connection provider that adapts the existing ConnectionPoolService
     * to the new infrastructure interface.
     */
    @Bean
    public ConnectionProvider connectionProvider(ConnectionPoolService connectionPoolService) {
        return new SpringConnectionProviderAdapter(connectionPoolService);
    }

    /**
     * Creates a configuration provider that adapts the existing session-based config management
     * to the new infrastructure interface.
     */
    @Bean
    public ConfigurationProvider configurationProvider(ExecuteSessionService executeSessionService) {
        return new SpringConfigurationProviderAdapter(executeSessionService);
    }


}