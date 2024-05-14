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

import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.service.ExecuteSessionService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
public class ThreadPoolConfigurator {

    @Bean("jobThreadPool")
    @Order(20)
    @ConditionalOnProperty(
            name = "hms-mirror.concurrency.max-threads")
    public TaskExecutor jobThreadPool(ExecuteSessionService executeSessionService,  @Value("${hms-mirror.concurrency.max-threads}") Integer value) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        // TODO: Need to remove this from the config that controls the migration and add
        //       it to the application configuration.
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getCurrentSession().getHmsMirrorConfig();

        executor.setCorePoolSize(value);
        executor.setMaxPoolSize(value);

        executor.setQueueCapacity(Integer.MAX_VALUE);
        executor.setThreadNamePrefix("job-");
        executor.initialize();
        return executor;
    }

    @Bean("metadataThreadPool")
    @Order(20)
    @ConditionalOnProperty(
            name = "hms-mirror.concurrency.max-threads")
    public TaskExecutor metadataThreadPool(ExecuteSessionService executeSessionService,  @Value("${hms-mirror.concurrency.max-threads}") Integer value) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getCurrentSession().getHmsMirrorConfig();
        // TODO: Need to remove this from the config that controls the migration and add
        //       it to the application configuration.
        executor.setCorePoolSize(value);
        executor.setMaxPoolSize(value);

        executor.setQueueCapacity(Integer.MAX_VALUE);
        executor.setThreadNamePrefix("metadata-");
        executor.initialize();
        return executor;
    }

    @Bean("reportingThreadPool")
    @Order(20)
    public TaskExecutor reportingThreadPool(ExecuteSessionService executeSessionService) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getCurrentSession().getHmsMirrorConfig();

        executor.setCorePoolSize(1);
        executor.setMaxPoolSize(1);

        executor.setQueueCapacity(Integer.MAX_VALUE);
        executor.setThreadNamePrefix("reporting-");
        executor.initialize();
        return executor;
    }

}