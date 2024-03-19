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

package com.cloudera.utils.hadoop.hms;

import com.cloudera.utils.hadoop.hms.mirror.service.ConfigService;
import org.apache.kerby.config.Conf;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ConcurrentTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executors;

@Configuration
//@EnableAsync
public class ThreadPoolConfigurator {

    @Bean("metadataThreadPool")
    public TaskExecutor metadataThreadPool(ConfigService configService) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(configService.getConfig().getTransfer().getConcurrency()/2);
        executor.setMaxPoolSize(configService.getConfig().getTransfer().getConcurrency());
        executor.setQueueCapacity(Integer.MAX_VALUE);
        executor.setThreadNamePrefix("metadata-");
        executor.initialize();
        return executor;
    }

    @Bean("jobThreadPool")
    public TaskExecutor jobThreadPool(ConfigService configService) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(configService.getConfig().getTransfer().getConcurrency()/2);
        executor.setMaxPoolSize(configService.getConfig().getTransfer().getConcurrency());
        executor.setQueueCapacity(Integer.MAX_VALUE);
        executor.setThreadNamePrefix("job-");
        executor.initialize();
        return executor;
    }

}
