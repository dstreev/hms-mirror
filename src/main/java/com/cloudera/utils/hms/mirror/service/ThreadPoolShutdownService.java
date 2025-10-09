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

package com.cloudera.utils.hms.mirror.service;

import lombok.extern.slf4j.Slf4j;
import org.rocksdb.RocksDB;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.annotation.Order;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class ThreadPoolShutdownService {

    private final TaskExecutor jobThreadPool;
    private final TaskExecutor metadataThreadPool;
    private final TaskExecutor reportingThreadPool;
    private final TaskExecutor executionThreadPool;
    private final TaskExecutor cliExecutionThreadPool;
    private final RocksDB rocksDB;

    @Autowired
    public ThreadPoolShutdownService(@Autowired(required = false) @Qualifier("jobThreadPool") TaskExecutor jobThreadPool,
                                   @Autowired(required = false) @Qualifier("metadataThreadPool") TaskExecutor metadataThreadPool,
                                   @Qualifier("reportingThreadPool") TaskExecutor reportingThreadPool,
                                   @Qualifier("executionThreadPool") TaskExecutor executionThreadPool,
                                   @Qualifier("cliExecutionThreadPool") TaskExecutor cliExecutionThreadPool,
                                   @Autowired(required = false) RocksDB rocksDB) {
        this.jobThreadPool = jobThreadPool;
        this.metadataThreadPool = metadataThreadPool;
        this.reportingThreadPool = reportingThreadPool;
        this.executionThreadPool = executionThreadPool;
        this.cliExecutionThreadPool = cliExecutionThreadPool;
        this.rocksDB = rocksDB;
    }

    @PreDestroy
    @Order(1) // Execute before RocksDBConfig @PreDestroy
    public void shutdownThreadPools() {
        log.info("Shutting down thread pools and coordinating with RocksDB...");
        
        // Shutdown thread pools first to stop any ongoing operations
        shutdownThreadPool("jobThreadPool", jobThreadPool);
        shutdownThreadPool("metadataThreadPool", metadataThreadPool);
        shutdownThreadPool("reportingThreadPool", reportingThreadPool);
        shutdownThreadPool("executionThreadPool", executionThreadPool);
        shutdownThreadPool("cliExecutionThreadPool", cliExecutionThreadPool);
        
        // Give a brief moment for any database operations to complete
        if (rocksDB != null) {
            try {
                Thread.sleep(500); // Brief pause for ongoing operations
                log.info("Thread pools shutdown complete - RocksDB will shutdown next");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted during shutdown coordination");
            }
        }
        
        log.info("All thread pools shut down successfully");
    }

    private void shutdownThreadPool(String poolName, TaskExecutor executor) {
        if (executor == null) {
            log.debug("{} is null, skipping shutdown", poolName);
            return;
        }
        
        if (executor instanceof ThreadPoolTaskExecutor) {
            ThreadPoolTaskExecutor threadPoolExecutor = (ThreadPoolTaskExecutor) executor;
            log.debug("Shutting down {}", poolName);
            
            threadPoolExecutor.shutdown();
            
            try {
                if (!threadPoolExecutor.getThreadPoolExecutor().awaitTermination(10, TimeUnit.SECONDS)) {
                    log.warn("{} did not terminate gracefully, forcing shutdown", poolName);
                    threadPoolExecutor.getThreadPoolExecutor().shutdownNow();
                    
                    if (!threadPoolExecutor.getThreadPoolExecutor().awaitTermination(5, TimeUnit.SECONDS)) {
                        log.error("{} did not terminate after forced shutdown", poolName);
                    }
                }
            } catch (InterruptedException e) {
                log.warn("Interrupted while waiting for {} to terminate", poolName);
                threadPoolExecutor.getThreadPoolExecutor().shutdownNow();
                Thread.currentThread().interrupt();
            }
            
            log.debug("{} shut down successfully", poolName);
        } else {
            log.debug("{} is not a ThreadPoolTaskExecutor, skipping shutdown", poolName);
        }
    }

    public void forceShutdown() {
        log.warn("Force shutting down all thread pools immediately");
        shutdownThreadPools();
    }
}