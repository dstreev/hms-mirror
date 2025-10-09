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

package com.cloudera.utils.hms.mirror.test;

import com.cloudera.utils.hms.mirror.config.RocksDBTestManager;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

/**
 * Base class for tests that use RocksDB. Ensures proper cleanup between tests
 * even when Spring shutdown hooks don't execute properly.
 */
@SpringBootTest
@TestPropertySource(properties = {
    "hms-mirror.rocksdb.enabled=true",
    "hms-mirror.rocksdb.test-mode=true",
    "hms-mirror.rocksdb.path=${java.io.tmpdir}/hms-mirror-test-${random.uuid}"
})
@Slf4j
public abstract class RocksDBTestBase {

    @Autowired(required = false)
    protected RocksDBTestManager rocksDBTestManager;

    private static RocksDBTestManager staticTestManager;

    @BeforeEach
    public void setUpRocksDB() {
        log.info("Setting up RocksDB for test: {}", getClass().getSimpleName());
        
        if (rocksDBTestManager != null) {
            // Reset cleanup state for test reuse scenarios
            rocksDBTestManager.resetCleanupState();
            
            // Store reference for static cleanup
            staticTestManager = rocksDBTestManager;
        } else {
            log.warn("RocksDBTestManager not available - RocksDB may not be enabled for this test");
        }
    }

    @AfterEach
    public void tearDownRocksDB() {
        log.info("Tearing down RocksDB after test: {}", getClass().getSimpleName());
        
        if (rocksDBTestManager != null) {
            try {
                // Force cleanup after each test
                rocksDBTestManager.forceCleanup();
                log.info("RocksDB cleanup completed for test: {}", getClass().getSimpleName());
            } catch (Exception e) {
                log.error("Error during RocksDB cleanup for test: {}", getClass().getSimpleName(), e);
            }
        }
    }

    @AfterAll
    public static void tearDownRocksDBClass() {
        log.info("Final RocksDB cleanup for test class");
        
        if (staticTestManager != null) {
            try {
                // Final cleanup for the entire test class
                staticTestManager.forceCleanup();
                log.info("Final RocksDB cleanup completed");
            } catch (Exception e) {
                log.error("Error during final RocksDB cleanup", e);
            }
        }
    }

    /**
     * Clean up RocksDB before the next test. Call this if you need fresh RocksDB state.
     */
    protected void cleanRocksDBForNextTest() {
        if (rocksDBTestManager != null) {
            rocksDBTestManager.forceCleanup();
            rocksDBTestManager.resetCleanupState();
        }
    }

    /**
     * Check if RocksDB is available for this test
     */
    protected boolean isRocksDBAvailable() {
        return rocksDBTestManager != null;
    }
}