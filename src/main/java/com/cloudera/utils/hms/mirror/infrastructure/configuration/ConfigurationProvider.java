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

package com.cloudera.utils.hms.mirror.infrastructure.configuration;

import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;

/**
 * Infrastructure interface for accessing configuration data.
 * Abstracts configuration loading and management from business logic.
 */
public interface ConfigurationProvider {
    
    /**
     * Gets the current HMS Mirror configuration.
     */
    HmsMirrorConfig getConfig();
    
    /**
     * Updates the configuration with the provided config.
     */
    void updateConfig(HmsMirrorConfig config);
    
    /**
     * Validates that the configuration is complete and valid.
     */
    boolean validateConfig();
    
    /**
     * Reloads configuration from the underlying source.
     */
    void reloadConfig();
}