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

import com.cloudera.utils.hms.mirror.domain.core.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.service.ExecuteSessionService;

/**
 * Adapter that bridges the existing Spring-based session/config management
 * with the new infrastructure interface. This allows gradual migration
 * without breaking existing functionality.
 * 
 * Note: This is configured as a bean in a configuration class, not auto-detected
 * via component scanning to avoid test configuration issues.
 */
public class SpringConfigurationProviderAdapter implements ConfigurationProvider {

    private final ExecuteSessionService executeSessionService;

    public SpringConfigurationProviderAdapter(ExecuteSessionService executeSessionService) {
        this.executeSessionService = executeSessionService;
    }

    @Override
    public HmsMirrorConfig getConfig() {
        try {
            return executeSessionService.getSession().getConfig();
        } catch (Exception e) {
            throw new RuntimeException("Failed to get config from session", e);
        }
    }

    @Override
    public void updateConfig(HmsMirrorConfig config) {
        try {
            executeSessionService.getSession().setConfig(config);
        } catch (Exception e) {
            throw new RuntimeException("Failed to update config in session", e);
        }
    }

    @Override
    public boolean validateConfig() {
        try {
            HmsMirrorConfig config = getConfig();
            return config != null && config.getClusters() != null && !config.getClusters().isEmpty();
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public void reloadConfig() {
        // The current session-based config doesn't have a reload method
        // This would need to be implemented if file-based config reloading is needed
    }
}