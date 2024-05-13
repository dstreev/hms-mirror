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

package com.cloudera.utils.hms.mirror.web.controller.api.v1;

import com.cloudera.utils.hms.mirror.service.ConnectionPoolService;
import com.cloudera.utils.hms.mirror.service.HmsMirrorCfgService;
import com.cloudera.utils.hms.mirror.web.service.ConfigService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
@RequestMapping(path = "/api/v1/runtime")
public class RuntimeController {

    private ConfigService configService;
    private HmsMirrorCfgService hmsMirrorCfgService;
    private ConnectionPoolService connectionPoolService;

    @Autowired
    public void setConfigService(ConfigService configService) {
        this.configService = configService;
    }

    @Autowired
    public void setHmsMirrorCfgService(HmsMirrorCfgService hmsMirrorCfgService) {
        this.hmsMirrorCfgService = hmsMirrorCfgService;
    }

    @Autowired
    public void setConnectionPoolService(ConnectionPoolService connectionPoolService) {
        this.connectionPoolService = connectionPoolService;
    }

    public boolean validate() {
        return hmsMirrorCfgService.validate();
    }


    public void start(boolean dryrun) {
        hmsMirrorCfgService.getHmsMirrorConfig().setExecute(!dryrun);

        /////  Should we consider moving this to the RuntimeService?  /////

        // Close all connections, so we can ensure we have a clean start.
        connectionPoolService.close();

        if (hmsMirrorCfgService.validate()) {
//            hmsMirrorCfgService.start();
        } else {
            log.error("Validation failed. Exiting.");
        }
    }

    public void status() {

    }

    public void stop() {

    }

}
