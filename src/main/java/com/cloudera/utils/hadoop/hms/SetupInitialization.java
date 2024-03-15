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

import com.cloudera.utils.hadoop.hms.mirror.Config;
import com.cloudera.utils.hadoop.hms.mirror.Conversion;
import com.cloudera.utils.hadoop.hms.mirror.Progression;
import com.cloudera.utils.hadoop.hms.mirror.Setup;
import com.cloudera.utils.hadoop.hms.mirror.service.ConnectionPoolService;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SetupInitialization {

    @Bean
    Setup setup(Config config, @Qualifier("conversion") Conversion conversion, ConnectionPoolService connectionPoolService, Progression progression) {
        Setup rtn = new Setup();

        rtn.setConfig(config);
        rtn.setConversion(conversion);
        rtn.setConnectionPoolService(connectionPoolService);
        rtn.setProgression(progression);

        if (!rtn.collect()) {
            // TODO: Need to address a setup error
        }

        return rtn;

    }
}
