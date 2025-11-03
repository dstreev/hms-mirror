/*
 * Copyright (c) 2023-2025. Cloudera, Inc. All Rights Reserved
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
package com.cloudera.utils.hms.mirror.cli.run;

import com.cloudera.utils.hms.mirror.cli.CliReporter;
import com.cloudera.utils.hms.mirror.cli.HmsMirrorCommandLineOptions;
import com.cloudera.utils.hms.mirror.domain.core.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.support.ConversionResult;
import com.cloudera.utils.hms.mirror.domain.support.ExecuteSession;
import com.cloudera.utils.hms.mirror.domain.support.RunStatus;
import com.cloudera.utils.hms.mirror.service.*;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;

import java.util.concurrent.CompletableFuture;

/*
Using the config, go through the databases and tables and collect the current states.
Create the target databases, where needed to support the migration.
 */
@Configuration
@Slf4j
@Getter
@Setter
@RequiredArgsConstructor
public class HmsMirrorAppCfg {

    @NonNull
    private final ReportWriterService reportWriterService;
    @NonNull
    private final CliReporter cliReporter;
    @NonNull
    private final ConversionResultService conversionResultService;
    @NonNull
    private final ExecutionContextService executionContextService;
    @NonNull
    private final ConnectionPoolService connectionPoolService;
    @NonNull
    private final DatabaseService databaseService;
    @NonNull
    private final HMSMirrorAppService hmsMirrorAppService;
    @NonNull
    private final TableService tableService;
    @NonNull
    private final TransferService transferService;

    // TODO: Need to address failures here...
    @Bean
    @Order(1000) // Needs to be the last thing to run.
    // Don't run when encrypting/decrypting passwords.
    @ConditionalOnProperty(
            name = "hms-mirror.config.password",
            matchIfMissing = true)
    public CommandLineRunner start() {
        return args -> {
            log.debug("Starting the HMS Mirror Application");
            ConversionResult conversionResult = getExecutionContextService().getConversionResult().orElseThrow(() ->
                    new IllegalStateException("Conversion result is not set."));
            HmsMirrorConfig config = getExecutionContextService().getHmsMirrorConfig().orElseThrow(() ->
                    new IllegalStateException("HmsMirrorConfig is not set."));
            CompletableFuture<Boolean> result = hmsMirrorAppService.cliRun(conversionResult);
            CompletableFuture<Void> runFuture = cliReporter.run(conversionResult, config);

            // Wait for this to finish. It contains the logic to watch the RunStatus for a complete signal.
            runFuture.join();

//            while (!result.isDone()) {
//                try {
//                    Thread.sleep(1000);
//                } catch (InterruptedException e) {
//                    throw new RuntimeException(e);
//                }
//            }
            cliReporter.refresh(Boolean.TRUE);
            getReportWriterService().writeReport();
        };
    }
}