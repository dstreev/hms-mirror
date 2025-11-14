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

package com.cloudera.utils.hms.mirror.web.service;

import com.cloudera.utils.hms.mirror.domain.support.ConversionResult;
import com.cloudera.utils.hms.mirror.domain.support.RunStatus;
import com.cloudera.utils.hms.mirror.exceptions.EncryptionException;
import com.cloudera.utils.hms.mirror.exceptions.MismatchException;
import com.cloudera.utils.hms.mirror.exceptions.RequiredConfigurationException;
import com.cloudera.utils.hms.mirror.exceptions.SessionException;
import com.cloudera.utils.hms.mirror.service.*;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
@Getter
@Setter
@Slf4j
@RequiredArgsConstructor
public class RuntimeService {

    @NonNull
    private final ConfigService configService;
    @NonNull
    private final ConversionResultService conversionResultService;
    @NonNull
    private final JobManagementService jobManagementService;
    @NonNull
    private final ExecutionContextService executionContextService;
    @NonNull
    private final HMSMirrorAppService hmsMirrorAppService;

    private final DatabaseService databaseService;
    private final TranslatorService translatorService;

    public RunStatus start(String jobKey, boolean dryrun) throws RequiredConfigurationException, MismatchException, SessionException, EncryptionException {

        ConversionResult conversionResult = getJobManagementService().buildConversionResultFromJobId(jobKey);
        conversionResult.getJobExecution().setDryRun(dryrun);
        getExecutionContextService().setConversionResult(conversionResult);
        getExecutionContextService().setRunStatus(conversionResult.getRunStatus());

        log.debug("Starting the HMS Mirror Application");
//        RunStatus runStatus = conversionResult.getRunStatus();
        // TODO: We need to add this to the web session we are currently using so we can track progress.

        // TODO: What
        CompletableFuture<Boolean> result = getHmsMirrorAppService().run(conversionResult);

        return conversionResult.getRunStatus();
    }
}