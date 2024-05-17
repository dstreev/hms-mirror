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

package com.cloudera.utils.hms.mirror.domain.support;

import com.cloudera.utils.hms.mirror.MessageCode;
import com.cloudera.utils.hms.mirror.domain.Messages;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Slf4j
@Getter
@Setter
public class RunStatus {
    @JsonIgnore
    private final Messages errors = new Messages(150);
    @JsonIgnore
    private final Messages warnings = new Messages(150);

    @JsonIgnore
    Future<Boolean> runningTask = null;

    /*
    Flag to indicate if the configuration has been validated.
    This should be reset before each run to ensure the configuration is validated.
     */
    private boolean configValidated = false;
    /*
    Keep track of the current running state.
     */
    private ProgressEnum progress = ProgressEnum.INITIALIZED;

    /*
    Track the current progress across the various stages of the operation.
     */
    private Map<StageEnum, CollectionEnum> stages = new LinkedHashMap<>();

    /*
    Maintain statistics on the operation.
     */
    private OperationStatistics operationStatistics = new OperationStatistics();

    public boolean isRunning() {
        boolean rtn = Boolean.FALSE;
        switch (progress) {
            case IN_PROGRESS:
            case STARTED:
            case CANCEL_FAILED:
                rtn = Boolean.TRUE;
                break;
            case CANCELLED:
            case COMPLETED:
            case FAILED:
            case INITIALIZED:
                rtn = Boolean.FALSE;
                break;
        }
        return rtn;
    }

    public ProgressEnum getProgress() {
        // If the task is still running, then the progress is still in progress.
        if (runningTask != null) {
            if (runningTask.isCancelled()) {
                progress = ProgressEnum.CANCELLED;
            } else if (!runningTask.isDone()) {
                progress = ProgressEnum.IN_PROGRESS;
            } else if (runningTask.isDone()) {
                // Don't attempt to get the result until the task is done.
                //     or else it will block.
                try {
                    if (runningTask.get()) {
                        progress = ProgressEnum.COMPLETED;
                    } else {
                        progress = ProgressEnum.FAILED;
                    }
                } catch (InterruptedException | ExecutionException e) {
                    //throw new RuntimeException(e);
                }
            }
        }
        return progress;
    }

    public RunStatus() {
        for (StageEnum stage : StageEnum.values()) {
            stages.put(stage, CollectionEnum.EMPTY);
        }
    }

    /*
    reset the state of the RunStatus.
     */
    public boolean reset() {
        boolean rtn = Boolean.TRUE;
        if (cancel()) {
            errors.clear();
            warnings.clear();
            progress = ProgressEnum.INITIALIZED;
            configValidated = false;
            stages.forEach((k, v) -> v = CollectionEnum.EMPTY);
            operationStatistics.reset();
        } else {
            rtn = Boolean.FALSE;
        }
        return rtn;
    }

    public boolean cancel() {
        boolean rtn = Boolean.TRUE;
        if (runningTask != null && !runningTask.isDone()) {
            if (runningTask.cancel(true)) {
                log.info("Task cancelled.");
                this.progress = ProgressEnum.CANCELLED;
            } else {
                log.error("Task could not be cancelled.");
                this.progress = ProgressEnum.CANCEL_FAILED;
                rtn = Boolean.FALSE;
            }
            ;
        }
        return rtn;
    }

    public void setStage(StageEnum stage, CollectionEnum collection) {
        stages.put(stage, collection);
    }

    public void addError(MessageCode code) {
        errors.set(code);
    }

    public void addError(MessageCode code, Object... messages) {
        errors.set(code, messages);
    }

    public void addWarning(MessageCode code) {
        warnings.set(code);
    }

    public void addWarning(MessageCode code, Object... message) {
        warnings.set(code, message);
    }

    public String getErrorMessage(MessageCode code) {
        return errors.getMessage(code.getCode());
    }

    public String getWarningMessage(MessageCode code) {
        return warnings.getMessage(code.getCode());
    }

    public Boolean hasErrors() {
        if (errors.getReturnCode() != 0) {
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }

    public Boolean hasWarnings() {
        if (warnings.getReturnCode() != 0) {
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }

    public String[] getErrorMessages() {
        return errors.getMessages();
    }

    public String[] getWarningMessages() {
        return warnings.getMessages();
    }


}
