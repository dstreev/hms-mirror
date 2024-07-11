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

package com.cloudera.utils.hms.mirror.web.config;

import com.cloudera.utils.hms.mirror.exceptions.RequiredConfigurationException;
import com.cloudera.utils.hms.mirror.exceptions.SessionException;
import com.cloudera.utils.hms.mirror.service.UIModelService;
import com.cloudera.utils.hms.mirror.web.controller.ConfigMVController;
import com.cloudera.utils.hms.mirror.web.controller.ControllerReferences;
import org.bouncycastle.openssl.EncryptionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

@ControllerAdvice
public class MirrorExceptionHandler {

    private UIModelService uiModelService;

    @Autowired
    public void setUiModelService(UIModelService uiModelService) {
        this.uiModelService = uiModelService;
    }

    @ExceptionHandler(value = SessionException.class)
    public String sessionExceptionHandler(Model model, SessionException exception) {
        model.addAttribute(ControllerReferences.TYPE, "Session Exception");
        model.addAttribute(ControllerReferences.MESSAGE, exception.getMessage());
        uiModelService.sessionToModel(model, 0, false);
        return "error";
    }

    @ExceptionHandler(value = RequiredConfigurationException.class)
    public String reqConfigExceptionHandler(Model model, RequiredConfigurationException exception) {
        model.addAttribute(ControllerReferences.TYPE, "Required Configuration");
        model.addAttribute(ControllerReferences.MESSAGE, exception.getMessage());
        uiModelService.sessionToModel(model, 0, false);
        return "error";
    }

    @ExceptionHandler(value = EncryptionException.class)
    public String encryptionExceptionHandler(Model model, EncryptionException exception) {
        model.addAttribute(ControllerReferences.TYPE, "Encryption/Decryption Issue");
        model.addAttribute(ControllerReferences.MESSAGE, exception.getMessage());
        uiModelService.sessionToModel(model, 0, false);
        return "error";
    }

}
