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

package com.cloudera.utils.hms.mirror.web.controller;

import com.cloudera.utils.hadoop.cli.CliEnvironment;
import com.cloudera.utils.hadoop.cli.DisabledException;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.support.ConnectionStatus;
import com.cloudera.utils.hms.mirror.domain.support.Connections;
import com.cloudera.utils.hms.mirror.domain.support.ExecuteSession;
import com.cloudera.utils.hms.mirror.exceptions.EncryptionException;
import com.cloudera.utils.hms.mirror.exceptions.SessionException;
import com.cloudera.utils.hms.mirror.service.ConfigService;
import com.cloudera.utils.hms.mirror.service.ConnectionPoolService;
import com.cloudera.utils.hms.mirror.service.ExecuteSessionService;
import com.cloudera.utils.hms.mirror.service.UIModelService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import java.sql.Connection;
import java.sql.SQLException;

import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

@Controller
@RequestMapping(path = "/connections")
@Slf4j
public class ConnectionMVController {

    private ConfigService configService;
    private ConnectionPoolService connectionPoolService;
    private ExecuteSessionService executeSessionService;
    private UIModelService uiModelService;
    private CliEnvironment cliEnvironment;

    @Autowired
    public void setCliEnvironment(CliEnvironment cliEnvironment) {
        this.cliEnvironment = cliEnvironment;
    }

    @Autowired
    public void setConfigService(ConfigService configService) {
        this.configService = configService;
    }

    @Autowired
    public void setConnectionPoolService(ConnectionPoolService connectionPoolService) {
        this.connectionPoolService = connectionPoolService;
    }

    @Autowired
    public void setExecuteSessionService(ExecuteSessionService executeSessionService) {
        this.executeSessionService = executeSessionService;
    }

    @Autowired
    public void setUiModelService(UIModelService uiModelService) {
        this.uiModelService = uiModelService;
    }

    @RequestMapping(value = "/validate", method = RequestMethod.GET)
    public String validate(Model model) {
        uiModelService.sessionToModel(model, 1, false);
        return "connections/validate";
    }

    @RequestMapping(value = "/doValidate", method = RequestMethod.POST)
    public String doValidate(Model model) throws SessionException, EncryptionException {

        executeSessionService.clearActiveSession();

        ExecuteSession session = executeSessionService.getSession();
        Connections connections = session.getConnections();

        HmsMirrorConfig config = session.getConfig();
        boolean configErrors = !configService.validateForConnections(session);
        if (!configErrors) {
            try {
                connectionPoolService.init();
            } catch (SQLException e) {
                configErrors = Boolean.TRUE;
            }
        };

        boolean finalConfigErrors = configErrors;
        config.getClusters().forEach((k, v) -> {
            if (nonNull(v)) {
                if (nonNull(v.getHiveServer2()) && !finalConfigErrors) {
                    try {
                        Connection conn = connectionPoolService.getConnectionPools().getHS2EnvironmentConnection(k);
                        connections.getHiveServer2Connections().get(k).setStatus(ConnectionStatus.SUCCESS);
                    } catch (SQLException se) {
                        connections.getHiveServer2Connections().get(k).setStatus(ConnectionStatus.FAILED);
                        connections.getHiveServer2Connections().get(k).setMessage(se.getMessage());
                    }
                } else if (nonNull(v.getHiveServer2())) {
                    connections.getHiveServer2Connections().get(k).setStatus(ConnectionStatus.CHECK_CONFIGURATION);
                } else {
                    connections.getHiveServer2Connections().get(k).setStatus(ConnectionStatus.NOT_CONFIGURED);
                }

                if (nonNull(v.getMetastoreDirect()) && !finalConfigErrors) {
                    try {
                        Connection conn = connectionPoolService.getConnectionPools().getMetastoreDirectEnvironmentConnection(k);
                        connections.getMetastoreDirectConnections().get(k).setStatus(ConnectionStatus.SUCCESS);
                    } catch (SQLException se) {
                        connections.getMetastoreDirectConnections().get(k).setStatus(ConnectionStatus.FAILED);
                        connections.getMetastoreDirectConnections().get(k).setMessage(se.getMessage());
                    }

                } else if (nonNull(v.getMetastoreDirect())) {
                    connections.getMetastoreDirectConnections().get(k).setStatus(ConnectionStatus.CHECK_CONFIGURATION);
                } else {
                    connections.getMetastoreDirectConnections().get(k).setStatus(ConnectionStatus.NOT_CONFIGURED);
                }

                if (!isBlank(v.getHcfsNamespace())) {
                    try {
                        CommandReturn cr = cliEnvironment.processInput("ls /");
                        if (cr.isError()) {
                            connections.getNamespaces().get(k).setStatus(ConnectionStatus.FAILED);
                            connections.getNamespaces().get(k).setMessage(cr.getError());
                        } else {
                            connections.getNamespaces().get(k).setStatus(ConnectionStatus.SUCCESS);
                        }
                    } catch (DisabledException e) {
                        connections.getNamespaces().get(k).setStatus(ConnectionStatus.DISABLED);
//                        throw new RuntimeException(e);
                    }
                } else {
                    connections.getNamespaces().get(k).setStatus(ConnectionStatus.NOT_CONFIGURED);
                }
            }
        });

        uiModelService.sessionToModel(model, 1, false);

        return "connections/validate";
    }
}
