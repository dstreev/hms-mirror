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

package com.cloudera.utils.hms.mirror.service;

import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.util.Protect;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;

@Service
@Slf4j
@Getter
public class PasswordService {

    public String decryptPassword(String passwordKey, String decryptPassword) {
        Protect protect = new Protect(passwordKey);
        String password = null;
        try {
            password = protect.decrypt(decryptPassword);
        } catch (Exception e) {
            log.error("Error decrypting encrypted password: {} with key: {}", decryptPassword, passwordKey);
        }
        return password;
    }

//    public boolean decryptConfigPasswords(HmsMirrorConfig hmsMirrorConfig) {
//        boolean success = true;
//        if (hmsMirrorConfig.getPasswordKey() != null) {
//            List<Environment> environments = Arrays.asList(Environment.LEFT, Environment.RIGHT);
//            for (Environment environment: environments) {
//                // Decrypt Passwords
//                Protect protect = new Protect(hmsMirrorConfig.getPasswordKey());
//                if (hmsMirrorConfig.getCluster(environment) != null
//                        && hmsMirrorConfig.getCluster(environment).getHiveServer2() != null
//                        && hmsMirrorConfig.getCluster(environment).getHiveServer2().getConnectionProperties().getProperty("password") != null) {
//                    try {
//                        hmsMirrorConfig.getCluster(environment).getHiveServer2()
//                                .getConnectionProperties().setProperty("password",
//                                        protect.decrypt(hmsMirrorConfig.getCluster(environment).getHiveServer2().getConnectionProperties().getProperty("password")));
//                        log.warn("PasswordApp decrypted for {} HS2 Configuration", environment);
//                    } catch (Exception e) {
//                        log.error("Issue decrypting password for {} HS2 Configuration", environment);
//                        success = false;
//                    }
//                }
//                if (hmsMirrorConfig.getCluster(environment).getMetastoreDirect() != null
//                        && hmsMirrorConfig.getCluster(environment).getMetastoreDirect().getConnectionProperties().getProperty("password") != null) {
//                    try {
//                        hmsMirrorConfig.getCluster(environment).getMetastoreDirect()
//                                .getConnectionProperties().setProperty("password",
//                                        protect.decrypt(hmsMirrorConfig.getCluster(environment).getMetastoreDirect().getConnectionProperties().getProperty("password")));
//                        log.warn("PasswordApp decrypted for {} Metastore Direct Configuration", environment);
//                    } catch (Exception e) {
//                        log.error("Issue decrypting password for {} Metastore Direct Configuration", environment);
//                        success = false;
//                    }
//                }
//            }
//        }
//        return success;
//    }

    public String encryptPassword(String passwordKey, String password) {
        // Used to generate encrypted password.
        String epassword = null;
        if (passwordKey != null) {
            Protect protect = new Protect(passwordKey);
            if (password != null) {
                epassword = null;
                try {
                    epassword = protect.encrypt(password);
                } catch (Exception e) {
                    log.error("Error encrypting password: {} with password key {}", password, passwordKey);
                }
            } else {
                // Missing PasswordApp to Encrypt.
                log.error("Missing PasswordApp to Encrypt");
            }
        } else {
            log.error("Missing PasswordApp Key used the encrypt the password");
//            throw new RuntimeException("Missing PasswordApp Key");
        }
        return epassword;
    }

}
