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

package com.cloudera.utils.hms.mirror.web.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DecisionTreeNode {
    
    private String id;
    private String title;
    private String description;
    private String category;
    private NodeType type;
    private List<DecisionOption> options;
    private Map<String, String> validationRules;
    private String nextPageId;
    private String nextNodeId;
    private List<String> dependencies;
    private boolean required;
    private String helpText;
    private Map<String, Object> metadata;
    private String conditionalLogic;
    
    public enum NodeType {
        SINGLE_SELECT,      // Radio buttons - single choice
        MULTI_SELECT,       // Checkboxes - multiple choices
        TEXT_INPUT,         // Text input field
        NUMBER_INPUT,       // Number input field
        BOOLEAN,            // Yes/No toggle
        CONNECTION_SELECT,  // Connection profile selector
        PAGE_TRANSITION,    // Just transition to next page
        CONDITIONAL         // Node shown based on previous selections
    }
}