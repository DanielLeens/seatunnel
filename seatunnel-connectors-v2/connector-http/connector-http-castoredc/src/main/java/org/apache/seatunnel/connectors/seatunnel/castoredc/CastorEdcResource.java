/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.seatunnel.connectors.seatunnel.castoredc;

import java.util.Locale;

/**
 * Provider route and response semantics for one selected resource.
 *
 * <p>Study discovery is ordered before all study-scoped resources.
 */
enum CastorEdcResource {
    USER("user", "user", false, Paging.NONE),
    STUDY("study", "study", false, Paging.LINK),
    AUDIT_TRIAL("audit-trail", "items", true, Paging.AUDIT),
    COUNTRY("country", "results", false, Paging.NONE),
    STUDY_SURVEY("survey", "surveys", true, Paging.HAL),
    STUDY_SURVEY_PACKAGE("survey-package", "survey_packages", true, Paging.HAL),
    STUDY_SITE("site", "sites", true, Paging.HAL),
    STUDY_FIELDS("field", "fields", true, Paging.HAL),
    STUDY_FIELD_DEPENDENCY("field-dependency", "fieldDependencies", true, Paging.HAL),
    STUDY_FIELD_VALIDATION("field-validation", "fieldValidations", true, Paging.HAL),
    STUDY_FORM("form", "forms", true, Paging.HAL),
    STUDY_ROLE("role", "roles", true, Paging.NONE),
    STUDY_FIELDOPTION_GROUPS("field-optiongroup", "fieldOptionGroups", true, Paging.HAL),
    STUDY_STATISTICS("statistics", null, true, Paging.NONE),
    STUDY_USER("user", "studyUsers", true, Paging.LINK),
    STUDY_VISIT("visit", "visits", true, Paging.HAL);

    final String id = name().toLowerCase(Locale.ROOT);
    final String envelope;
    final boolean child;
    final Paging pagination;
    private final String route;

    CastorEdcResource(String route, String envelope, boolean child, Paging pagination) {
        this.route = route;
        this.envelope = envelope;
        this.child = child;
        this.pagination = pagination;
    }

    String path(String study) {
        return "/api/" + (child ? "study/" + study + "/" : "") + route;
    }

    static CastorEdcResource from(String resource) {
        for (CastorEdcResource item : values()) {
            if (item.id.equals(resource)) {
                return item;
            }
        }
        throw CastorEdcConfig.invalid("resources", "unsupported resource name");
    }

    CastorEdcReadException failure(String message) {
        return new CastorEdcReadException(id, message);
    }

    /**
     * Source of proof for continuation.
     *
     * <p>Only declared page collections can advance from page counts alone.
     */
    enum Paging {
        NONE,
        LINK,
        HAL,
        AUDIT
    }
}
