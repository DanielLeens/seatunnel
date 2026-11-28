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
package org.apache.seatunnel.connectors.seatunnel.capsulecrm;

import java.util.Locale;

/**
 * Fixed Capsule v2 read routes and their response envelopes.
 *
 * <p>The declaration order places the organisation discovery phase before employee lookup.
 */
enum CapsuleCrmResource {
    USERS("users", "users", false),
    PARTIES("parties", "parties"),
    TASKS("tasks", "tasks"),
    EMPLOYEES("parties/%s/people", "parties"),
    PROJECTS("kases", "kases"),
    OPPORTUNITIES("opportunities", "opportunities"),
    PIPELINES("pipelines", "pipelines"),
    MILESTONES("milestones", "milestones"),
    SITE("site", "site", false),
    TAGS("%s/tags", "tags"),
    CUSTOM_FIELDS("%s/fields/definitions", "definitions"),
    LOST_REASONS("lostreasons", "lostReasons", false),
    BOARD("boards", "boards"),
    CATEGORIES("categories", "categories"),
    ACTIVITY_TYPES("activitytypes", "activityTypes"),
    STAGES("stages", "stages");

    final String id = name().toLowerCase(Locale.ROOT);
    final String envelope;
    final boolean pageSizeSupported;
    private final String route;

    CapsuleCrmResource(String route, String envelope) {
        this(route, envelope, true);
    }

    CapsuleCrmResource(String route, String envelope, boolean pageSizeSupported) {
        this.route = route;
        this.envelope = envelope;
        this.pageSizeSupported = pageSizeSupported;
    }

    String path(String entity, String parent) {
        String segment = this == EMPLOYEES ? parent : entity;
        return "/api/v2/" + (route.contains("%s") ? route.replace("%s", segment) : route);
    }

    static CapsuleCrmResource from(String resource) {
        for (CapsuleCrmResource item : values()) {
            if (item.id.equals(resource)) {
                return item;
            }
        }
        throw CapsuleCrmConfig.invalid("resources", "unsupported resource name");
    }

    CapsuleCrmReadException failure(String message) {
        return new CapsuleCrmReadException(id, message);
    }
}
