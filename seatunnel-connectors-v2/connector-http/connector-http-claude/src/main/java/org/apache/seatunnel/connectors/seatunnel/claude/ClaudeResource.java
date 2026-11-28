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
package org.apache.seatunnel.connectors.seatunnel.claude;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;

/**
 * Defines the selected Enterprise Analytics operations and their native window sizes.
 *
 * <p>Usage and cost retain the original product and model grouping without flattening buckets.
 */
enum ClaudeResource {
    USERS("users", "users", Kind.DAILY, 1, "user", "id"),
    CHAT_PROJECTS("chat_projects", "apps/chat/projects", Kind.DAILY, 1, "project_id", null),
    SKILLS("skills", "skills", Kind.DAILY, 1, "skill_name", null),
    CONNECTORS("connectors", "connectors", Kind.DAILY, 1, "connector_name", null),
    SUMMARIES("summaries", "summaries", Kind.SUMMARY, 1, null, null),
    USER_USAGE_REPORT(
            "user_usage_report", "user_usage_report", Kind.USER_REPORT, 1, "actor", "user_id"),
    USER_COST_REPORT(
            "user_cost_report", "user_cost_report", Kind.USER_REPORT, 1, "actor", "user_id"),
    USAGE_REPORT("usage_report", "usage_report", Kind.BUCKET_REPORT, 7, null, null),
    COST_REPORT("cost_report", "cost_report", Kind.BUCKET_REPORT, 7, null, null);

    private static final String PREFIX = "/v1/organizations/analytics/";

    final String name;
    final String route;
    final Kind kind;
    final int days;
    final String identityField;
    final String nestedIdentityField;

    ClaudeResource(
            String name,
            String route,
            Kind kind,
            int days,
            String identityField,
            String nestedIdentityField) {
        this.name = name;
        this.route = PREFIX + route;
        this.kind = kind;
        this.days = days;
        this.identityField = identityField;
        this.nestedIdentityField = nestedIdentityField;
    }

    boolean isReport() {
        return kind == Kind.USER_REPORT || kind == Kind.BUCKET_REPORT;
    }

    int limit(int configured) {
        return kind == Kind.BUCKET_REPORT ? Math.min(configured, 31) : configured;
    }

    List<String> groupBy() {
        if (this == USER_COST_REPORT || this == COST_REPORT) {
            return List.of("product", "model", "cost_type");
        }
        return List.of("product", "model");
    }

    static String midnight(LocalDate date) {
        return date.atStartOfDay().toInstant(ZoneOffset.UTC).toString();
    }

    Failure failure(String reason) {
        return new Failure(name, reason);
    }

    /**
     * Separates daily cards, unpaginated summaries, and both reporting envelope shapes.
     *
     * <p>The connector does not use the newer report endpoints in these legacy resource plans.
     */
    enum Kind {
        DAILY,
        SUMMARY,
        USER_REPORT,
        BUCKET_REPORT
    }

    /**
     * Reports a resource-specific failure without raw credential, response, or cursor data.
     *
     * <p>Every producer supplies only an internal, fixed diagnostic.
     */
    static final class Failure extends RuntimeException {
        private static final long serialVersionUID = 1L;

        Failure(String resource, String reason) {
            super("Claude Analytics " + resource + ": " + reason);
        }
    }
}
