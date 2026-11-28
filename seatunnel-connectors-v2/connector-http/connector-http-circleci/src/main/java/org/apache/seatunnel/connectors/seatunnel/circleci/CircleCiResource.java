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
package org.apache.seatunnel.connectors.seatunnel.circleci;

import okhttp3.HttpUrl;

/**
 * CircleCI endpoints with their response shapes and optional time coordinates.
 *
 * <p>Only a resource can construct its native selectors and decide whether a provider token
 * applies.
 */
enum CircleCiResource {
    CONTEXT("context", "/api/v2/context", Shape.PAGE, "created_at", true),
    SELF_IDS("self_ids", "/api/v2/me", Shape.OBJECT, null, false),
    SELF_COLLABORATIONS(
            "self_collaborations", "/api/v2/me/collaborations", Shape.ARRAY, null, false),
    ME("me", "/api/v1.1/me", Shape.OBJECT, "created_at", false),
    PROJECTS("projects", "/api/v1.1/projects", Shape.ARRAY, null, false),
    PIPELINES("pipelines", "/api/v2/pipeline", Shape.PAGE, "created_at", true),
    SPECIFIC_PROJECT("specific_project", "/api/v2/project", Shape.OBJECT, null, false),
    WORKFLOW("workflow", "/api/v2/workflow", Shape.OBJECT, "created_at", true),
    WORKFLOW_JOBS("workflow_jobs", "/api/v2/workflow", Shape.PAGE, "started_at", false),
    JOBS("jobs", "/api/v2/project", Shape.OBJECT, null, false),
    INSIGHTS_METRICS("insights_metrics", "/api/v2/insights/pages", Shape.OBJECT, null, false),
    INSIGHTS_BRANCHES("insights_branches", "/api/v2/insights", Shape.OBJECT, null, false);

    /**
     * Separates singleton and flat-array responses from the native paginated envelope.
     *
     * <p>Only the latter can return a continuation.
     */
    enum Shape {
        PAGE,
        ARRAY,
        OBJECT
    }

    final String id;
    final Shape shape;
    final String timeField;
    final boolean timeRequired;
    private final String path;

    CircleCiResource(String id, String path, Shape shape, String timeField, boolean timeRequired) {
        this.id = id;
        this.path = path;
        this.shape = shape;
        this.timeField = timeField;
        this.timeRequired = timeRequired;
    }

    static CircleCiResource from(String name) {
        for (CircleCiResource resource : values()) {
            if (resource.id.equals(name)) {
                return resource;
            }
        }
        throw CircleCiConfig.invalid("resources", "unknown resource");
    }

    boolean hasTime() {
        return timeField != null;
    }

    boolean perWorkflow() {
        return this == WORKFLOW || this == WORKFLOW_JOBS;
    }

    /**
     * Appends only selectors accepted by this route and uses the configured origin for every page.
     */
    HttpUrl target(HttpUrl origin, CircleCiConfig settings, CircleCiSplit position) {
        HttpUrl.Builder builder = origin.newBuilder().encodedPath(path);
        switch (this) {
            case CONTEXT:
                builder.addQueryParameter("owner-id", settings.orgId);
                break;
            case PIPELINES:
                builder.addQueryParameter("org-slug", settings.orgSlug);
                break;
            case WORKFLOW:
                builder.addPathSegment(settings.workflowIds.get(position.workflowIndex));
                break;
            case WORKFLOW_JOBS:
                builder.addPathSegment(settings.workflowIds.get(position.workflowIndex))
                        .addPathSegment("job");
                break;
            case JOBS:
                builder.addPathSegments(settings.projectSlug)
                        .addPathSegment("job")
                        .addPathSegment(Long.toString(position.jobNumber(settings)));
                break;
            case SPECIFIC_PROJECT:
                builder.addPathSegments(settings.projectSlug);
                break;
            case INSIGHTS_METRICS:
                builder.addPathSegments(settings.projectSlug).addPathSegment("summary");
                if (settings.reportingWindow != null) {
                    builder.addQueryParameter("reporting-window", settings.reportingWindow);
                }
                break;
            case INSIGHTS_BRANCHES:
                builder.addPathSegments(settings.projectSlug).addPathSegment("branches");
                break;
            default:
                break;
        }
        if (shape == Shape.PAGE && position.token != null) {
            builder.addQueryParameter("page-token", position.token);
        }
        return builder.build();
    }

    String parent(CircleCiConfig settings, CircleCiSplit position) {
        switch (this) {
            case CONTEXT:
                return settings.orgId;
            case PIPELINES:
                return settings.orgSlug;
            case WORKFLOW_JOBS:
                return settings.workflowIds.get(position.workflowIndex);
            case INSIGHTS_METRICS:
            case INSIGHTS_BRANCHES:
            case JOBS:
                return settings.projectSlug;
            default:
                return null;
        }
    }

    CircleCiReadException failure(String detail) {
        return new CircleCiReadException(id, detail);
    }
}
