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
package org.apache.seatunnel.connectors.seatunnel.chameleon;

/**
 * Links each Chameleon collection to its read-only endpoint and native timestamp.
 *
 * <p>Microsurvey responses use a survey identifier obtained through the survey list.
 */
enum ChameleonResource {
    HELPBARS("helpbars", "/v3/edit/searches", "searches", Shape.COMPLETE, false),
    SEGMENTS("segments", "/v3/edit/segments", "segments", Shape.CURSOR, false),
    URLS("urls", "/v3/edit/urls", "urls", Shape.CURSOR, false),
    URL_GROUPS("url_groups", "/v3/edit/url_groups", "url_groups", Shape.CURSOR, false),
    CHANGES("changes", "/v3/edit/changes", "changes", Shape.CURSOR, false),
    LAUNCHERS("launchers", "/v3/edit/launchers", "launchers", Shape.CURSOR, false),
    TOOLTIPS("tooltips", "/v3/edit/tooltips", "tooltips", Shape.CURSOR, false),
    TOURS("tours", "/v3/edit/tours", "tours", Shape.CURSOR, false),
    SURVEYS("surveys", "/v3/edit/surveys", "surveys", Shape.CURSOR, false),
    SURVEY_RESPONSES("survey_responses", "/v3/analyze/responses", "responses", Shape.CURSOR, true);

    final String name;
    final String path;
    final String items;
    final Shape shape;
    final boolean scoped;

    ChameleonResource(String name, String path, String items, Shape shape, boolean scoped) {
        this.name = name;
        this.path = path;
        this.items = items;
        this.shape = shape;
        this.scoped = scoped;
    }

    String timestamp() {
        return this == CHANGES ? "created_at" : "updated_at";
    }

    Failure failure(String reason) {
        return new Failure(name, reason);
    }

    /**
     * Distinguishes the full HelpBar result from cursor-paginated lists.
     *
     * <p>Only paginated lists include query parameters for positioning.
     */
    enum Shape {
        COMPLETE,
        CURSOR
    }

    /**
     * Carries an error message safe to show in the job.
     *
     * <p>Request credentials, identifiers, continuation values and response payloads are excluded.
     */
    static final class Failure extends RuntimeException {
        private static final long serialVersionUID = 1L;

        Failure(String name, String reason) {
            super("Chameleon " + name + ": " + reason);
        }
    }
}
