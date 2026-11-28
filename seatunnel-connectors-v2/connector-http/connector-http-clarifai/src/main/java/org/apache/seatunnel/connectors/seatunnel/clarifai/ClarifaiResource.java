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

package org.apache.seatunnel.connectors.seatunnel.clarifai;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

/**
 * Maps each selectable resource to its Clarifai v2 route and response key.
 *
 * <p>Depth counts the hidden app and model inventories used during a selected traversal.
 */
enum ClarifaiResource {
    APPLICATIONS("applications", "apps", "", 0, 20, true),
    DATASETS("datasets", "datasets", "/datasets", 1, 100, true),
    MODELS("models", "models", "/models", 1, 100, true),
    MODEL_VERSIONS("model_versions", "model_versions", "", 2, 100, false),
    WORKFLOWS("workflows", "workflows", "/workflows", 1, 100, false),
    APP_INPUTS("app_inputs", "inputs", "/inputs", 1, 20, false),
    APP_INPUTS_JOBS("app_inputs_jobs", "inputs_add_jobs", "/inputs/jobs/add", 1, 20, false),
    APP_CONCEPTS("app_concepts", "concepts", "/concepts", 1, 20, false);

    final String id;
    final String key;
    final String suffix;
    final int depth;
    final int defaultPageSize;
    final boolean incremental;

    ClarifaiResource(
            String id,
            String key,
            String suffix,
            int depth,
            int defaultPageSize,
            boolean incremental) {
        this.id = id;
        this.key = key;
        this.suffix = suffix;
        this.depth = depth;
        this.defaultPageSize = defaultPageSize;
        this.incremental = incremental;
    }

    static ClarifaiResource of(String id) {
        if ("worklows".equals(id)) {
            return WORKFLOWS;
        }
        for (ClarifaiResource resource : values()) {
            if (resource.id.equals(id)) {
                return resource;
            }
        }
        throw new IllegalArgumentException("Clarifai source: unsupported resource");
    }

    String target(String user, String app, String model, int page, int limit) {
        String base = "/v2/users/" + segment(user) + "/apps";
        if (depth > 0) {
            base += "/" + segment(app);
        }
        if (depth == 2) {
            base += "/models/" + segment(model) + "/versions";
        }
        return base + suffix + "?page=" + page + "&per_page=" + limit;
    }

    private static String segment(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8)
                .replace("+", "%20")
                .replace("*", "%2A");
    }
}
