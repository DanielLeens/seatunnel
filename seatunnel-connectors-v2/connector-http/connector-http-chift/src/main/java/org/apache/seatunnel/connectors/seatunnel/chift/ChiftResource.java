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
package org.apache.seatunnel.connectors.seatunnel.chift;

import java.util.Locale;

/**
 * Chift metadata collections and their fixed paths.
 *
 * <p>Each parent is ordered immediately before its scoped child, regardless of output selection.
 */
enum ChiftResource {
    CONSUMERS("consumerid", false),
    CONNECTIONS("connectionid", true),
    SYNCS("syncid", false),
    EXECUTIONS("id", true);

    final String id = name().toLowerCase(Locale.ROOT);
    final String idField;
    final boolean child;

    ChiftResource(String idField, boolean child) {
        this.idField = idField;
        this.child = child;
    }

    String path(String parent) {
        switch (this) {
            case CONNECTIONS:
                return "/consumers/" + parent + "/connections";
            case EXECUTIONS:
                return "/syncs/" + parent + "/executions";
            default:
                return "/" + id;
        }
    }

    static ChiftResource from(String value) {
        for (ChiftResource candidate : values()) {
            if (candidate.id.equals(value)) {
                return candidate;
            }
        }
        throw ChiftConfig.invalid("resources", "unsupported resource name");
    }

    ChiftReadException failure(String detail) {
        return new ChiftReadException(id, detail);
    }
}
