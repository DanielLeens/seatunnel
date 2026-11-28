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

package org.apache.seatunnel.connectors.seatunnel.circa;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

/**
 * Maps one selectable resource to the fixed Circa v1 wire contract.
 *
 * <p>Relationships depend on an independent full inventory of their parent resource.
 */
enum CircaResource {
    EVENTS("events", "/events", true, true, null, null),
    CONTACTS("contacts", "/contacts", true, true, null, null),
    TEAMS("teams", "/teams", false, false, null, null),
    COMPANIES("companies", "/companies", true, true, null, null),
    COMPANY_CONTACTS("company_contacts", "/companies", true, false, null, COMPANIES),
    EVENT_FIELDS("event_fields", "/fields", false, false, "Event", null),
    CONTACT_FIELDS("contact_fields", "/fields", false, false, "Contact", null),
    COMPANY_FIELDS("company_fields", "/fields", false, false, "Company", null),
    EVENT_CONTACTS("event_contacts", "/events", true, false, null, EVENTS);

    final String id;
    final String path;
    final boolean paged;
    final boolean incremental;
    final String fieldFor;
    final CircaResource parent;

    CircaResource(
            String id,
            String path,
            boolean paged,
            boolean incremental,
            String fieldFor,
            CircaResource parent) {
        this.id = id;
        this.path = path;
        this.paged = paged;
        this.incremental = incremental;
        this.fieldFor = fieldFor;
        this.parent = parent;
    }

    static CircaResource of(String id) {
        for (CircaResource resource : values()) {
            if (resource.id.equals(id)) {
                return resource;
            }
        }
        throw new IllegalArgumentException("Circa source: unsupported resource");
    }

    String target(String parentId) {
        if (parent == null) {
            return "/api/v1" + path;
        }
        return "/api/v1"
                + path
                + "/"
                + URLEncoder.encode(parentId, StandardCharsets.UTF_8)
                        .replace("+", "%20")
                        .replace("*", "%2A")
                + "/contacts";
    }
}
