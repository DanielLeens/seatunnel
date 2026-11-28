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
package org.apache.seatunnel.connectors.seatunnel.ciscomeraki;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

/**
 * Defines the supported Dashboard and Secure Connect operations in traversal order.
 *
 * <p>Organizations and networks provide the parent identifiers for nested reads.
 */
enum CiscoMerakiResource {
    ORGANIZATIONS("organizations", "/api/v1/organizations", "id", true),
    DATACENTERS(
            "datacenters", "/api/secureConnect/v1/organizations/{org}/datacenters", null, false),
    NETWORKS("organization_networks", "/api/v1/organizations/{org}/networks", "id", true),
    SETTINGS("organization_network_settings", "/api/v1/networks/{network}/settings", null, false),
    DEVICES(
            "organization_devices",
            "/api/v1/organizations/{org}/wirelessController/overview/byDevice",
            null,
            true),
    API_REQUESTS("organization_apiRequests", "/api/v1/organizations/{org}/apiRequests", null, true),
    ADMINS("organization_admins", "/api/v1/organizations/{org}/admins", "id", false),
    SAML("organization_saml", "/api/v1/organizations/{org}/saml", null, false);

    final String name;
    final String path;
    final String identity;
    final boolean paged;

    CiscoMerakiResource(String name, String path, String identity, boolean paged) {
        this.name = name;
        this.path = path;
        this.identity = identity;
        this.paged = paged;
    }

    String path(String organization, String network) {
        if (this == SETTINGS) {
            return path.replace("{network}", segment(network));
        }
        return this == ORGANIZATIONS ? path : path.replace("{org}", segment(organization));
    }

    static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    private String segment(String value) {
        if (value == null || value.equals(".") || value.equals("..")) {
            throw failure("missing or unsafe parent identity");
        }
        return encode(value);
    }

    Failure failure(String reason) {
        return new Failure(name, reason);
    }

    /**
     * Reports a fixed local diagnostic for a failed operation.
     *
     * <p>Credentials, continuation targets, response bodies, and parent values are excluded.
     */
    static final class Failure extends RuntimeException {
        private static final long serialVersionUID = 1L;

        Failure(String resource, String reason) {
            super("Cisco Meraki " + resource + ": " + reason);
        }
    }
}
