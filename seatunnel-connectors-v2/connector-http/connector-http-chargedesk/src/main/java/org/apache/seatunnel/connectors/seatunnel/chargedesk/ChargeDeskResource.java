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
package org.apache.seatunnel.connectors.seatunnel.chargedesk;

/**
 * Declares the supported list routes, identity and time fields, and native offset limits.
 *
 * <p>The server-side filter is specified only when it refers to the time field being exported.
 */
enum ChargeDeskResource {
    CHARGES("charges", "charge_id", "occurred", "occurred", 50000),
    CUSTOMERS("customers", "customer_id", "first_seen", "first_seen", 50000),
    SUBSCRIPTIONS("subscriptions", "subscription_id", "first_seen", null, 10000),
    PRODUCTS("products", "product_id", "first_seen", null, 10000);

    final String name;
    final String identity;
    final String timeField;
    final String timeFilter;
    final int maxOffset;

    ChargeDeskResource(
            String name, String identity, String timeField, String timeFilter, int maxOffset) {
        this.name = name;
        this.identity = identity;
        this.timeField = timeField;
        this.timeFilter = timeFilter;
        this.maxOffset = maxOffset;
    }

    int requestCount(int pageSize, int offset) {
        int remaining = maxOffset - offset;
        return remaining == 0 ? pageSize : Math.min(pageSize, remaining);
    }

    boolean dated() {
        return this != PRODUCTS;
    }

    Failure failure(String reason) {
        return new Failure(name, reason);
    }

    /**
     * Reports a scoped error without including credentials, IDs or record values.
     *
     * <p>Raw gateway response details remain private to the transport.
     */
    static final class Failure extends RuntimeException {
        private static final long serialVersionUID = 1L;

        Failure(String collection, String reason) {
            super("ChargeDesk " + collection + ": " + reason);
        }
    }
}
