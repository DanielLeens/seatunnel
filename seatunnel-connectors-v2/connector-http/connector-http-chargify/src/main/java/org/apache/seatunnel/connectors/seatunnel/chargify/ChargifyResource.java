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
package org.apache.seatunnel.connectors.seatunnel.chargify;

import java.util.Locale;

/**
 * Static Chargify collection routes and their provider response shape.
 *
 * <p>Every request path is derived from one of these known resources.
 */
enum ChargifyResource {
    CUSTOMERS("customer"),
    SUBSCRIPTIONS("subscription"),
    INVOICES(null),
    COUPONS("coupon"),
    TRANSACTIONS("transaction");

    final String id = name().toLowerCase(Locale.ROOT);
    final String wrapper;

    ChargifyResource(String wrapper) {
        this.wrapper = wrapper;
    }

    String file() {
        return id + ".json";
    }

    static ChargifyResource from(String name) {
        for (ChargifyResource entry : values()) {
            if (entry.id.equals(name)) {
                return entry;
            }
        }
        throw ChargifyConfig.invalid("resources", "unsupported resource name");
    }

    ChargifyReadException failure(String detail) {
        return new ChargifyReadException(id, detail);
    }
}
