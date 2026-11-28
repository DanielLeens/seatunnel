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

package org.apache.seatunnel.connectors.seatunnel.cartcom;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Locale;

/**
 * Maps Cart.com resource names to fixed request paths and response collections.
 *
 * <p>Current responses can use the resource name. Some order endpoints also use a shorter
 * collection name which has to be recognized without losing any records.
 */
enum CartComResource {
    CUSTOMERS(true),
    ORDERS(true),
    ORDER_PAYMENTS(true, "payments"),
    ORDER_STATUSES(false),
    ORDER_ITEMS(true, "items"),
    PRODUCTS(true),
    ADDRESSES(true);

    final String id = name().toLowerCase(Locale.ROOT);
    final String path = "/api/v1/" + id;
    final boolean incremental;
    final String alternateCollection;

    CartComResource(boolean incremental) {
        this(incremental, null);
    }

    CartComResource(boolean incremental, String alternateCollection) {
        this.incremental = incremental;
        this.alternateCollection = alternateCollection;
    }

    static CartComResource parse(String name) {
        for (CartComResource value : values()) {
            if (value.id.equals(name)) {
                return value;
            }
        }
        throw CartComConfig.invalid("resource must be a documented Cart.com resource");
    }

    /**
     * Builds the exact target used for both transmission and central authentication.
     *
     * <p>Every page retains the same timestamp bounds and sort. Numeric IDs break equal-timestamp
     * ties in the provider's order before applying page boundaries.
     */
    String target(CartComConfig config, int page, Instant ceiling) {
        StringBuilder result = new StringBuilder(path).append("?count=").append(config.pageSize);
        if (page > 1) {
            result.append("&page=").append(page);
        }
        if (incremental) {
            String filter = "gte:" + config.lower + " AND lt:" + ceiling;
            result.append("&sort=updated_at%2Cid&updated_at=");
            result.append(URLEncoder.encode(filter, StandardCharsets.UTF_8));
        }
        return result.toString();
    }
}
