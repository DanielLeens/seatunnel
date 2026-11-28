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
package org.apache.seatunnel.connectors.seatunnel.chartmogul;

/**
 * Declares the ChartMogul resources and their native pagination or metric interval.
 *
 * <p>The four customer count resources share an endpoint and retain their own interval names.
 */
enum ChartMogulResource {
    CUSTOMERS("customers", "/v1/customers", null),
    ACTIVITIES("activities", "/v1/activities", null),
    CUSTOMER_DAILY_COUNT("customer_daily_count", "/v1/metrics/customer-count", "day"),
    CUSTOMER_WEEKLY_COUNT("customer_weekly_count", "/v1/metrics/customer-count", "week"),
    CUSTOMER_MONTHLY_COUNT("customer_monthly_count", "/v1/metrics/customer-count", "month"),
    CUSTOMER_QUARTERLY_COUNT("customer_quarterly_count", "/v1/metrics/customer-count", "quarter");

    final String name;
    final String path;
    final String interval;

    ChartMogulResource(String name, String path, String interval) {
        this.name = name;
        this.path = path;
        this.interval = interval;
    }

    boolean dated() {
        return this != CUSTOMERS;
    }

    int limit(ChartMogulConfig config) {
        return interval == null ? config.pageSize : config.maxItems;
    }

    Failure failure(String reason) {
        return new Failure(name, reason);
    }

    /**
     * Reports a bounded diagnostic without the API key, cursor or any original records.
     *
     * <p>The resource name gives context without exposing customer identifiers in logs.
     */
    static final class Failure extends RuntimeException {
        private static final long serialVersionUID = 1L;

        Failure(String resource, String reason) {
            super("ChartMogul " + resource + ": " + reason);
        }
    }
}
