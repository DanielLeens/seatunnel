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
package org.apache.seatunnel.connectors.seatunnel.carequalitycommission;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.util.OptionRule;

import java.util.List;

/**
 * Configures CQC Syndication reads and bounded paging.
 *
 * <p>The Syndication subscription key uses the default sensitive token option name.
 */
public final class CareQualityCommissionSourceOptions {
    public static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("CareQualityCommission Syndication subscription key.");
    public static final Option<List<String>> RESOURCES =
            Options.key("resources")
                    .listType()
                    .defaultValue(List.of())
                    .withDescription("Resource names. An empty list reads all six.");
    public static final Option<String> CERTIFICATE =
            Options.key("tls_trust_certificate")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional source-local PEM or base64 DER X.509 trust anchor.");
    public static final Option<Integer> PAGE_SIZE =
            number(
                    "page_size",
                    1000,
                    "Items requested per page for location and provider lists, from 1 to 1000.");
    public static final Option<Integer> MAX_PAGES =
            number("max_pages_per_collection", 10000, "Maximum accepted pages per collection.");
    public static final Option<Integer> MAX_SCAN_PAGES =
            number("max_pages_per_scan", 500000, "Maximum confirmed responses across the scan.");
    public static final Option<Integer> MAX_ITEMS =
            number(
                    "max_unpaginated_items",
                    10000,
                    "Maximum number of entries in a complete list response.");
    public static final Option<Integer> CONNECT_TIMEOUT =
            number("connect_timeout_ms", 10000, "TCP connect timeout.");
    public static final Option<Integer> REQUEST_TIMEOUT =
            number("request_timeout_ms", 30000, "Deadline for one HTTPS attempt.");
    public static final Option<Integer> OPERATION_TIMEOUT =
            number(
                    "operation_timeout_ms",
                    120000,
                    "Deadline across retries and waits of one read.");
    public static final Option<Integer> INTERVAL =
            number("request_interval_ms", 0, "Minimum time between HTTPS attempts.");
    public static final Option<Integer> RETRIES =
            number("max_retries", 3, "Maximum additional attempts for a failed read.");
    public static final Option<Integer> RETRY_DELAY =
            number("max_retry_delay_ms", 60000, "Maximum accepted server retry delay or backoff.");
    public static final Option<Integer> BODY_LIMIT =
            number("max_response_bytes", 8388608, "Maximum bytes before and after decompression.");

    private CareQualityCommissionSourceOptions() {}

    static OptionRule rule() {
        return OptionRule.builder()
                .required(TOKEN)
                .optional(
                        RESOURCES,
                        CERTIFICATE,
                        PAGE_SIZE,
                        MAX_PAGES,
                        MAX_SCAN_PAGES,
                        MAX_ITEMS,
                        CONNECT_TIMEOUT,
                        REQUEST_TIMEOUT,
                        OPERATION_TIMEOUT,
                        INTERVAL,
                        RETRIES,
                        RETRY_DELAY,
                        BODY_LIMIT)
                .build();
    }

    private static Option<Integer> number(String name, int value, String description) {
        return Options.key(name).intType().defaultValue(value).withDescription(description);
    }
}
