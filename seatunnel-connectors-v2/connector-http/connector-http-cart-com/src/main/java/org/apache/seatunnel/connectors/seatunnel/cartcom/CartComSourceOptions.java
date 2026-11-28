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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

/**
 * User options for the bounded Cart.com source.
 *
 * <p>Each source selects one resource and one merchant or central router credential set.
 */
public final class CartComSourceOptions {
    public static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Access token issued for the configured Cart.com merchant host.");
    public static final Option<String> RESOURCE =
            Options.key("resource")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("One supported Cart.com export resource.");
    public static final Option<String> STORE_HOST =
            Options.key("store_host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Cart.com merchant DNS hostname for single-store authentication.");
    public static final Option<String> SITE_ID =
            Options.key("site_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Site ID from Cart.com central router provisioning.");
    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Cart.com central router provisioning user.");
    public static final Option<String> SECRET_KEY =
            Options.key("secret_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("HMAC secret issued to the Cart.com provisioning user.");
    public static final Option<String> START_DATE =
            Options.key("start_date")
                    .stringType()
                    .defaultValue("1970-01-01T00:00:00Z")
                    .withDescription("Inclusive starting timestamp for resources with updated_at.");
    public static final Option<String> END_DATE =
            Options.key("end_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Exclusive timestamp bound. Defaults to the first read time.");
    public static final Option<String> INCREMENTAL_START =
            Options.key("incremental_start")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional prior updated_at checkpoint for a new batch.");
    public static final Option<Integer> LOOKBACK_SECONDS =
            Options.key("lookback_seconds")
                    .intType()
                    .defaultValue(0)
                    .withDescription("Seconds to replay before the prior incremental timestamp.");

    public static final Option<Integer> PAGE_SIZE =
            Options.key("page_size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "Number of records requested per cursor page, from 1 to 1000.");
    public static final Option<Integer> MAX_PAGES =
            Options.key("max_pages")
                    .intType()
                    .defaultValue(2000)
                    .withDescription(
                            "Maximum numbered pages in the export, including empty pages.");
    public static final Option<Integer> CONNECT_TIMEOUT_MS =
            Options.key("connect_timeout_ms")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("Maximum TCP connect time in milliseconds.");
    public static final Option<Integer> READ_TIMEOUT_MS =
            Options.key("read_timeout_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription("Maximum socket read time in milliseconds.");
    public static final Option<Integer> REQUEST_DEADLINE_MS =
            Options.key("request_deadline_ms")
                    .intType()
                    .defaultValue(60000)
                    .withDescription("Total HTTP request and retry budget in milliseconds.");
    public static final Option<Integer> MAX_ATTEMPTS =
            Options.key("max_attempts")
                    .intType()
                    .defaultValue(4)
                    .withDescription("Maximum HTTP attempts, including the initial request.");
    public static final Option<Integer> MAX_RESPONSE_BYTES =
            Options.key("max_response_bytes")
                    .intType()
                    .defaultValue(8 * 1024 * 1024)
                    .withDescription("Maximum wire and decoded response size in bytes.");
    public static final Option<String> TLS_TRUST_CERTIFICATE =
            Options.key("tls_trust_certificate")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Engine-local PEM or DER trust anchor file. Defaults to JVM trust.");

    private CartComSourceOptions() {}
}
