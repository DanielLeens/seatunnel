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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

/**
 * Defines Clarifai Source options for one selected resource.
 *
 * <p>The fixed Clarifai v2 API authority scopes use of the personal access token.
 */
public final class ClarifaiSourceOptions {
    public static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Clarifai personal access token for resource discovery.");
    public static final Option<String> USER_ID =
            Options.key("user_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Clarifai application owner to enumerate.");

    public static final Option<Integer> PAGE_SIZE =
            Options.key("page_size")
                    .intType()
                    .noDefaultValue()
                    .withDescription("Optional page size override from 1 to 1000.");

    public static final Option<String> RESOURCE =
            Options.key("resource")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Clarifai v2 resource to read.");

    public static final Option<String> START_DATE =
            Options.key("start_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Initial inclusive ISO 8601 timestamp with an explicit offset.");

    public static final Option<String> END_DATE =
            Options.key("end_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Inclusive upper timestamp bound. Defaults to the first read time.");

    public static final Option<String> INCREMENTAL_START =
            Options.key("incremental_start")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Previous cursor timestamp for a later batch.");

    public static final Option<Integer> LOOKBACK_SECONDS =
            Options.key("lookback_seconds")
                    .intType()
                    .defaultValue(1)
                    .withDescription("Seconds of read overlap before the previous cursor.");

    public static final Option<Integer> MAX_PAGES =
            Options.key("max_pages")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("Total accepted responses across lists and parent discovery.");

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
                    .withDescription("Total HTTPS request and retry budget in milliseconds.");

    public static final Option<Integer> MAX_ATTEMPTS =
            Options.key("max_attempts")
                    .intType()
                    .defaultValue(4)
                    .withDescription("Maximum attempts for a retryable GET, including the first.");

    public static final Option<Integer> MAX_RESPONSE_BYTES =
            Options.key("max_response_bytes")
                    .intType()
                    .defaultValue(8 * 1024 * 1024)
                    .withDescription("Maximum wire or decompressed response size in bytes.");

    public static final Option<String> TLS_TRUST_CERTIFICATE =
            Options.key("tls_trust_certificate")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Absolute path to engine-local PEM or DER trust anchors.");

    private ClarifaiSourceOptions() {}
}
