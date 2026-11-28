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
package org.apache.seatunnel.connectors.seatunnel.circleci;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;

import java.io.Serializable;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.Base64;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Validated source options and the deterministic order for scoped CircleCI reads.
 *
 * <p>The restore scope includes hashed credential material and options that change the scanned
 * data. Authentication itself is available only from the current job configuration.
 */
final class CircleCiConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Pattern UUID =
            Pattern.compile("[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}");
    private static final Pattern SLUG = Pattern.compile("[a-zA-Z0-9._-]{1,128}");
    private static final Set<String> WINDOWS =
            Set.of("last-24-hours", "last-7-days", "last-30-days", "last-60-days", "last-90-days");

    static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("CircleCI personal API token.");
    static final Option<List<String>> RESOURCES =
            Options.key("resources")
                    .listType()
                    .noDefaultValue()
                    .withDescription("CircleCI resources to read.");
    static final Option<String> ORG_ID =
            Options.key("org_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Organization UUID for context.");
    static final Option<String> ORG_SLUG =
            Options.key("org_slug")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Organization slug in vcs/organization form for pipelines.");
    static final Option<String> PROJECT_SLUG =
            Options.key("project_slug")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Project slug in vcs/organization/project form.");
    static final Option<List<String>> WORKFLOW_IDS =
            Options.key("workflow_ids")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "Workflow UUIDs for workflow, workflow jobs, or job discovery.");
    static final Option<Long> JOB_NUMBER =
            Options.key("job_number")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Direct job number. If omitted, discover jobs from configured workflows.");
    static final Option<String> START_DATE =
            Options.key("start_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Inclusive timestamp with an offset for dated records.");
    static final Option<String> END_DATE =
            Options.key("end_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Exclusive timestamp with an offset for dated records.");
    static final Option<String> REPORTING_WINDOW =
            Options.key("reporting_window")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Insights summary reporting window. The provider default is last-90-days.");
    static final Option<Integer> MAX_JOBS =
            Options.key("max_jobs")
                    .intType()
                    .defaultValue(4096)
                    .withDescription("Maximum unique job numbers retained during discovery.");
    static final Option<Integer> MAX_REQUESTS =
            Options.key("max_requests")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("Maximum successful responses processed in one scan.");
    static final Option<Integer> MAX_ATTEMPTS =
            Options.key("max_attempts")
                    .intType()
                    .defaultValue(4)
                    .withDescription("Maximum attempts per request including the initial call.");
    static final Option<Integer> TIMEOUT =
            Options.key("request_timeout_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription(
                            "Page deadline in milliseconds including retries, transfer and decoding.");
    static final Option<Integer> MAX_BYTES =
            Options.key("max_response_bytes")
                    .intType()
                    .defaultValue(8388608)
                    .withDescription("Maximum decoded response bytes.");
    static final Option<String> ENDPOINT =
            Options.key("endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional custom HTTPS origin requiring a worker-local trust file.");
    static final Option<String> TRUST_CERTIFICATE =
            Options.key("trust_certificate")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Worker-local PEM certificate file for a custom HTTPS origin.");

    final String token;
    final List<CircleCiResource> resources;
    /**
     * A discovered job number is scoped to the configured project.
     *
     * <p>Add the workflow jobs stage only when the selected job details have no explicit number.
     */
    final List<CircleCiResource> plan;

    final String orgId;
    final String orgSlug;
    final String projectSlug;
    final List<String> workflowIds;
    final Long jobNumber;
    final Instant start;
    final Instant end;
    final String reportingWindow;
    final int maxJobs;
    final int maxRequests;
    final int attempts;
    final int timeout;
    final int maxBytes;
    final String origin;
    final String trustCertificate;
    final String scope;

    CircleCiConfig(ReadonlyConfig options) {
        token = options.get(TOKEN);
        if (token == null
                || token.isEmpty()
                || token.length() > 4096
                || token.chars().anyMatch(character -> character < 33 || character > 126)) {
            throw invalid("token", "expected a nonempty personal API token");
        }
        EnumSet<CircleCiResource> requested = selection(options.get(RESOURCES));
        resources = List.copyOf(requested);
        orgId = uuid(options.get(ORG_ID), "org_id", requested.contains(CircleCiResource.CONTEXT));
        orgSlug =
                slug(
                        options.get(ORG_SLUG),
                        "org_slug",
                        2,
                        requested.contains(CircleCiResource.PIPELINES));
        projectSlug =
                slug(
                        options.get(PROJECT_SLUG),
                        "project_slug",
                        3,
                        requested.contains(CircleCiResource.JOBS)
                                || requested.contains(CircleCiResource.SPECIFIC_PROJECT)
                                || requested.contains(CircleCiResource.INSIGHTS_METRICS)
                                || requested.contains(CircleCiResource.INSIGHTS_BRANCHES));
        jobNumber = options.get(JOB_NUMBER);
        if (jobNumber != null && (jobNumber < 0 || !requested.contains(CircleCiResource.JOBS))) {
            throw invalid("job_number", "expected a nonnegative number for jobs");
        }
        if (discoversJobs()) {
            requested.add(CircleCiResource.WORKFLOW_JOBS);
        }
        workflowIds =
                workflows(
                        options.get(WORKFLOW_IDS),
                        requested.contains(CircleCiResource.WORKFLOW)
                                || requested.contains(CircleCiResource.WORKFLOW_JOBS));
        plan = List.copyOf(requested);
        start = timestamp(options.get(START_DATE), "start_date");
        end = timestamp(options.get(END_DATE), "end_date");
        if ((start != null || end != null)
                && resources.stream().noneMatch(CircleCiResource::hasTime)) {
            throw invalid("start_date / end_date", "a temporal resource must be selected");
        }
        if (start != null && end != null && !end.isAfter(start)) {
            throw invalid("end_date", "must be later than start_date");
        }
        reportingWindow = options.get(REPORTING_WINDOW);
        if (reportingWindow != null
                && (!WINDOWS.contains(reportingWindow)
                        || !requested.contains(CircleCiResource.INSIGHTS_METRICS))) {
            throw invalid("reporting_window", "choose a supported Insights summary window");
        }
        maxJobs = range(options, MAX_JOBS, 1, 10000);
        maxRequests = range(options, MAX_REQUESTS, 1, 20000);
        attempts = range(options, MAX_ATTEMPTS, 1, 6);
        timeout = range(options, TIMEOUT, 100, 300000);
        maxBytes = range(options, MAX_BYTES, 1024, 32 * 1024 * 1024);
        String endpoint = options.get(ENDPOINT);
        trustCertificate = options.get(TRUST_CERTIFICATE);
        if ((endpoint == null) != (trustCertificate == null)
                || (trustCertificate != null && trustCertificate.isBlank())) {
            throw invalid("endpoint", "custom HTTPS requires trust_certificate");
        }
        origin = endpoint == null ? "https://circleci.com" : httpsOrigin(endpoint);
        scope =
                digest(
                        String.join(
                                "\n",
                                "circleci-v1",
                                digest(token),
                                resources.toString(),
                                Objects.toString(orgId, ""),
                                Objects.toString(orgSlug, ""),
                                Objects.toString(projectSlug, ""),
                                workflowIds.toString(),
                                Objects.toString(jobNumber, ""),
                                Objects.toString(start, ""),
                                Objects.toString(end, ""),
                                Objects.toString(reportingWindow, ""),
                                Integer.toString(maxJobs),
                                Integer.toString(maxRequests),
                                Integer.toString(maxBytes),
                                origin,
                                Objects.toString(trustCertificate, "")));
    }

    static OptionRule rules() {
        return OptionRule.builder()
                .required(TOKEN, RESOURCES)
                .optional(
                        ORG_ID,
                        ORG_SLUG,
                        PROJECT_SLUG,
                        WORKFLOW_IDS,
                        JOB_NUMBER,
                        START_DATE,
                        END_DATE,
                        REPORTING_WINDOW,
                        MAX_JOBS,
                        MAX_REQUESTS,
                        MAX_ATTEMPTS,
                        TIMEOUT,
                        MAX_BYTES,
                        ENDPOINT,
                        TRUST_CERTIFICATE)
                .build();
    }

    private static EnumSet<CircleCiResource> selection(List<String> names) {
        if (names == null || names.isEmpty() || names.size() > CircleCiResource.values().length) {
            throw invalid("resources", "choose at least one supported resource");
        }
        EnumSet<CircleCiResource> selected = EnumSet.noneOf(CircleCiResource.class);
        for (String name : names) {
            if (!selected.add(CircleCiResource.from(name))) {
                throw invalid("resources", "choose each resource only once");
            }
        }
        return selected;
    }

    private static List<String> workflows(List<String> selected, boolean required) {
        if (selected == null && !required) {
            return List.of();
        }
        if (selected == null || selected.isEmpty() || selected.size() > 100) {
            throw invalid("workflow_ids", "choose between one and 100 workflow UUIDs");
        }
        Set<String> ordered = new LinkedHashSet<>();
        for (String id : selected) {
            if (!ordered.add(uuid(id, "workflow_ids", true))) {
                throw invalid("workflow_ids", "workflow UUIDs must be unique");
            }
        }
        return List.copyOf(ordered);
    }

    boolean discoversJobs() {
        return resources.contains(CircleCiResource.JOBS) && jobNumber == null;
    }

    Instant cutoff() {
        if (end != null || start == null) {
            return end;
        }
        Instant now = Instant.now();
        if (!now.isAfter(start)) {
            throw invalid("start_date", "must precede the captured cutoff");
        }
        return now;
    }

    static boolean isUuid(String value) {
        return value != null && UUID.matcher(value).matches();
    }

    private static String uuid(String value, String key, boolean required) {
        if (value == null && !required) {
            return null;
        }
        if (!isUuid(value)) {
            throw invalid(key, "expected a UUID");
        }
        return value.toLowerCase(Locale.ROOT);
    }

    private static String slug(String value, String key, int parts, boolean required) {
        if (value == null && !required) {
            return null;
        }
        if (value == null || value.length() > 386) {
            throw invalid(key, "expected a scoped CircleCI slug");
        }
        String[] segments = value.split("/", -1);
        if (segments.length != parts) {
            throw invalid(key, "expected a scoped CircleCI slug");
        }
        for (String segment : segments) {
            if (!SLUG.matcher(segment).matches() || ".".equals(segment) || "..".equals(segment)) {
                throw invalid(key, "expected a scoped CircleCI slug");
            }
        }
        return value;
    }

    static boolean sameProject(String actual, String requested) {
        return shortVcs(actual).equals(shortVcs(requested));
    }

    private static String shortVcs(String slug) {
        if (slug.startsWith("github/")) {
            return "gh/" + slug.substring(7);
        }
        if (slug.startsWith("bitbucket/")) {
            return "bb/" + slug.substring(10);
        }
        return slug;
    }

    private static Instant timestamp(String value, String key) {
        if (value == null) {
            return null;
        }
        if (value.length() <= 64) {
            try {
                return OffsetDateTime.parse(value).toInstant();
            } catch (DateTimeParseException ignored) {
                // Option values may contain sensitive material.
            }
        }
        throw invalid(key, "expected an ISO-8601 timestamp with a UTC offset");
    }

    private static String httpsOrigin(String value) {
        try {
            URI uri = URI.create(value);
            if ("https".equalsIgnoreCase(uri.getScheme())
                    && uri.getHost() != null
                    && uri.getRawUserInfo() == null
                    && uri.getRawQuery() == null
                    && uri.getRawFragment() == null
                    && (uri.getPort() == -1 || (uri.getPort() > 0 && uri.getPort() <= 65535))
                    && ("".equals(uri.getRawPath()) || "/".equals(uri.getRawPath()))) {
                return "https://" + uri.getRawAuthority();
            }
        } catch (IllegalArgumentException ignored) {
            // Do not echo the invalid endpoint.
        }
        throw invalid("endpoint", "expected a bare HTTPS origin");
    }

    private static int range(ReadonlyConfig options, Option<Integer> option, int min, int max) {
        int value = options.get(option);
        if (value < min || value > max) {
            throw invalid(option.key(), "expected a value from " + min + " to " + max);
        }
        return value;
    }

    static String digest(String value) {
        return digest(value.getBytes(StandardCharsets.UTF_8));
    }

    static String digest(byte[] value) {
        try {
            return Base64.getUrlEncoder()
                    .withoutPadding()
                    .encodeToString(MessageDigest.getInstance("SHA-256").digest(value));
        } catch (NoSuchAlgorithmException unavailable) {
            throw new IllegalStateException("SHA-256 is unavailable");
        }
    }

    static IllegalArgumentException invalid(String key, String message) {
        return new IllegalArgumentException("CircleCI option " + key + ": " + message);
    }
}
