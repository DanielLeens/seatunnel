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
package org.apache.seatunnel.connectors.seatunnel.capsulecrm;

import okhttp3.HttpUrl;

import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Extracts and validates Capsule's RFC-style next-page links.
 *
 * <p>Only the page number survives validation. A new request is assembled by the caller from its
 * original resource and scope each time.
 */
final class CapsuleCrmLink {
    private static final int MAX_HEADER_LENGTH = 16384;
    private static final Pattern TARGET = Pattern.compile("\\G[ \\t]*<([^<>\\s]+)>");
    private static final String TOKEN = "[!#$%&'*+.^_`|~0-9A-Za-z-]+";
    private static final Pattern PARAMETER =
            Pattern.compile(
                    "\\G[ \\t]*;[ \\t]*("
                            + TOKEN
                            + ")(?:[ \\t]*=[ \\t]*(?:\"((?:[^\"\\\\]|\\\\.)*+)\"|("
                            + TOKEN
                            + ")))?");
    private static final Pattern SEPARATOR = Pattern.compile("\\G[ \\t]*,[ \\t]*");
    private static final Pattern QUOTED_PAIR = Pattern.compile("\\\\(.)");

    private CapsuleCrmLink() {}

    static int next(List<String> headers, HttpUrl request, int current, CapsuleCrmResource resource)
            throws CapsuleCrmReadException {
        if (headers.isEmpty()) {
            return 0;
        }
        if (headers.size() > 32 || resource == CapsuleCrmResource.SITE) {
            throw resource.failure("unexpected or excessive Link metadata");
        }
        int total = 0;
        int result = 0;
        for (String header : headers) {
            total += header.length();
            if (total > MAX_HEADER_LENGTH || header.isBlank()) {
                throw resource.failure("invalid Link metadata");
            }
            int at = 0;
            Matcher target = TARGET.matcher(header);
            Matcher parameter = PARAMETER.matcher(header);
            Matcher separator = SEPARATOR.matcher(header);
            while (target.find(at)) {
                String link = target.group(1);
                at = target.end();
                String relation = null;
                while (parameter.find(at)) {
                    if ("rel".equalsIgnoreCase(parameter.group(1))) {
                        if (relation != null) {
                            throw resource.failure("duplicate relation in Link metadata");
                        }
                        String quoted = parameter.group(2);
                        relation =
                                quoted == null
                                        ? parameter.group(3)
                                        : QUOTED_PAIR.matcher(quoted).replaceAll("$1");
                        if (relation == null || relation.isBlank()) {
                            throw resource.failure("missing relation in Link metadata");
                        }
                    }
                    at = parameter.end();
                }
                if (relation != null && hasNext(relation)) {
                    if (result != 0) {
                        throw resource.failure("multiple next-page links");
                    }
                    result = validate(link, request, current, resource);
                }
                if (header.substring(at).isBlank()) {
                    at = header.length();
                    break;
                }
                if (!separator.find(at) || separator.end() == header.length()) {
                    break;
                }
                at = separator.end();
            }
            if (at != header.length()) {
                throw resource.failure("invalid Link metadata");
            }
        }
        return result;
    }

    private static boolean hasNext(String relation) {
        for (String entry : relation.toLowerCase(Locale.ROOT).split("[ \\t]+")) {
            if ("next".equals(entry)) {
                return true;
            }
        }
        return false;
    }

    private static int validate(
            String link, HttpUrl request, int current, CapsuleCrmResource resource)
            throws CapsuleCrmReadException {
        HttpUrl next = request.resolve(link);
        if (next == null
                || !request.scheme().equals(next.scheme())
                || !request.host().equals(next.host())
                || request.port() != next.port()
                || !request.encodedPath().equals(next.encodedPath())
                || !next.username().isEmpty()
                || !next.password().isEmpty()
                || next.fragment() != null) {
            throw resource.failure("next-page link leaves the requested API scope");
        }
        for (String key : next.queryParameterNames()) {
            List<String> values = next.queryParameterValues(key);
            if (values.size() != 1
                    || (!"page".equals(key)
                            && (!request.queryParameterNames().contains(key)
                                    || !Objects.equals(
                                            values.get(0), request.queryParameter(key))))) {
                throw resource.failure("next-page link changes the selected filters");
            }
        }
        int expected = current + 1;
        if (!Integer.toString(expected).equals(next.queryParameter("page"))) {
            throw resource.failure("next-page link does not advance by one page");
        }
        return expected;
    }
}
