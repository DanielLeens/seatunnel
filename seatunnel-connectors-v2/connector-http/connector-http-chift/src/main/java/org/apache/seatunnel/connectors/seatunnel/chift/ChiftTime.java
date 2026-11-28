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
package org.apache.seatunnel.connectors.seatunnel.chift;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Reads the provider's exact execution start instant.
 *
 * <p>Legacy values without a UTC offset require an explicitly configured time zone and must map to
 * one instant. The original provider value is retained independently in the payload.
 */
final class ChiftTime {
    private static final Pattern COMPACT_OFFSET = Pattern.compile("([+-][0-9]{2})([0-9]{2})$");

    private ChiftTime() {}

    static Instant start(JsonNode field, ZoneId fallback) throws ChiftReadException {
        ChiftResource operation = ChiftResource.EXECUTIONS;
        if (!field.isTextual() || field.textValue().length() > 128) {
            throw operation.failure("expected an execution start timestamp");
        }
        String text = field.textValue();
        if (text.length() > 10 && text.charAt(10) == ' ') {
            text = text.substring(0, 10) + 'T' + text.substring(11);
        }
        text = COMPACT_OFFSET.matcher(text).replaceFirst("$1:$2");
        try {
            return OffsetDateTime.parse(text).toInstant();
        } catch (DateTimeParseException noOffset) {
            if (fallback == null) {
                throw operation.failure("expected an offset timestamp or explicit legacy_timezone");
            }
        }
        try {
            LocalDateTime local = LocalDateTime.parse(text);
            List<ZoneOffset> offsets = fallback.getRules().getValidOffsets(local);
            if (offsets.size() != 1) {
                throw operation.failure("execution start is ambiguous in legacy_timezone");
            }
            return local.toInstant(offsets.get(0));
        } catch (DateTimeException invalid) {
            throw operation.failure("invalid execution start timestamp");
        }
    }
}
