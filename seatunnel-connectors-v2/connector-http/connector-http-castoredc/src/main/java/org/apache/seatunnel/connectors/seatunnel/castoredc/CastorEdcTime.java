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
package org.apache.seatunnel.connectors.seatunnel.castoredc;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.List;

/**
 * Interprets the ISO and serialized PHP time values returned by Castor.
 *
 * <p>Ambiguous local time during an offset transition fails instead of changing the selected
 * interval. The unmodified provider value remains part of the payload.
 */
final class CastorEdcTime {
    private CastorEdcTime() {}

    static Instant time(JsonNode record, CastorEdcResource resource, ZoneId fallback)
            throws CastorEdcReadException {
        JsonNode value;
        switch (resource) {
            case USER:
            case STUDY_USER:
                value = record.path("last_login");
                break;
            case STUDY:
                value = record.path("created_on");
                break;
            case AUDIT_TRIAL:
                value = record.path("datetime");
                break;
            default:
                return null;
        }
        if (value.isNull() || value.isMissingNode()) {
            return null;
        }
        JsonNode instant = value.isObject() ? value.path("date") : value;
        if (value.isObject() && instant.isNull()) {
            return null;
        }
        if (!instant.isTextual() || instant.textValue().length() > 128) {
            throw resource.failure("expected a provider timestamp string or PHP DateTime object");
        }
        JsonNode timezone = value.isObject() ? value.path("timezone") : null;
        String text = instant.textValue().replace(' ', 'T');
        try {
            return OffsetDateTime.parse(text).toInstant();
        } catch (DateTimeParseException absentOffset) {
            // Parse an offset-free value only after considering an explicit offset.
        }
        try {
            LocalDateTime local = LocalDateTime.parse(text);
            ZoneId zone =
                    timezone == null || timezone.isMissingNode() || timezone.isNull()
                            ? fallback
                            : ZoneId.of(timezone.asText(""));
            List<ZoneOffset> offsets = zone.getRules().getValidOffsets(local);
            if (offsets.size() != 1) {
                throw resource.failure("provider local timestamp is ambiguous in its time zone");
            }
            return local.toInstant(offsets.get(0));
        } catch (DateTimeException invalid) {
            throw resource.failure("invalid provider timestamp or time zone");
        }
    }

    static String day(Instant instant, long overlap) {
        return instant.atOffset(ZoneOffset.UTC).toLocalDate().plusDays(overlap).toString();
    }
}
