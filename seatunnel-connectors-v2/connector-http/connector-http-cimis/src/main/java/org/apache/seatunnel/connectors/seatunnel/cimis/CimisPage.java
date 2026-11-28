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
package org.apache.seatunnel.connectors.seatunnel.cimis;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Decodes one current CIMIS document and validates the identity of every returned row.
 *
 * <p>Rows preserve the provider's calendar values and raw measurement content. Progress can advance
 * only after the whole slice has been published.
 */
final class CimisPage {
    private static final int MAX_REFERENCE_ROWS = 50000;
    private static final int MAX_DATA_ROWS = 6000;
    private static final Pattern HOUR = Pattern.compile("(?:(?:[01][0-9]|2[0-3])[0-5][0-9]|2400)");
    private static final Pattern ZIP = Pattern.compile("[0-9]{5}");
    private static final Pattern STATION = Pattern.compile("[0-9]{1,9}");

    private static final ObjectMapper JSON =
            new ObjectMapper(
                            JsonFactory.builder()
                                    .streamReadConstraints(
                                            StreamReadConstraints.builder()
                                                    .maxNestingDepth(80)
                                                    .maxNumberLength(256)
                                                    .build())
                                    .build())
                    .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));

    final List<SeaTunnelRow> rows;
    final CimisSplit next;
    final String fingerprint;

    private CimisPage(List<SeaTunnelRow> rows, CimisSplit next, String fingerprint) {
        this.rows = rows;
        this.next = next;
        this.fingerprint = fingerprint;
    }

    /**
     * Acquires and checks one selected reference document or weather slice.
     *
     * <p>Only the exact current volume rejection can subdivide an unpublished slice. Previously
     * emitted content must stay bound to its original scope.
     */
    static CimisPage read(
            CimisTransport transport,
            CimisConfig config,
            CimisSplit state,
            String table,
            long deadline)
            throws CimisReadException, InterruptedException {
        CimisResource resource = config.plan.get(state.resourceIndex);
        byte[] result;
        try {
            result = transport.get(resource, state, deadline);
        } catch (CimisReadException failure) {
            if (!failure.volumeLimit || state.published != 0) {
                throw failure;
            }
            long days = ChronoUnit.DAYS.between(state.from, state.through(config)) + 1;
            if (days <= 1) {
                throw resource.failure("single-day request exceeds CIMIS volume limit (ERR2112)");
            }
            return new CimisPage(List.of(), state.subdivide((int) (days / 2L)), null);
        }
        JsonNode root = document(result, resource);
        List<SeaTunnelRow> rows =
                resource == CimisResource.DATA
                        ? weather(root, config, state)
                        : references(root, resource);
        Set<String> ids = new HashSet<>();
        MessageDigest digest = CimisConfig.sha256();
        for (SeaTunnelRow row : rows) {
            String id = (String) row.getField(1);
            if (!ids.add(id)) {
                throw resource.failure("duplicate record identity in one CIMIS response");
            }
            hashField(digest, id);
            hashField(digest, (String) row.getField(8));
            row.setTableId(table);
        }
        String fingerprint =
                Base64.getUrlEncoder().withoutPadding().encodeToString(digest.digest());
        if (state.published < 0
                || state.published > rows.size()
                || state.published > 0 && !fingerprint.equals(state.fingerprint)) {
            throw resource.failure(
                    "provider slice changed since checkpoint or has an invalid row offset");
        }
        CimisTransport.checkDeadline(resource, deadline);
        return new CimisPage(rows, state.advance(config, resource), fingerprint);
    }

    private static JsonNode document(byte[] bytes, CimisResource resource)
            throws CimisReadException {
        try {
            String text =
                    StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes)).toString();
            JsonNode root = JSON.readTree(text);
            if (root != null
                    && root.isObject()
                    && !root.hasNonNull("error")
                    && !root.hasNonNull("Error")) {
                return root;
            }
        } catch (IOException | RuntimeException invalid) {
            // Parser errors can expose provider content.
        }
        throw resource.failure("expected a successful, valid UTF-8 JSON object");
    }

    private static List<SeaTunnelRow> references(JsonNode root, CimisResource resource)
            throws CimisReadException {
        JsonNode references = array(root.path(resource.envelope), MAX_REFERENCE_ROWS, resource);
        List<SeaTunnelRow> output = new ArrayList<>(references.size());
        for (JsonNode record : references) {
            if (!record.isObject()) {
                throw resource.failure("expected a CIMIS reference record object");
            }
            String id;
            if (resource == CimisResource.STATIONS) {
                id = station(record.path("StationNbr"), resource);
            } else {
                String zip = requiredText(record.path("ZipCode"), resource);
                if (!ZIP.matcher(zip).matches()) {
                    throw resource.failure("expected a five-digit ZIP string");
                }
                id =
                        resource == CimisResource.STATION_ZIPCODES
                                ? station(record.path("StationNbr"), resource) + ":" + zip
                                : zip;
            }
            output.add(
                    new SeaTunnelRow(
                            new Object[] {
                                resource.id,
                                id,
                                null,
                                null,
                                null,
                                null,
                                null,
                                null,
                                payload(record, resource)
                            }));
        }
        return output;
    }

    private static List<SeaTunnelRow> weather(JsonNode root, CimisConfig config, CimisSplit state)
            throws CimisReadException {
        CimisResource resource = CimisResource.DATA;
        JsonNode providers = array(root.path("Data").path("Providers"), 64, resource);
        List<SeaTunnelRow> output = new ArrayList<>();
        String target = config.targets.get(state.targetIndex);
        boolean hourly = config.hourlyModes.get(state.modeIndex);
        LocalDate through = state.through(config);
        for (JsonNode provider : providers) {
            String name = requiredText(provider.path("Name"), resource);
            String type = requiredText(provider.path("Type"), resource);
            String owner = requiredText(provider.path("Owner"), resource);
            boolean stationProvider = "station".equalsIgnoreCase(type);
            if (stationProvider && config.targetType.spatialOnly
                    || !stationProvider
                            && (!"spatial".equalsIgnoreCase(type)
                                    || config.targetType == CimisTarget.STATION_NUMBERS
                                    || hourly)) {
                throw resource.failure(
                        "provider type does not match the requested target or interval");
            }
            for (JsonNode record :
                    array(provider.path("Records"), MAX_DATA_ROWS - output.size(), resource)) {
                if (!record.isObject()) {
                    throw resource.failure("expected a CIMIS weather record object");
                }
                String date = requiredText(record.path("Date"), resource);
                verifyDate(date, state.from, through);
                String scope = requiredText(record.path("Scope"), resource);
                if (!(hourly ? "hourly" : "daily").equalsIgnoreCase(scope)) {
                    throw resource.failure("record scope does not match the requested interval");
                }
                String hour = optionalText(record.path("Hour"), resource);
                if (hourly && hour == null || hour != null && !HOUR.matcher(hour).matches()) {
                    throw resource.failure("expected an original hour from 0000 through 2400");
                }
                String stationId = stationProvider ? station(record.path("Station"), resource) : "";
                if (config.targetType == CimisTarget.STATION_NUMBERS && !target.equals(stationId)) {
                    throw resource.failure("record station does not match the requested target");
                }
                String zip = optionalText(record.path("ZipCodes"), resource);
                if (config.targetType == CimisTarget.ZIP_CODES
                        && zip != null
                        && !zip.isBlank()
                        && !matchesZip(zip, target)) {
                    throw resource.failure("record ZIP list does not include the requested target");
                }
                String location =
                        stationProvider
                                ? stationId
                                : String.join(
                                        "\n",
                                        optionalIdentity(record.path("Coordinate"), resource),
                                        optionalIdentity(record.path("Address"), resource),
                                        zip == null ? "" : zip);
                String id =
                        CimisConfig.digest(
                                String.join(
                                        "\n",
                                        config.targetType.id,
                                        target,
                                        name,
                                        type,
                                        owner,
                                        location,
                                        date,
                                        scope,
                                        hour == null ? "" : hour));
                output.add(
                        new SeaTunnelRow(
                                new Object[] {
                                    resource.id,
                                    id,
                                    target,
                                    date,
                                    hour,
                                    name,
                                    type,
                                    owner,
                                    payload(record, resource)
                                }));
            }
        }
        return output;
    }

    private static void verifyDate(String value, LocalDate from, LocalDate through)
            throws CimisReadException {
        try {
            LocalDate date = LocalDate.parse(value);
            if (!date.isBefore(from) && !date.isAfter(through)) {
                return;
            }
        } catch (DateTimeParseException invalid) {
            throw CimisResource.DATA.failure("expected an ISO CIMIS weather date");
        }
        throw CimisResource.DATA.failure("weather date lies outside the requested range");
    }

    private static boolean matchesZip(String values, String target) {
        for (String value : values.split(",")) {
            if (target.equals(value.trim())) {
                return true;
            }
        }
        return false;
    }

    private static JsonNode array(JsonNode node, int limit, CimisResource resource)
            throws CimisReadException {
        if (!node.isArray() || node.size() > limit) {
            throw resource.failure("expected the native CIMIS array within its response limit");
        }
        return node;
    }

    private static String station(JsonNode value, CimisResource resource)
            throws CimisReadException {
        String text = value.isTextual() || value.isIntegralNumber() ? value.asText() : "";
        if (!STATION.matcher(text).matches() || Integer.parseInt(text) <= 0) {
            throw resource.failure("expected a positive CIMIS station number");
        }
        return Integer.toString(Integer.parseInt(text));
    }

    private static String requiredText(JsonNode value, CimisResource resource)
            throws CimisReadException {
        String text = optionalText(value, resource);
        if (text == null || text.isBlank()) {
            throw resource.failure("missing required text in CIMIS record");
        }
        return text;
    }

    private static String optionalText(JsonNode value, CimisResource resource)
            throws CimisReadException {
        if (value.isMissingNode() || value.isNull()) {
            return null;
        }
        String text = value.textValue();
        if (text == null
                || text.length() > 8192
                || text.codePoints()
                        .anyMatch(
                                character ->
                                        Character.isISOControl(character)
                                                || character >= Character.MIN_SURROGATE
                                                        && character <= Character.MAX_SURROGATE)) {
            throw resource.failure("invalid CIMIS identity text");
        }
        return text;
    }

    private static String optionalIdentity(JsonNode value, CimisResource resource)
            throws CimisReadException {
        String text = optionalText(value, resource);
        return text == null ? "" : text;
    }

    private static String payload(JsonNode record, CimisResource resource)
            throws CimisReadException {
        try {
            return JSON.writeValueAsString(record);
        } catch (IOException | RuntimeException invalid) {
            throw resource.failure("could not encode the CIMIS record");
        }
    }

    private static void hashField(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        int length = bytes.length;
        digest.update((byte) (length >>> 24));
        digest.update((byte) (length >>> 16));
        digest.update((byte) (length >>> 8));
        digest.update((byte) length);
        digest.update(bytes);
    }
}
