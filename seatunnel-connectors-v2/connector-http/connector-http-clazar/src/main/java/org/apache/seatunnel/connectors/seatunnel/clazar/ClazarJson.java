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
package org.apache.seatunnel.connectors.seatunnel.clazar;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Strict precision-preserving encoding for Clazar request and response objects.
 *
 * <p>Failures carry only the operation name because provider responses can contain sensitive data.
 */
final class ClazarJson {
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

    private ClazarJson() {}

    static JsonNode object(byte[] response, String operation) throws ClazarReadException {
        try {
            String decoded =
                    StandardCharsets.UTF_8
                            .newDecoder()
                            .decode(ByteBuffer.wrap(response))
                            .toString();
            JsonNode root = JSON.readTree(decoded);
            if (root != null && root.isObject()) {
                return root;
            }
        } catch (IOException | RuntimeException invalid) {
            throw new ClazarReadException(operation, "invalid UTF-8 JSON response");
        }
        throw new ClazarReadException(operation, "expected a JSON response object");
    }

    static String record(JsonNode value, ClazarResource resource) throws ClazarReadException {
        try {
            return JSON.writeValueAsString(value);
        } catch (IOException | RuntimeException invalid) {
            throw resource.failure("could not preserve the provider record");
        }
    }

    static String credentials(String clientId, String clientSecret) throws ClazarReadException {
        try {
            return JSON.writeValueAsString(
                    Map.of("client_id", clientId, "client_secret", clientSecret));
        } catch (IOException | RuntimeException invalid) {
            throw new ClazarReadException("authentication", "could not encode client credentials");
        }
    }
}
