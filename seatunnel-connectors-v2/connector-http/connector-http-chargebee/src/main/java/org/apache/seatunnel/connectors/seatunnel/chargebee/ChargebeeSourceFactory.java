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

package org.apache.seatunnel.connectors.seatunnel.chargebee;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;

import com.google.auto.service.AutoService;

import java.io.Serializable;

import static org.apache.seatunnel.connectors.seatunnel.chargebee.ChargebeeSourceOptions.CONNECT_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.chargebee.ChargebeeSourceOptions.END_DATE;
import static org.apache.seatunnel.connectors.seatunnel.chargebee.ChargebeeSourceOptions.INCREMENTAL_START;
import static org.apache.seatunnel.connectors.seatunnel.chargebee.ChargebeeSourceOptions.LOOKBACK_SECONDS;
import static org.apache.seatunnel.connectors.seatunnel.chargebee.ChargebeeSourceOptions.MAX_ATTEMPTS;
import static org.apache.seatunnel.connectors.seatunnel.chargebee.ChargebeeSourceOptions.MAX_PAGES;
import static org.apache.seatunnel.connectors.seatunnel.chargebee.ChargebeeSourceOptions.MAX_RESPONSE_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.chargebee.ChargebeeSourceOptions.PAGE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.chargebee.ChargebeeSourceOptions.PRODUCT_CATALOG;
import static org.apache.seatunnel.connectors.seatunnel.chargebee.ChargebeeSourceOptions.READ_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.chargebee.ChargebeeSourceOptions.REQUEST_DEADLINE_MS;
import static org.apache.seatunnel.connectors.seatunnel.chargebee.ChargebeeSourceOptions.RESOURCE;
import static org.apache.seatunnel.connectors.seatunnel.chargebee.ChargebeeSourceOptions.SITE;
import static org.apache.seatunnel.connectors.seatunnel.chargebee.ChargebeeSourceOptions.START_DATE;
import static org.apache.seatunnel.connectors.seatunnel.chargebee.ChargebeeSourceOptions.TLS_TRUST_CERTIFICATE;
import static org.apache.seatunnel.connectors.seatunnel.chargebee.ChargebeeSourceOptions.TOKEN;

/**
 * Registers the bounded Chargebee V2 table source.
 *
 * <p>The option rule and distribution mapping use the same public source identity.
 */
@AutoService(Factory.class)
public final class ChargebeeSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return ChargebeeSource.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(SITE, TOKEN, RESOURCE, START_DATE)
                .optional(
                        PRODUCT_CATALOG,
                        END_DATE,
                        INCREMENTAL_START,
                        LOOKBACK_SECONDS,
                        PAGE_SIZE,
                        MAX_PAGES,
                        CONNECT_TIMEOUT_MS,
                        READ_TIMEOUT_MS,
                        REQUEST_DEADLINE_MS,
                        MAX_ATTEMPTS,
                        MAX_RESPONSE_BYTES,
                        TLS_TRUST_CERTIFICATE)
                .build();
    }

    /**
     * Validates the site, catalog, authentication and read bounds before any network work can
     * start.
     *
     * @param context the engine's connector factory context
     * @return the selected bounded resource
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        ChargebeeConfig config = new ChargebeeConfig(context.getOptions());
        return () -> (SeaTunnelSource<T, SplitT, StateT>) new ChargebeeSource(config);
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return ChargebeeSource.class;
    }
}
