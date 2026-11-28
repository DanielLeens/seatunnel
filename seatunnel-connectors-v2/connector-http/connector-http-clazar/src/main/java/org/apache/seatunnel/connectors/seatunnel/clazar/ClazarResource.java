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

import okhttp3.HttpUrl;

/**
 * Clazar entity and marketplace analytics read routes.
 *
 * <p>Entity paths end in a slash, while dataset paths identify the dataset without a trailing
 * slash.
 */
enum ClazarResource {
    BUYERS("buyers", true),
    LISTINGS("listings", true),
    CONTRACTS("contracts", true),
    OPPORTUNITIES("opportunities", true),
    PRIVATE_OFFERS("private_offers", true),
    AWS_COSELL_OPPORTUNITIES("aws_cosell_opportunities", false),
    AWS_DISBURSEMENTS("aws_marketplace_disbursements", false),
    AWS_REVENUE("aws_marketplace_revenue", false),
    AZURE_COSELL_OPPORTUNITIES("azure_cosell_opportunities", false),
    AZURE_CUSTOMERS("azure_marketplace_customers", false),
    AZURE_METERED_USAGE("azure_marketplace_metered_usage", false),
    AZURE_ORDERS("azure_marketplace_orders", false),
    AZURE_REVENUE("azure_marketplace_revenue", false),
    GCP_CHARGES_AND_USAGE("gcp_marketplace_charges_and_usage", false),
    GCP_DAILY_INSIGHTS("gcp_marketplace_daily_insights", false),
    GCP_DISBURSEMENTS("gcp_marketplace_disbursements", false),
    GCP_DISBURSEMENTS_SUMMARY("gcp_marketplace_disbursements_summary", false),
    GCP_INCREMENTAL_DAILY_INSIGHTS("gcp_marketplace_incremental_daily_insights", false),
    GCP_INCREMENTAL_MONTHLY_INSIGHTS("gcp_marketplace_incremental_monthly_insights", false),
    GCP_MONTHLY_INSIGHTS("gcp_marketplace_monthly_insights", false);

    final String id;
    final boolean entity;
    final String path;

    ClazarResource(String nativeName, boolean entity) {
        this.entity = entity;
        id = entity ? nativeName : "analytics_" + nativeName;
        path = entity ? "/" + nativeName + "/" : "/analytics/datasets/" + nativeName;
    }

    static ClazarResource from(String name) {
        for (ClazarResource resource : values()) {
            if (resource.id.equals(name)) {
                return resource;
            }
        }
        throw ClazarConfig.invalid("resources", "unknown resource");
    }

    HttpUrl target(HttpUrl origin, int page, int pageSize) {
        return origin.newBuilder()
                .encodedPath(path)
                .addQueryParameter("page", Integer.toString(page))
                .addQueryParameter("page_size", Integer.toString(pageSize))
                .build();
    }

    ClazarReadException failure(String reason) {
        return new ClazarReadException(id, reason);
    }
}
