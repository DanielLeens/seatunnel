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

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Binds each resource to a versioned API route and the exact supported list parameters.
 *
 * <p>Catalog eligibility is based on the provider's API v2 specifications. Hidden parent discovery
 * deliberately does not apply the leaf's date filter.
 */
enum ChargebeeResource {
    ITEM("/items", "item", "updated_at", Window.MONTHLY, "updated_at", false, 2, null),
    ADDON("/addons", "addon", "updated_at", Window.MONTHLY, null, true, 1, null),
    ATTACHED_ITEM(
            "/items/{id}/attached_items",
            "attached_item",
            "updated_at",
            Window.SINGLE,
            null,
            false,
            2,
            ITEM),
    CUSTOMER("/customers", "customer", "updated_at", Window.MONTHLY, "updated_at", true, 0, null),
    COUPON("/coupons", "coupon", "updated_at", Window.MONTHLY, null, false, 0, null),
    CONTACT("/customers/{id}/contacts", "contact", null, Window.NONE, null, false, 0, CUSTOMER),
    CREDIT_NOTE(
            "/credit_notes", "credit_note", "updated_at", Window.MONTHLY, "date", true, 0, null),
    EVENT("/events", "event", "occurred_at", Window.MONTHLY, "occurred_at", false, 0, null),
    GIFT("/gifts", "gift", "updated_at", Window.LOCAL, null, false, 0, null),
    ORDER("/orders", "order", "updated_at", Window.MONTHLY, "updated_at", true, 0, null),
    HOSTED_PAGE("/hosted_pages", "hosted_page", "updated_at", Window.MONTHLY, null, false, 0, null),
    INVOICE("/invoices", "invoice", "updated_at", Window.MONTHLY, "updated_at", true, 0, null),
    ITEM_PRICE(
            "/item_prices",
            "item_price",
            "updated_at",
            Window.MONTHLY,
            "updated_at",
            false,
            2,
            null),
    PLAN("/plans", "plan", "updated_at", Window.MONTHLY, null, true, 1, null),
    PAYMENT_SOURCE(
            "/payment_sources",
            "payment_source",
            "updated_at",
            Window.MONTHLY,
            "updated_at",
            true,
            0,
            null),
    PROMOTIONAL_CREDIT(
            "/promotional_credits",
            "promotional_credit",
            "created_at",
            Window.MONTHLY,
            null,
            false,
            0,
            null),
    SUBSCRIPTION(
            "/subscriptions",
            "subscription",
            "updated_at",
            Window.MONTHLY,
            "updated_at",
            true,
            0,
            null),
    SUBSCRIPTION_WITH_SCHEDULED_CHANGES(
            "/subscriptions/{id}/retrieve_with_scheduled_changes",
            "subscription",
            null,
            Window.NONE,
            null,
            false,
            0,
            SUBSCRIPTION),
    TRANSACTION(
            "/transactions",
            "transaction",
            "updated_at",
            Window.MONTHLY,
            "updated_at",
            true,
            0,
            null),
    UNBILLED_CHARGE(
            "/unbilled_charges",
            "unbilled_charge",
            "updated_at",
            Window.LOCAL,
            null,
            false,
            0,
            null),
    VIRTUAL_BANK_ACCOUNT(
            "/virtual_bank_accounts",
            "virtual_bank_account",
            "updated_at",
            Window.MONTHLY,
            null,
            false,
            0,
            null),
    QUOTE("/quotes", "quote", "updated_at", Window.MONTHLY, "date", true, 0, null),
    QUOTE_LINE_GROUP(
            "/quotes/{id}/quote_line_groups",
            "quote_line_group",
            null,
            Window.NONE,
            null,
            false,
            0,
            QUOTE),
    SITE_MIGRATION_DETAIL(
            "/site_migration_details",
            "site_migration_detail",
            "migrated_at",
            Window.LOCAL,
            null,
            false,
            0,
            null),
    COMMENT("/comments", "comment", "created_at", Window.MONTHLY, "created_at", false, 0, null),
    ITEM_FAMILY(
            "/item_families", "item_family", "updated_at", Window.MONTHLY, null, false, 2, null),
    DIFFERENTIAL_PRICE(
            "/differential_prices",
            "differential_price",
            "updated_at",
            Window.LOCAL,
            null,
            false,
            2,
            null);

    final String id = name().toLowerCase(Locale.ROOT);
    final String path;
    final String envelope;
    final String cursor;
    private final Window window;
    private final String ascendingSort;
    private final boolean includeDeleted;
    private final int catalog;
    final ChargebeeResource parent;

    ChargebeeResource(
            String path,
            String envelope,
            String cursor,
            Window window,
            String ascendingSort,
            boolean includeDeleted,
            int catalog,
            ChargebeeResource parent) {
        this.path = "/api/v2" + path;
        this.envelope = envelope;
        this.cursor = cursor;
        this.window = window;
        this.ascendingSort = ascendingSort;
        this.includeDeleted = includeDeleted;
        this.catalog = catalog;
        this.parent = parent;
    }

    static ChargebeeResource parse(String name, String productCatalog) {
        for (ChargebeeResource resource : values()) {
            if (resource.id.equals(name)) {
                if (resource.catalog != 0 && !productCatalog.equals(resource.catalog + ".0")) {
                    throw ChargebeeConfig.invalid(
                            "resource is unavailable for the selected product_catalog");
                }
                return resource;
            }
        }
        throw ChargebeeConfig.invalid("resource must be a documented Chargebee resource");
    }

    boolean singleton() {
        return this == SUBSCRIPTION_WITH_SCHEDULED_CHANGES;
    }

    String identityKey() {
        return this == SITE_MIGRATION_DETAIL ? "entity_id" : "id";
    }

    /**
     * Computes a closed UTC interval without losing equal-second records at a month boundary.
     *
     * <p>Adjacent months advance from this inclusive end by one second.
     */
    long windowEnd(long from, long ceiling) {
        if (window != Window.MONTHLY) {
            return ceiling;
        }
        long next =
                Instant.ofEpochSecond(from).atOffset(ZoneOffset.UTC).plusMonths(1).toEpochSecond();
        return Math.min(ceiling, next - 1);
    }

    /**
     * Builds fixed origin-form GET targets. The only cursor input is the opaque list offset.
     *
     * <p>The scheduled subscription lookup is a singleton resource without list parameters. Its
     * inventory uses the provider's has-scheduled-changes filter.
     */
    String target(
            ChargebeeConfig config,
            String parentId,
            String offset,
            long from,
            long through,
            boolean hidden) {
        String route = parentId == null ? path : path.replace("{id}", segment(parentId));
        if (singleton()) {
            return route;
        }
        Map<String, String> query = new LinkedHashMap<>();
        query.put("limit", Integer.toString(config.pageSize));
        if (offset != null) {
            query.put("offset", offset);
        }
        if (ascendingSort != null) {
            query.put("sort_by[asc]", ascendingSort);
        }
        if (includeDeleted) {
            query.put("include_deleted", "true");
        }
        if (hidden && config.resource == SUBSCRIPTION_WITH_SCHEDULED_CHANGES) {
            query.put("has_scheduled_changes[is]", "true");
        }
        if (!hidden && (window == Window.MONTHLY || window == Window.SINGLE)) {
            query.put(cursor + "[between]", "[" + from + "," + through + "]");
        }
        return route
                + "?"
                + query.entrySet().stream()
                        .map(entry -> form(entry.getKey()) + "=" + form(entry.getValue()))
                        .collect(Collectors.joining("&"));
    }

    private static String form(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private static String segment(String value) {
        return form(value).replace("+", "%20").replace("*", "%2A");
    }

    /**
     * Indicates how the provider can apply the selected record's date filter.
     *
     * <p>Some resources expose a cursor in records but no corresponding list query parameter.
     */
    private enum Window {
        NONE,
        LOCAL,
        SINGLE,
        MONTHLY
    }
}
