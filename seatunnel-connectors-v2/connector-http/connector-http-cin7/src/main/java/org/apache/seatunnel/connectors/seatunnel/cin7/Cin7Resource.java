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
package org.apache.seatunnel.connectors.seatunnel.cin7;

/**
 * Defines the V2 read contracts for the supported Cin7 Core collections and details.
 *
 * <p>Declaration order is also scan order. Each detail is associated with one summary collection.
 */
enum Cin7Resource {
    BANK_ACCOUNTS("bank_accounts", "ref/accountBank", "BankAccountsList", "AccountID"),
    ATTRIBUTE_SETS("attribute_sets", "ref/attributeset", "AttributeSetList", "ID"),
    ACCOUNTS("accounts", "ref/account", "AccountsList", "Code"),
    BRANDS("brands", "ref/brand", "BrandList", "ID"),
    CARRIERS("carriers", "ref/carrier", "CarrierList", "CarrierID"),
    CUSTOMERS(
            "customers",
            "customer",
            "CustomerList",
            "ID",
            "&IncludeDeprecated=true&IncludeProductPrices=true"),
    DEALS("deals", "reference/deals", "Deals", "ID"),
    LOCATIONS("locations", "ref/location", "LocationList", "ID"),
    PRODUCTS(
            "products",
            "product",
            "Products",
            "ID",
            "&IncludeBOM=true&IncludeMovements=true&IncludeSuppliers=true&IncludeDeprecated=true"
                    + "&IncludeAttachments=true&IncludeCustomPrices=true&IncludeReorderLevels=true"),
    PURCHASE_LIST("purchase_list", "purchaseList", "PurchaseList", "ID"),
    PURCHASES("purchases", "advanced-purchase", null, "ID", "&CombineAdditionalCharges=true"),
    SUPPLIERS("suppliers", "supplier", "SupplierList", "ID", "&IncludeDeprecated=true"),
    OPPORTUNITIES("opportunities", "crm/opportunity", "opportunityList", "ID"),
    SALE_LIST("sale_list", "saleList", "SaleList", "SaleID"),
    SALES("sales", "sale", null, "ID", "&IncludeTransactions=true&CombineAdditionalCharges=true"),
    TAXES("taxes", "ref/tax", "TaxRuleList", "ID"),
    PRODUCT_CATEGORIES("product_categories", "ref/category", "CategoryList", "ID"),
    PRODUCT_FAMILY("product_family", "productFamily", "ProductFamilies", "ID"),
    LEADS("leads", "crm/lead", "LeadList", "ID"),
    PRODUCT_AVAILABILITY(
            "product_availability", "ref/productavailability", "ProductAvailabilityList", "ID");

    static final String BASE_PATH = "/ExternalApi/v2/";

    final String name;
    final String route;
    final String array;
    final String id;
    final String options;

    Cin7Resource(String name, String route, String array, String id) {
        this(name, route, array, id, "");
    }

    Cin7Resource(String name, String route, String array, String id, String options) {
        this.name = name;
        this.route = route;
        this.array = array;
        this.id = id;
        this.options = options;
    }

    Cin7Resource detail() {
        if (this == PURCHASE_LIST) {
            return PURCHASES;
        }
        return this == SALE_LIST ? SALES : null;
    }

    int pageLimit(int configured) {
        return this == DEALS ? Math.min(configured, 25) : configured;
    }

    Failure failure(String reason) {
        return new Failure(name, reason);
    }

    /**
     * Reports a resource and a local diagnostic without including a URL, header or response body.
     *
     * <p>Transport and parsing callers use only fixed failure descriptions.
     */
    static final class Failure extends RuntimeException {
        private static final long serialVersionUID = 1L;

        Failure(String resource, String reason) {
            super("Cin7 Core " + resource + ": " + reason);
        }
    }
}
