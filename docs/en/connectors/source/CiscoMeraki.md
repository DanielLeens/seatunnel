<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

import ChangeLog from "../changelog/connector-http-cisco-meraki.md";

# Cisco Meraki

The `CiscoMeraki` source reads Dashboard and Secure Connect records in `BATCH` mode from the global `https://api.meraki.com` origin. Set `token` to a Cisco Meraki Dashboard API key with permission for the selected organizations and operations. Dashboard receives `Authorization: Bearer`; Secure Connect data centers use `X-Cisco-Meraki-API-Key`. Secure Connect organization access depends on the corresponding product entitlement. [Cisco Meraki authentication](https://developer.cisco.com/meraki/api-v1/authorization/).

## Resources

| Resource | GET path | `id` |
| --- | --- | --- |
| `organizations` | `/api/v1/organizations` | `id` |
| `datacenters` | `/api/secureConnect/v1/organizations/{organizationId}/datacenters` | Null |
| `organization_networks` | `/api/v1/organizations/{organizationId}/networks` | `id` |
| `organization_network_settings` | `/api/v1/networks/{networkId}/settings` | Null |
| `organization_devices` | `/api/v1/organizations/{organizationId}/wirelessController/overview/byDevice` | Null |
| `organization_apiRequests` | `/api/v1/organizations/{organizationId}/apiRequests` | Null |
| `organization_admins` | `/api/v1/organizations/{organizationId}/admins` | `id` |
| `organization_saml` | `/api/v1/organizations/{organizationId}/saml` | Null |

Selecting a child reads its parent organizations internally. Selecting network settings also discovers their networks. Only selected resources are emitted. The wireless controller resource emits one native overview object per API page, including its `items` and metadata. Secure Connect data centers, SAML and network settings retain their full native objects.

Paginated endpoints follow the provider's `Link` continuation, including on short or empty list pages. Continuations must stay on the configured operation and global origin and preserve page size and time filters. The source treats redirects as request failures and supports the global API origin. [Cisco Meraki API origin guidance](https://documentation.meraki.com/Platform_Management/Dashboard_Administration/Troubleshooting_and_Support/Troubleshooting/Meraki_Dashboard_URL_Update_-_PCI_Compliance).

## Time window

API request logs send native `t0` and `t1` and retain records whose `ts` is within the inclusive UTC window. By default the window covers the 30 days before the scan starts. The provider exposes up to 31 days of request history. Date options also filter administrators by `lastActive` locally. Administrators without an activity time are included in unfiltered reads. Other resources and parent discovery are unaffected. [API request log reference](https://developer.cisco.com/meraki/api-v1/get-organization-api-requests/).

## Output

| Column | Type | Description |
| --- | --- | --- |
| `resource` | STRING | Resource name |
| `id` | STRING | Native text identifier when the response has one. |
| `organization_id` | STRING | Organization scope, including the organization itself on organization rows. |
| `network_id` | STRING | Network ID on network rows and settings, null for organization-scoped rows. |
| `record` | STRING | Native JSON object, including unknown fields, nulls and numeric precision. |

## Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `token` | String | Yes |  | Meraki Dashboard API key. |
| `resources` | List of strings | No | `[]` | The resources in the table. An empty list reads all eight. |
| `start_date` | String | No |  | Inclusive ISO 8601 start for request logs or administrator activity. |
| `end_date` | String | No |  | Inclusive ISO 8601 end for request logs or administrator activity. A missing value uses the scan start time. |
| `tls_trust_certificate` | String | No |  | Optional source-local PEM or base64 DER X.509 trust anchor. HTTPS hostname verification remains enabled. |
| `page_size` | Integer | No | `100` | Records per paginated request, from 3 to 1000. |
| `max_unpaginated_items` | Integer | No | `10000` | Maximum administrators in one response, from 1 to 100000. |
| `max_pages_per_collection` | Integer | No | `10000` | Maximum pages for a collection or parent, from 1 to 100000. |
| `max_pages_per_scan` | Integer | No | `100000` | Maximum accepted responses in the scan, from 1 to 1000000. |
| `connect_timeout_ms` | Integer | No | `10000` | TCP connect timeout, from 100 to 300000 ms. |
| `request_timeout_ms` | Integer | No | `30000` | Deadline per HTTPS attempt, from 100 to 300000 ms. |
| `operation_timeout_ms` | Integer | No | `120000` | Deadline across all retries and waits in one read. At least `request_timeout_ms` and at most 600000 ms. |
| `request_interval_ms` | Integer | No | `100` | Minimum spacing between HTTPS attempts, from 0 to 60000 ms. |
| `max_retries` | Integer | No | `3` | Additional transient attempts, from 0 to 8. |
| `max_retry_delay_ms` | Integer | No | `60000` | Maximum retry wait, from 250 to 600000 ms. |
| `max_response_bytes` | Integer | No | `8388608` | Maximum wire and decompressed bytes, from 256 to 16777216. |

## Recovery

One reader advances through the organizations and their selected child operations. A checkpoint pins the date window and stores pagination and row positions. On restore, the reader verifies the saved organization, network and active response before continuing. Mutations can invalidate a saved response. The snapshot excludes raw credentials and records. Use the same API key, resources, dates and page size to restore. A saved request log window can age out of the provider's 31-day history.

## Example

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  CiscoMeraki {
    token = "${MERAKI_API_KEY}"
    resources = ["organizations", "organization_networks", "organization_network_settings"]
  }
}
sink {
  Console {}
}
```

<ChangeLog />
