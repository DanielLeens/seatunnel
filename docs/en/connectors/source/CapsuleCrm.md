<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# CapsuleCrm

## Description

The Capsule CRM source reads account data through the Capsule API v2 using an existing personal access token or a pre-issued OAuth access token. It sends authenticated HTTPS GET requests and preserves each complete returned record. Build a bounded scan by selecting one or more resources and optionally setting an opportunity update window.

## Resources

All paths are relative to `https://api.capsulecrm.com/api/v2`.

| Resource | Route | Scope |
| --- | --- | --- |
| `users` | `/users` | Account users |
| `parties` | `/parties` | People and organisations |
| `tasks` | `/tasks` | Account tasks |
| `employees` | `/parties/{id}/people` | People linked to each discovered organisation |
| `projects` | `/kases` | Projects use the API's historical route name |
| `opportunities` | `/opportunities` | Opportunities, with an optional `updatedAt` window |
| `pipelines` | `/pipelines` | Opportunity pipelines |
| `milestones` | `/milestones` | Opportunity milestones |
| `site` | `/site` | One object describing the token's account |
| `tags` | `/{entity}/tags` | Tag definitions for the selected entity |
| `custom_fields` | `/{entity}/fields/definitions` | Field definitions for the selected entity |
| `lost_reasons` | `/lostreasons` | Opportunity loss reasons |
| `board` | `/boards` | Project boards |
| `categories` | `/categories` | Task categories |
| `activity_types` | `/activitytypes` | Activity type definitions |
| `stages` | `/stages` | Account-level project stages, with board metadata in the returned records |

Selecting `employees` scans all organisation pages visible to the token, including when `parties` is absent from the output. The source preserves Capsule's default visibility and status filters. Archived or otherwise excluded records remain subject to those API defaults.

## Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `token` | string | Yes | | Capsule bearer token without the `Bearer` prefix. |
| `resources` | list of strings | Yes | | One or more distinct resource names from the table. |
| `entity` | string | No | `parties` | Entity for tags and custom fields. Accepts `parties`, `opportunities`, or `kases`. |
| `start_date` | string | No | | Inclusive opportunity `updatedAt` lower bound, in ISO-8601 with a UTC offset. |
| `end_date` | string | No | | Exclusive opportunity `updatedAt` upper bound, in ISO-8601 with a UTC offset. When only `start_date` is set, the source captures the upper bound at the initial scan. |
| `page_size` | integer | No | `100` | Requested rows per page for resources that document page size support. Range 1–100. |
| `max_parents` | integer | No | `10000` | Maximum organisations kept for employee traversal. Range 1–50000. |
| `max_requests` | integer | No | `100000` | Maximum successful logical page responses per scan, including discovery. Range 1–1000000. |
| `max_attempts` | integer | No | `4` | Attempts per page, including the first GET. Range 1–10. |
| `request_timeout_ms` | integer | No | `30000` | Total time per page, including its reads, retries and backoff. Range 100–3700000. |
| `max_response_bytes` | integer | No | `8388608` | Maximum decoded response bytes. Range 1024–67108864. |
| `endpoint` | string | No | `https://api.capsulecrm.com` | Custom bare HTTPS origin. When set, also provide `trust_certificate`. |
| `trust_certificate` | string | No | | Worker-local path to a PEM trust certificate for the custom origin. Hostname validation is still applied. |

Date options apply to `opportunities` only. An unset lower bound scans from the earliest opportunity visible to the token. A configured lower bound is sent with a one-second overlap because Capsule's `since` is exclusive. The source then enforces the exact requested boundaries locally, including fractional seconds and equal timestamps. On HTTP 429 it waits until the advertised `X-RateLimit-Reset` epoch second and any later `Retry-After` bound when the retry fits the page deadline.

## Output and recovery

| Column | Type | Description |
| --- | --- | --- |
| `resource` | string | Selected source resource. |
| `record_id` | string | Provider ID, or the account subdomain for `site`. |
| `parent_id` | nullable string | Organisation ID for an employee. Entity name for a tag or custom-field definition. |
| `updated_at` | nullable string | Original `updatedAt` text when returned. |
| `payload` | string | Complete record encoded as JSON, retaining nested data and JSON number precision. |

The source uses one bounded scan. Page continuation follows validated Capsule `Link` metadata within the same route and filters, including empty pages that provide a next link. Checkpoint recovery retains the page, organisation traversal, frozen window, and emitted offset. It fails if the active partially emitted page or its continuation changed before replay. Restore with the same read scope. Each fresh scan uses its configured dates. Capsule pages can change during a scan, so cross-page duplicates can occur and the scan has no transaction isolation or delete events.

## Example

```hocon
env {
  job.mode = "BATCH"
}

source {
  CapsuleCrm {
    token = ${CAPSULE_CRM_TOKEN}
    resources = ["opportunities", "employees", "tags"]
    entity = "opportunities"
    start_date = "2026-01-01T00:00:00Z"
    end_date = "2026-02-01T00:00:00Z"
    plugin_output = "capsule"
  }
}

sink {
  Console {
    plugin_input = "capsule"
  }
}
```

See [Capsule API reading and pagination](https://developer.capsulecrm.com/v2/overview/reading-from-the-api), [the opportunity endpoint](https://developer.capsulecrm.com/v2/operations/Opportunity), and [Capsule rate limits](https://developer.capsulecrm.com/v2/overview/handling-api-responses).
