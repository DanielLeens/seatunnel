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

import ChangeLog from "../changelog/connector-http-claude.md";

# Claude Analytics

The `Claude` source reads Claude Enterprise engagement, adoption, usage and cost analytics in `BATCH` mode. Enable the Enterprise Analytics API in claude.ai and create an Analytics API key with the `read:analytics` scope. Set it in `token`. The connector sends `x-api-key` and `anthropic-version: 2026-10-21` to `api.anthropic.com`. Engagement data is available with an Enterprise plan. Usage and cost reflect the organization's plan and usage credits. [Claude Enterprise Analytics API](https://platform.claude.com/docs/en/manage-claude/analytics-api).

## Resources

All paths are relative to `/v1/organizations/analytics/`.

| Resource | GET path | Request slice | Native identity |
| --- | --- | --- | --- |
| `users` | `users` | UTC day | `user.id` |
| `chat_projects` | `apps/chat/projects` | UTC day | `project_id` |
| `skills` | `skills` | UTC day | `skill_name` |
| `connectors` | `connectors` | UTC day | `connector_name` |
| `summaries` | `summaries` | UTC day | Null |
| `user_usage_report` | `user_usage_report` | UTC day | `actor.user_id` |
| `user_cost_report` | `user_cost_report` | UTC day | `actor.user_id` |
| `usage_report` | `usage_report` | Up to 7 UTC days | Null |
| `cost_report` | `cost_report` | Up to 7 UTC days | Null |

The four report resources use the supported Enterprise report endpoints. Per-user metrics group by product and model, and cost reports also group by cost type. Each organization-wide bucket retains its complete `results` array, including empty buckets. The API caps each legacy organization bucket at its top 100 groups. Amounts stay in the provider's original representation and units. [Usage and cost report behavior](https://platform.claude.com/docs/en/manage-claude/usage-cost-analytics-api#migrate-from-the-claude-enterprise-usage-and-cost-reports).

## Date window

Both date options select inclusive UTC days before today. When omitted, the start is 7 days before the scan begins, with an earliest date of 2026-01-01. The last default day for engagement and summaries is 3 days before scanning. The report defaults include yesterday. Report dates must also be within the provider's 365-day lookback.

The source uses exact exclusive UTC boundaries for the underlying report requests and freezes its date plan for recovery. Availability depends on when the provider refreshes the data. A rejected or unavailable date is reported as a read error.

## Output

| Column | Type | Description |
| --- | --- | --- |
| `resource` | STRING | Resource name. |
| `id` | STRING | Native user, project, skill, or connector identity when present. Null for aggregate rows. |
| `organization_id` | STRING | Organization ID returned by a report. Null on engagement rows. |
| `window_start` | STRING | Start of the requested slice, inclusive UTC midnight. |
| `window_end` | STRING | End of the requested slice, exclusive UTC midnight. |
| `data_refreshed_at` | STRING | Report freshness watermark as supplied by the API, when available. |
| `record` | STRING | Native JSON object, including group results, numeric values and nulls. |

Several grouped rows can share the same native identity. Consult each row's dimensions and date fields in `record` when building keys downstream.

## Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `token` | String | Yes |  | Enterprise Analytics API key with the `read:analytics` scope. |
| `resources` | List of strings | No | `[]` | Names from the resource table. An empty list selects all nine. |
| `start_date` | String | No |  | Inclusive first UTC day, `YYYY-MM-DD`. Defaults to 7 days before scan start. |
| `end_date` | String | No |  | Inclusive last UTC day, `YYYY-MM-DD`. See the date window section for defaults. |
| `page_size` | Integer | No | `1000` | Requested page size from 1 to 1000. Organization-wide bucket reports use at most 31. |
| `max_records_per_response` | Integer | No | `10000` | Maximum records in one response, from 1 to 100000. |
| `max_pages_per_collection` | Integer | No | `10000` | Maximum pages per resource and date slice, from 1 to 100000. |
| `max_pages_per_scan` | Integer | No | `100000` | Maximum accepted responses per scan, from 1 to 1000000. |
| `tls_trust_certificate` | String | No |  | Optional PEM or base64 DER X.509 trust anchor for this source. HTTPS hostname verification remains enabled. |
| `connect_timeout_ms` | Integer | No | `10000` | TCP connection timeout from 100 to 300000 ms. |
| `request_timeout_ms` | Integer | No | `30000` | Deadline per HTTPS attempt from 100 to 300000 ms. |
| `operation_timeout_ms` | Integer | No | `120000` | Deadline across retries and delays for one read. At least `request_timeout_ms` and at most 600000 ms. |
| `request_interval_ms` | Integer | No | `1000` | Minimum time between HTTPS attempts, from 0 to 60000 ms. |
| `max_retries` | Integer | No | `3` | Extra attempts after eligible transient failures, from 0 to 8. |
| `max_retry_delay_ms` | Integer | No | `60000` | Maximum retry delay from 250 to 600000 ms. |
| `max_response_bytes` | Integer | No | `8388608` | Maximum wire and decoded response bytes, from 256 to 16777216. |

The application uses bounded retries for throttled or otherwise transient requests. The service shares its analytics rate limit across the organization.

## Recovery

One reader traverses the requested resources and date slices. A checkpoint retains the cursor, the date plan and the row position within the response. Restore with the same key, resources, date options and page size. The reader verifies the saved response before publishing its remaining rows. The checkpoint captures a scan position for a mutable upstream dataset.

A refresh can expire a provider cursor and return HTTP 410. Run the affected date range as a fresh scan if the provider has invalidated the cursor. Empty reports do not create rows for their watermark.

## Example

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  Claude {
    token = "${CLAUDE_ANALYTICS_KEY}"
    resources = ["users", "summaries", "usage_report"]
  }
}
sink {
  Console {}
}
```

<ChangeLog />
