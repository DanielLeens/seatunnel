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

# Chift

## Description

The Chift source reads consumers, connections, sync definitions and sync executions for one Chift account. It uses credentials from the same account for a JSON token exchange and authenticated HTTPS reads.

## Resources

| Resource | GET path | Scope |
| --- | --- | --- |
| `consumers` | `/consumers` | Consumers visible in the account |
| `connections` | `/consumers/{consumer_id}/connections` | Active and inactive connections for each discovered consumer |
| `syncs` | `/syncs` | Syncs configured in the account |
| `executions` | `/syncs/{syncid}/executions` | Execution records for each discovered sync |

The connector discovers consumers and syncs before reading their children, even when parent rows are absent from the output. It uses Chift's default execution visibility, which excludes previews. An access failure for a requested parent stops the scan and reports the resource and HTTP status.

## Options

| Option | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `auth_client_id` | string | Yes | | Chift API client ID. |
| `auth_client_secret` | string | Yes | | Chift API client secret. |
| `auth_account_id` | string | Yes | | Account UUID linked to the API credentials. |
| `resources` | list of strings | Yes | | One or more distinct resource names from the table. |
| `start_date` | string | No | | Inclusive execution `start` lower bound as an ISO-8601 timestamp with offset. |
| `end_date` | string | No | | Exclusive execution `start` upper bound. With only a start date, the upper bound is frozen at scan start. |
| `legacy_timezone` | string | No | | Explicit IANA zone or offset for execution timestamps that the provider sends without an offset. |
| `page_size` | integer | No | `50` | Requested execution page size. Range 1–500. |
| `max_parents` | integer | No | `10000` | Maximum retained consumers or syncs in a parent family. Range 1–50000. |
| `max_requests` | integer | No | `100000` | Maximum successfully processed data responses per scan. Range 1–1000000. |
| `max_attempts` | integer | No | `4` | Maximum attempts for one GET or token exchange. Range 1–10. |
| `request_timeout_ms` | integer | No | `30000` | Total deadline for one data response, including token exchange and retries. Range 100–300000. |
| `max_response_bytes` | integer | No | `8388608` | Decoded byte limit for a data response. Range 1024–67108864. |
| `endpoint` | string | No | `https://api.chift.eu` | Optional bare HTTPS origin. Requires `trust_certificate`. |
| `trust_certificate` | string | No | | Worker-local PEM trust certificate path for a custom origin. Hostname validation still applies. |

Execution bounds are evaluated against the returned `start` with exact timestamp comparison. Chift's list reference documents date query parameters without specifying which timestamp they compare or their boundary semantics. The connector pages through executions before applying the configured window. Offset-free execution records require an explicit `legacy_timezone`. Local timestamps that cannot be uniquely interpreted in that zone fail the scan.

Chift's JSON `POST /token` returns a short-lived bearer token. The connector caches it until close to its reported expiration and renews it once after an eligible HTTP 401. The token and API credentials stay out of checkpoint state. A bounded retry honors `Retry-After` when present. A token request with an uncertain transport result fails without reissuing that POST.

## Output and recovery

| Column | Type | Description |
| --- | --- | --- |
| `resource` | string | Selected resource. |
| `record_id` | string | Provider identifier, preserving its original value. |
| `parent_id` | nullable string | Owning consumer UUID for connections or sync UUID for executions. |
| `started_at` | nullable string | Exact execution start normalized to UTC. |
| `payload` | string | Complete returned record as JSON, including nested values and numeric precision. |

The source validates the reported page, size and total, and rejects duplicate IDs between adjacent execution pages. A checkpoint preserves the active parent, page and partially emitted offset and the frozen bound. Before emitting a remaining suffix, recovery verifies the active page contents and metadata. Fresh scans can observe different provider data. The API offers neither a common transactional view across pages nor deletion events.

## Example

```hocon
env {
  job.mode = "BATCH"
}

source {
  Chift {
    auth_client_id = ${CHIFT_CLIENT_ID}
    auth_client_secret = ${CHIFT_CLIENT_SECRET}
    auth_account_id = ${CHIFT_ACCOUNT_ID}
    resources = ["consumers", "connections", "syncs", "executions"]
    start_date = "2026-01-01T00:00:00Z"
    end_date = "2026-02-01T00:00:00Z"
    plugin_output = "chift"
  }
}

sink {
  Console {
    plugin_input = "chift"
  }
}
```

See [Chift authentication](https://docs.chift.eu/home/authentication.md), [execution list](https://docs.chift.eu/api-reference/endpoints/syncs/get-executions-for-a-sync.md), and [retry timing](https://docs.chift.eu/home/rate-limits.md).
