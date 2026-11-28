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

import ChangeLog from '../changelog/connector-http-circa.md';

# Circa

The Circa source reads events, contacts, teams, companies, contact relationships, and field definitions from the Circa event-management API at `app.circa.co`. A bounded job reads one resource with parallelism 1.

| Resource | Read behavior |
| --- | --- |
| `events` | Full list or an inclusive `updated_at` range. Includes undated events. |
| `contacts` | Full list or an inclusive `updated_at` range. |
| `teams` | Full unpaginated list. |
| `companies` | Full list or an inclusive `updated_at` range. |
| `company_contacts` | Contacts for every discovered company. |
| `event_fields` | Field definitions for events. |
| `contact_fields` | Definitions for contacts, including EventContact fields when returned by Circa. |
| `company_fields` | Field definitions for companies. |
| `event_contacts` | Contacts for every discovered event. |

The output has four string columns: `id`, `parent_id`, `cursor_timestamp`, and `data`. `parent_id` is populated for the two relationship resources. A contact can appear under several parent IDs. The three timestamp-filtered resources project `updated_at` as the UTC cursor when available. The `data` column is the full provider record as JSON and preserves original date and time fields, nested custom fields, explicit null values, and numeric precision.

| Option | Default | Description |
| --- | --- | --- |
| `api_key` | Required | Organization API secret key from Circa's integration settings, sent as a Bearer credential. |
| `resource` | Required | One resource from the table above. |
| `start_date` | Absent | Initial inclusive whole-second ISO 8601 timestamp with an explicit offset. Enables filtering for events, contacts, and companies. |
| `end_date` | Time when the split starts | Optional inclusive upper bound for a timestamp-filtered read. |
| `incremental_start` | Absent | Optional previous timestamp as a lower bound for a subsequent run. Requires `start_date`. |
| `lookback_seconds` | `1` | Replay overlap before `incremental_start`, without going earlier than `start_date`. |
| `max_pages` | `10000` | Cap on accepted API pages and unpaged responses, including hidden parent discovery. |
| `connect_timeout_ms` | `10000` | TCP connection timeout. |
| `read_timeout_ms` | `30000` | Socket read inactivity timeout. |
| `request_deadline_ms` | `60000` | End-to-end deadline per request including backoff and retries. |
| `max_attempts` | `4` | Total attempts for eligible transient GET responses. |
| `max_response_bytes` | `8388608` | Per-response limit for both network and decompressed bytes. |
| `tls_trust_certificate` | JVM trust store | Optional absolute engine-local PEM or DER trust anchor path. TLS hostname verification remains enabled. |

Circa list filters are JSON request bodies on GET requests. Paginated lists use fixed 25-record pages. The connector advances using the provider's explicit pagination when present, or the page length when pagination is absent. Team and field lists are returned as a single response. Requests stay on the configured provider identity and redirects are disabled.

Relationship scans enumerate the complete parent lists independently, including older parents. Checkpoint restore refetches a partially consumed page and verifies its digest before skipping saved positions. A changed page causes a recoverable job failure to prevent silent skips. The remote paginated inventory can change during a scan, so a job has no point-in-time snapshot across pages. Subsequent jobs can supply the latest observed `cursor_timestamp` as `incremental_start`. Inclusive time bounds and overlap can re-read rows, so use the resource identity or the relationship's parent and contact IDs when deduplicating downstream.

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  Circa {
    api_key = "<organization-api-key>"
    resource = "events"
    start_date = "2026-01-01T00:00:00Z"
    end_date = "2026-01-31T23:59:59Z"
  }
}
sink {
  Console {}
}
```

## Changelog

<ChangeLog />
