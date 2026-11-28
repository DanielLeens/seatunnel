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

import ChangeLog from '../changelog/connector-http-clarifai.md';

# Clarifai

The Clarifai source lists existing resources from the Clarifai v2 API. A bounded job selects one resource and uses source parallelism 1.

| Resource | Scope and response |
| --- | --- |
| `applications` | Applications for the configured owner. |
| `datasets` | Datasets in each application. |
| `models` | Models in each application. |
| `model_versions` | Versions of each model in each application. |
| `workflows` | Workflows in each application. The historical `worklows` spelling is accepted as an alias. |
| `app_inputs` | Input records in each application. Payload links and media fields are returned as data. |
| `app_inputs_jobs` | Existing input-adding jobs, with their progress and status. |
| `app_concepts` | Concepts in each application. |

The output contains six string columns: `id`, `user_id`, `app_id`, `model_id`, `cursor_timestamp`, and `data`. The first two identify the resource and configured owner. The parent columns come from the requested path. `app_id` is null for application rows, and `model_id` is set only for model versions. Use these columns together to distinguish resources with the same ID in different scopes. The `data` column retains the complete JSON record, including nested status, unknown fields, explicit nulls, and numeric precision. The connector reads list endpoints without downloading media or modifying provider resources.

| Option | Default | Description |
| --- | --- | --- |
| `token` | Required | A personal access token with the needed resource read scopes. Sent as `Authorization: Key <token>`. |
| `user_id` | Required | Clarifai application owner whose resource graph is read. |
| `resource` | Required | One resource from the list above. |
| `page_size` | Resource-specific | Optional limit from 1 to 1000. The defaults are 20 for applications, inputs, input-adding jobs and concepts, and 100 for the other resources. The override also applies to parent pages. |
| `start_date` | Absent | Enables an inclusive `modified_at` filter for applications, datasets, or models. Use an ISO 8601 timestamp with an explicit offset. |
| `end_date` | First read time | Optional inclusive end timestamp for a filtered read. |
| `incremental_start` | Absent | The previous cursor for a later filtered read. Requires `start_date`. |
| `lookback_seconds` | `1` | Overlap before the previous cursor, bounded by `start_date`. |
| `max_pages` | `10000` | Total accepted list pages, including hidden application and model discovery. |
| `connect_timeout_ms` | `10000` | TCP connection timeout. |
| `read_timeout_ms` | `30000` | Socket read inactivity timeout. |
| `request_deadline_ms` | `60000` | Total request timeout including retries and backoff. |
| `max_attempts` | `4` | Number of attempts for eligible transient HTTP or API status responses. |
| `max_response_bytes` | `8388608` | Limit on both wire and decompressed response bytes. |
| `tls_trust_certificate` | JVM trust store | Optional absolute engine-local path to a PEM or DER trust anchor. The connector still verifies the API hostname. |

For the three timestamp-filtered resources, known `modified_at` timestamps are converted to UTC in `cursor_timestamp`. A missing timestamp is emitted with a null cursor, including in a filtered read. Subsequent jobs can pass the latest known cursor as `incremental_start`. The inclusive range and overlap can produce duplicates across runs.

The connector sends `page` and `per_page` as GET query parameters. A short page ends the current list. HTTP success also requires a successful numerical API status. The documented API/client deprecation warnings require an explicit list. A route or permission error fails the job. The connector stays on `api.clarifai.com` and disables redirects.

Hidden parent discovery scans without the selected resource's time filter. Checkpoint restore verifies the current app, model and list page digests before skipping recorded positions. A changed page causes a job failure so the reader can avoid skipping uncertain rows. The remote API can change during a scan, so a paginated read is not a consistent snapshot.

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  Clarifai {
    token = "<personal-access-token>"
    user_id = "example-user"
    resource = "datasets"
    start_date = "2026-01-01T00:00:00Z"
  }
}
sink {
  Console {}
}
```

## Changelog

<ChangeLog />
