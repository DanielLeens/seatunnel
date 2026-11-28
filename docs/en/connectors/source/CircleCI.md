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

# CircleCI

The CircleCI source reads project, workflow, and account records using the CircleCI HTTPS API. Generate a personal API token for a user with access to the requested resources. Requests use the `Circle-Token` header in both API versions.

## Resources

| Resource | GET route | Record identity | Record time |
| --- | --- | --- | --- |
| `context` | `/api/v2/context` with `owner-id` | Context ID | `created_at` |
| `self_ids` | `/api/v2/me` | User ID | |
| `self_collaborations` | `/api/v2/me/collaborations` | Organization ID, or VCS type and slug when the ID is null | |
| `me` | `/api/v1.1/me` | Analytics ID, or login when the analytics ID is absent | `created_at` when present |
| `projects` | `/api/v1.1/projects` | VCS URL of a followed project | |
| `pipelines` | `/api/v2/pipeline` with `org-slug` | Pipeline ID | `created_at` |
| `specific_project` | `/api/v2/project/{project-slug}` | Project ID | |
| `workflow` | `/api/v2/workflow/{workflow-id}` | Workflow ID | `created_at` |
| `workflow_jobs` | `/api/v2/workflow/{workflow-id}/job` | Job ID | `started_at` when present |
| `jobs` | `/api/v2/project/{project-slug}/job/{job-number}` | Job number in the configured project | |
| `insights_metrics` | `/api/v2/insights/pages/{project-slug}/summary` | Project ID, or configured slug when the ID is absent | |
| `insights_branches` | `/api/v2/insights/{project-slug}/branches` | Project ID | |

Use a slug from CircleCI. For example, use `gh/my-organization` and `gh/my-organization/my-project` for a GitHub OAuth project. For a GitHub App or GitLab project, use `circleci/{organization-id}` and `circleci/{organization-id}/{project-id}`.

For `jobs`, provide a `job_number` or let the source discover job numbers from `workflow_ids`. Discovery reads all the configured workflow job pages and looks up each distinct numbered job in the configured project. It runs independently of the output time window and also works when `jobs` is the only selected resource. Select `workflow_jobs` to retain entries such as approvals without a job number.

CircleCI returns pipelines for up to 250 recently built projects that the authenticated user follows. Insights branches contain at most 5,000 branches visible in Insights. The provider computes summary metrics from at most 90 days of activity, and trends cover at most 30 days. The `projects` payload also accepts an absent `branches` field because CircleCI scheduled its removal for December 7, 2026.

## Options

| Option | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `token` | string | Yes | | Personal API token. |
| `resources` | list of strings | Yes | | Unique resource names from the table. |
| `org_id` | string | For `context` | | Organization UUID. |
| `org_slug` | string | For `pipelines` | | Organization slug with two path segments. |
| `project_slug` | string | For project details, job details, and Insights | | Project slug with three path segments. |
| `workflow_ids` | list of strings | For workflows, workflow jobs, and job discovery | | Between 1 and 100 unique workflow UUIDs. |
| `job_number` | long | No | | Nonnegative explicit job number. Omit to discover from `workflow_ids`. |
| `start_date` | string | No | | Inclusive ISO 8601 timestamp with an offset. |
| `end_date` | string | No | | Exclusive ISO 8601 timestamp with an offset. A scan with only `start_date` captures its end when the split is created. |
| `reporting_window` | string | No | `last-90-days` (provider) | Summary window. Also accepts `last-24-hours`, `last-7-days`, `last-30-days`, or `last-60-days`. |
| `max_jobs` | integer | No | `4096` | Maximum retained job numbers for discovery, range 1–10000. |
| `max_requests` | integer | No | `10000` | Maximum committed responses per scan, range 1–20000. |
| `max_attempts` | integer | No | `4` | Attempts per request including the initial GET, range 1–6. |
| `request_timeout_ms` | integer | No | `30000` | Deadline for each response including retries and decoding, range 100–300000. |
| `max_response_bytes` | integer | No | `8388608` | Maximum decoded bytes per response, range 1024–33554432. |
| `endpoint` | string | No | `https://circleci.com` | Custom bare HTTPS origin. Requires `trust_certificate`. |
| `trust_certificate` | string | For a custom origin | | Path to a worker-local X.509 PEM trust file. The endpoint hostname is verified. |

Date bounds filter only the dated resources in the table. The connector retains undated legacy user and workflow job records. A scan reads documented continuation tokens from contexts, pipelines, and workflow jobs. A parent can yield up to 4,096 continuations, and a single response can contain up to 20,000 records. HTTP 429 retries respect the provider's advertised delay within the deadline.

## Output and recovery

| Column | Type | Description |
| --- | --- | --- |
| `resource` | string | Resource name. |
| `record_id` | string | Identity described in the resources table. |
| `parent_id` | nullable string | Organization UUID for contexts, organization slug for pipelines, workflow UUID for workflow jobs, or project slug for job details and Insights. |
| `record_time` | nullable string | ISO 8601 timestamp in UTC with fractional precision preserved. |
| `payload` | string | Full JSON object with nested data, nulls, and exact numeric values. |

Use bounded batch mode with source parallelism 1. Checkpoints retain the current traversal, date cutoff, and row offset. A restored partial response must match its saved fingerprint before the source emits its remaining rows. CircleCI can change results during a scan. This is a paginated read of the provider's current data, without a global snapshot or change stream.

## Example

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  CircleCI {
    token = ${CIRCLECI_TOKEN}
    resources = ["pipelines", "workflow", "workflow_jobs", "jobs", "insights_metrics"]
    org_slug = "gh/my-organization"
    project_slug = "gh/my-organization/my-project"
    workflow_ids = ["4c372420-727b-4753-a9e2-6f5483416ac6"]
    start_date = "2026-10-01T00:00:00Z"
    end_date = "2026-11-01T00:00:00Z"
    reporting_window = "last-30-days"
    plugin_output = "circleci_records"
  }
}

sink {
  Console {
    plugin_input = "circleci_records"
  }
}
```

[CircleCI API v2 reference](https://circleci.com/docs/api/v2/), [CircleCI API v1.1 reference](https://circleci.com/docs/api/v1/), [CircleCI job details guide](https://circleci.com/docs/guides/toolkit/api-developers-guide/#get-job-details), [project branches deprecation](https://circleci.com/changelog/deprecation-of-branches-data-in-the-v1-and-v1-1-project-and-settings-apis/).
