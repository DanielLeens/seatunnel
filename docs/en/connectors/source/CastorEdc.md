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

# CastorEdc

## Description

The Castor EDC source reads bounded study metadata, user records, and audit events from the regional Castor EDC/CDMS API. It uses OAuth client credentials for the chosen region and applies the credentials' existing access restrictions.

## Resources

Paths below are relative to the regional `/api` prefix.

| Resource | GET path | Output |
| --- | --- | --- |
| `user` | `/user` | Users visible to the credential |
| `study` | `/study` | Visible studies |
| `audit_trial` | `/study/{study_id}/audit-trail` | Audit events in the selected time window |
| `country` | `/country` | Country reference data |
| `study_survey` | `/study/{study_id}/survey` | Survey definitions |
| `study_survey_package` | `/study/{study_id}/survey-package` | Survey package definitions |
| `study_site` | `/study/{study_id}/site` | Study sites |
| `study_fields` | `/study/{study_id}/field` | Study field definitions |
| `study_field_dependency` | `/study/{study_id}/field-dependency` | Field dependencies |
| `study_field_validation` | `/study/{study_id}/field-validation` | Field validations |
| `study_form` | `/study/{study_id}/form` | Study forms |
| `study_role` | `/study/{study_id}/role` | Study roles |
| `study_fieldoption_groups` | `/study/{study_id}/field-optiongroup` | Field option groups |
| `study_statistics` | `/study/{study_id}/statistics` | One statistics object per study |
| `study_user` | `/study/{study_id}/user` | Users assigned to each study |
| `study_visit` | `/study/{study_id}/visit` | Visit definitions |

Study-scoped resources discover all visible studies even when `study` is absent from the output or its records fall outside the output time window. A permission failure stops the reader with the resource and HTTP status. The provider currently documents the audit endpoint as beta. Audit content and historical availability follow the provider's access and retention rules.

## Configuration

| Option | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `auth_client_id` | string | Yes | | Castor OAuth client ID. |
| `auth_client_secret` | string | Yes | | Castor OAuth client secret. |
| `resources` | list of strings | Yes | | One or more distinct names from the table. |
| `url_region` | string | No | `uk` | `uk`, `us`, `au`, or `eu`/`nl`. The latter two use `data.castoredc.com`. |
| `start_date` | string | For `audit_trial` | | Inclusive lower bound. Use an ISO-8601 timestamp with UTC offset. |
| `end_date` | string | No | | Exclusive upper bound. Use an ISO-8601 timestamp with UTC offset. With only a start date, the initial scan time is frozen as the upper bound. |
| `legacy_timezone` | string | No | `UTC` | IANA time zone or offset for timestamps that supply neither an offset nor a time zone. |
| `audit_page_size` | integer | No | | Requested audit page size, range 1–1000. Omit to use the provider default. |
| `request_interval_ms` | integer | No | `1100` | Minimum gap between request starts in one reader, including OAuth and retries. Range 1000–60000. |
| `max_studies` | integer | No | `10000` | Maximum retained study IDs for child traversal. Range 1–50000. |
| `max_requests` | integer | No | `100000` | Maximum successfully processed logical responses per scan, including discovery. Range 1–1000000. |
| `max_attempts` | integer | No | `4` | Attempt limit for one data read or one token exchange. Range 1–10. |
| `request_timeout_ms` | integer | No | `30000` | Total acquisition time for one data response, including pacing, OAuth and retries. Range 100–650000. |
| `max_response_bytes` | integer | No | `8388608` | Maximum decoded bytes in a data response. Range 1024–67108864. |
| `endpoint` | string | No | Regional HTTPS origin | Bare HTTPS override. Supply `trust_certificate` with it. |
| `trust_certificate` | string | No | | Worker-local PEM trust certificate for a custom origin. Hostname checks still apply. |

The date window applies to `created_on` for study output, `last_login` for user and study-user output, and audit event time. Users who have never signed in appear in an unfiltered scan. Audit requests use calendar dates one day beyond each query boundary and then apply the exact time window locally to accommodate date-only provider filtering. Responses with an ambiguous offset-free local time fail explicitly.

The connector obtains a regional token using `/oauth/token`, renews it before the next read when its lifetime is too short, and renews once following an eligible HTTP 401. Each retry respects `Retry-After` when available. HTTP 429 without that header uses Castor's documented ten-minute default window and fails if the wait exceeds the remaining page budget. One reader paces its own requests. Castor applies its quotas across all users of the same client credentials.

## Output and checkpoint restore

| Column | Type | Description |
| --- | --- | --- |
| `resource` | string | Resource selected in the source. |
| `record_id` | string | Provider ID; role name for a role without an ID. Audit rows without an ID use a content digest and identical events can share that value. |
| `study_id` | nullable string | Uppercase study UUID for study and study-scoped output. |
| `record_time` | nullable string | Exact provider timestamp normalized to UTC for dated resources. |
| `payload` | string | Complete JSON record with nested data, nulls and numeric precision preserved. |

The source validates continuation within the same regional origin, study path and fixed query filters. Checkpoints retain the frozen window, study traversal, page, and emitted offset. Restoring a partially emitted page checks the provider records and continuation before emitting more. Start a fresh scan with a different read scope. Result pages can change during a run, and these APIs do not provide a common transactional snapshot or deletion events.

## Example

```hocon
env {
  job.mode = "BATCH"
}

source {
  CastorEdc {
    auth_client_id = ${CASTOR_CLIENT_ID}
    auth_client_secret = ${CASTOR_CLIENT_SECRET}
    url_region = "eu"
    resources = ["study", "study_fields", "study_statistics"]
    plugin_output = "castor"
  }
}

sink {
  Console {
    plugin_input = "castor"
  }
}
```

See [Castor EDC API](https://data.castoredc.com/api), [regional OAuth and permissions](https://helpdesk.castoredc.com/hc/en-us/articles/27149233860509-Castor-EDC-CDMS-Application-Programming-Interface-API), [audit trail](https://helpdesk.castoredc.com/hc/en-us/articles/27127424188317-Audit-trail-in-EDC-CDMS), and [API use policy](https://helpdesk.castoredc.com/hc/en-us/articles/28549477683741-API-Developer-Policy).
