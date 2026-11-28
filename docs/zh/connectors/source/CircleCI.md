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

CircleCI Source 通过 CircleCI HTTPS API 读取项目、工作流和账户记录。请使用有权访问所选资源的用户创建个人 API 令牌。两个 API 版本均通过 `Circle-Token` 请求头认证。

## 资源

| 资源 | GET 路径 | 记录标识 | 记录时间 |
| --- | --- | --- | --- |
| `context` | `/api/v2/context`，带 `owner-id` | 上下文 ID | `created_at` |
| `self_ids` | `/api/v2/me` | 用户 ID | |
| `self_collaborations` | `/api/v2/me/collaborations` | 组织 ID。若 ID 为 null，使用 VCS 类型和 slug | |
| `me` | `/api/v1.1/me` | Analytics ID。若缺失，使用登录名 | 有值时取 `created_at` |
| `projects` | `/api/v1.1/projects` | 用户关注项目的 VCS URL | |
| `pipelines` | `/api/v2/pipeline`，带 `org-slug` | 流水线 ID | `created_at` |
| `specific_project` | `/api/v2/project/{project-slug}` | 项目 ID | |
| `workflow` | `/api/v2/workflow/{workflow-id}` | 工作流 ID | `created_at` |
| `workflow_jobs` | `/api/v2/workflow/{workflow-id}/job` | 作业 ID | 有值时取 `started_at` |
| `jobs` | `/api/v2/project/{project-slug}/job/{job-number}` | 配置项目内的作业编号 | |
| `insights_metrics` | `/api/v2/insights/pages/{project-slug}/summary` | 项目 ID。若缺失，使用配置的项目 slug | |
| `insights_branches` | `/api/v2/insights/{project-slug}/branches` | 项目 ID | |

使用 CircleCI 中的组织和项目 slug。例如，GitHub OAuth 项目可用 `gh/my-organization` 和 `gh/my-organization/my-project`。GitHub App 或 GitLab 项目可用 `circleci/{organization-id}` 和 `circleci/{organization-id}/{project-id}`。

读取 `jobs` 时，可指定 `job_number`，也可通过 `workflow_ids` 发现编号。发现过程遍历所有配置工作流的作业页，在指定项目内对作业编号去重后读取详情。此过程独立于输出时间窗口，仅选择 `jobs` 时也可使用。选择 `workflow_jobs` 可保留没有作业编号的条目，例如审批。

CircleCI 流水线接口最多覆盖当前用户关注且最近构建的 250 个项目。Insights 分支最多返回 5,000 个在 Insights 中可见的分支。概要指标最多汇总 90 天，趋势最多覆盖 30 天。CircleCI 计划于 2026 年 12 月 7 日移除旧版项目响应的 `branches` 字段，因此 `projects` 支持缺少该字段的响应。

## 配置

| 参数 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `token` | string | 是 | | CircleCI 个人 API 令牌。 |
| `resources` | list of strings | 是 | | 上表中一个或多个不重复的资源名。 |
| `org_id` | string | `context` 必填 | | 组织 UUID。 |
| `org_slug` | string | `pipelines` 必填 | | 两段式组织 slug。 |
| `project_slug` | string | 项目详情、作业详情和 Insights 必填 | | 三段式项目 slug。 |
| `workflow_ids` | list of strings | 工作流、工作流作业和作业发现必填 | | 1–100 个不重复的工作流 UUID。 |
| `job_number` | long | 否 | | 非负作业编号。省略时通过 `workflow_ids` 发现。 |
| `start_date` | string | 否 | | 含偏移量的 ISO 8601 起始时间，包含该边界。 |
| `end_date` | string | 否 | | 含偏移量的 ISO 8601 结束时间，不包含该边界。仅配置起始时间时，以分片创建时间为结束时间。 |
| `reporting_window` | string | 否 | `last-90-days`（供应商默认） | Insights 概要区间。也支持 `last-24-hours`、`last-7-days`、`last-30-days` 和 `last-60-days`。 |
| `max_jobs` | integer | 否 | `4096` | 发现模式保留的唯一作业编号上限，范围 1–10000。 |
| `max_requests` | integer | 否 | `10000` | 一次扫描提交的响应上限，范围 1–20000。 |
| `max_attempts` | integer | 否 | `4` | 每次请求的尝试次数，含首次 GET，范围 1–6。 |
| `request_timeout_ms` | integer | 否 | `30000` | 每个响应的总时限，含重试和解码，范围 100–300000。 |
| `max_response_bytes` | integer | 否 | `8388608` | 单次响应解码后的字节上限，范围 1024–33554432。 |
| `endpoint` | string | 否 | `https://circleci.com` | 自定义 HTTPS 源地址。需同时配置 `trust_certificate`。 |
| `trust_certificate` | string | 自定义源地址必填 | | Worker 本地可读的 X.509 PEM 信任文件路径，仍校验主机名。 |

时间范围仅过滤上表中带记录时间的资源。没有时间的旧版用户和工作流作业记录会保留。上下文、流水线和工作流作业沿用 API 返回的分页令牌。单个父级最多可续页 4,096 次，单次响应最多容纳 20,000 条记录。HTTP 429 重试在总时限内遵守供应商返回的等待时间。

## 输出和恢复

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `resource` | string | 资源名。 |
| `record_id` | string | 资源表中的记录标识。 |
| `parent_id` | nullable string | 上下文的组织 UUID、流水线的组织 slug、工作流作业的工作流 UUID，或作业详情和 Insights 的项目 slug。 |
| `record_time` | nullable string | 保留小数秒精度的 UTC ISO 8601 时间。 |
| `payload` | string | 完整 JSON 对象，保留嵌套字段、null 和精确数值。 |

请使用有界批模式，Source 并行度为 1。检查点记录当前读取位置、结束时间和页内偏移。恢复未读完的响应时，校验通过后才输出其余记录。CircleCI 可能在扫描期间更新查询结果，此 Source 提供当前数据的分页读取。全局快照和变更流不在此读取范围内。

## 示例

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

[CircleCI API v2 文档](https://circleci.com/docs/api/v2/)、[CircleCI API v1.1 文档](https://circleci.com/docs/api/v1/)、[作业详情指南](https://circleci.com/docs/guides/toolkit/api-developers-guide/#get-job-details)、[项目分支字段移除公告](https://circleci.com/changelog/deprecation-of-branches-data-in-the-v1-and-v1-1-project-and-settings-apis/)。
