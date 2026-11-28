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

`Claude` 源在 `BATCH` 模式下读取 Claude Enterprise 的参与度、使用情况、用量和成本分析。请在 claude.ai 启用 Enterprise Analytics API，并创建有 `read:analytics` 权限的 Analytics API key，将其配置为 `token`。连接器向 `api.anthropic.com` 发送 `x-api-key` 和 `anthropic-version: 2026-10-21`。Enterprise 套餐提供参与度数据，用量与成本反映组织套餐及用量积分。 [Claude Enterprise Analytics API](https://platform.claude.com/docs/en/manage-claude/analytics-api)。

## 资源

以下路径基于 `/v1/organizations/analytics/`。

| 资源 | GET 路径 | 请求区间 | 原生标识 |
| --- | --- | --- | --- |
| `users` | `users` | 单个 UTC 日 | `user.id` |
| `chat_projects` | `apps/chat/projects` | 单个 UTC 日 | `project_id` |
| `skills` | `skills` | 单个 UTC 日 | `skill_name` |
| `connectors` | `connectors` | 单个 UTC 日 | `connector_name` |
| `summaries` | `summaries` | 单个 UTC 日 | 空 |
| `user_usage_report` | `user_usage_report` | 单个 UTC 日 | `actor.user_id` |
| `user_cost_report` | `user_cost_report` | 单个 UTC 日 | `actor.user_id` |
| `usage_report` | `usage_report` | 最多 7 个 UTC 日 | 空 |
| `cost_report` | `cost_report` | 最多 7 个 UTC 日 | 空 |

四项报告使用供应商仍支持的 Enterprise 报告接口。按用户统计的数据按产品和模型分组，成本再按成本类型分组。组织级时间桶保留完整的 `results` 数组，包含空桶。该接口每个组织级时间桶最多返回排名前 100 的分组。金额保留供应商原始类型与单位。 [用量和成本报告行为](https://platform.claude.com/docs/en/manage-claude/usage-cost-analytics-api#migrate-from-the-claude-enterprise-usage-and-cost-reports)。

## 日期窗口

两个日期选项均使用 UTC 日期，包含所选日期，最迟可选昨天。起点默认扫描开始前第 7 天，最早日期为 2026-01-01。参与度和汇总的默认末日为扫描开始前第 3 天，报告默认包含昨天。报告还受供应商 365 天历史范围限制。

底层报告请求使用排他的 UTC 结束边界。连接器冻结日期计划以供恢复。实际数据可用性取决于供应商的刷新时间。供应商拒绝日期或数据尚未可用时，连接器会报告读取错误。

## 输出

| 列 | 类型 | 说明 |
| --- | --- | --- |
| `resource` | STRING | 资源名称。 |
| `id` | STRING | 原生用户、项目、技能或连接器标识。汇总行为空。 |
| `organization_id` | STRING | 报告返回的组织 ID，参与度行为空。 |
| `window_start` | STRING | 本次查询区间起点，含 UTC 零点。 |
| `window_end` | STRING | 本次查询区间终点，排除 UTC 零点。 |
| `data_refreshed_at` | STRING | 可用时返回 API 原生的报告刷新水位。 |
| `record` | STRING | 原生 JSON 对象，保留分组结果、数值与空值。 |

多个分组行可以共享同一原生标识。下游生成键时可以使用 `record` 内的分组维度和日期。

## 选项

| 名称 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `token` | String | 是 |  | 拥有 `read:analytics` 权限的 Enterprise Analytics API key。 |
| `resources` | 字符串列表 | 否 | `[]` | 从资源表选择名称。空列表读取全部九项。 |
| `start_date` | String | 否 |  | 首个 UTC 日期（含当天），格式 `YYYY-MM-DD`。默认扫描开始前第 7 天。 |
| `end_date` | String | 否 |  | 末个 UTC 日期（含当天），格式 `YYYY-MM-DD`。默认值见日期窗口。 |
| `page_size` | Integer | 否 | `1000` | 请求页大小 1 至 1000。组织时间桶报告最多请求 31。 |
| `max_records_per_response` | Integer | 否 | `10000` | 单个响应的最大记录数，范围 1 至 100000。 |
| `max_pages_per_collection` | Integer | 否 | `10000` | 每项资源每个日期区间的页数上限，范围 1 至 100000。 |
| `max_pages_per_scan` | Integer | 否 | `100000` | 扫描接受的响应总数上限，范围 1 至 1000000。 |
| `tls_trust_certificate` | String | 否 |  | 本源专用的可选 PEM 或 base64 DER X.509 信任证书。始终启用 HTTPS 主机名校验。 |
| `connect_timeout_ms` | Integer | 否 | `10000` | TCP 连接超时范围 100 至 300000 毫秒。 |
| `request_timeout_ms` | Integer | 否 | `30000` | 单次 HTTPS 尝试期限，范围 100 至 300000 毫秒。 |
| `operation_timeout_ms` | Integer | 否 | `120000` | 单次读取含重试和等待的总期限。至少为 `request_timeout_ms`，最多 600000 毫秒。 |
| `request_interval_ms` | Integer | 否 | `1000` | HTTPS 尝试之间的最短时间间隔，范围 0 至 60000 毫秒。 |
| `max_retries` | Integer | 否 | `3` | 符合条件的临时读失败额外尝试次数，范围 0 至 8。 |
| `max_retry_delay_ms` | Integer | 否 | `60000` | 重试等待上限，范围 250 至 600000 毫秒。 |
| `max_response_bytes` | Integer | 否 | `8388608` | 传输与解压后响应体字节上限，范围 256 至 16777216。 |

连接器对限流或其他临时错误有限重试。供应商的 Analytics 限额由整个组织共享。

## 恢复

一个读取器遍历所选资源和日期区间。检查点保存游标、日期计划及页内位置。恢复请使用相同的 key、资源、日期和页大小。读取器在输出剩余行前校验保存的响应。检查点保留的是可变上游数据的扫描位置。

供应商刷新可能使游标过期并返回 HTTP 410。遇到该情况时，可对受影响的日期范围发起新的扫描。空报告没有可输出的水位行。

## 示例

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
