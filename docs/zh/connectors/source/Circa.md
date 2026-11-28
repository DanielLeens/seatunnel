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

Circa Source 读取 `app.circa.co` 活动管理服务中的活动、联系人、团队、公司、联系人关联及字段定义。每个有界作业读取一种资源，并行度为 1。

| 资源 | 读取行为 |
| --- | --- |
| `events` | 全量读取或按闭区间过滤 `updated_at`，含未设置活动日期的记录。 |
| `contacts` | 全量读取或按闭区间过滤 `updated_at`。 |
| `teams` | 全量读取，不分页。 |
| `companies` | 全量读取或按闭区间过滤 `updated_at`。 |
| `company_contacts` | 逐个读取已发现公司的联系人。 |
| `event_fields` | 活动字段定义。 |
| `contact_fields` | 联系人字段定义，保留服务返回的 EventContact 字段。 |
| `company_fields` | 公司字段定义。 |
| `event_contacts` | 逐个读取已发现活动的联系人。 |

输出有四个字符串列，分别为 `id`、`parent_id`、`cursor_timestamp` 和 `data`。两种关联资源填写 `parent_id`，同一联系人可以出现在不同父记录下。支持时间过滤的资源在有 `updated_at` 时提供 UTC 游标。 `data` 是完整记录的 JSON，保留供应商的日期、时间、嵌套自定义字段、明确的 null 和数值精度。

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `api_key` | 必填 | 从 Circa 集成设置获取组织 API 密钥，以 Bearer 凭证发送。 |
| `resource` | 必填 | 从上表选择一个资源。 |
| `start_date` | 未配置 | 初始时间下界，包含边界。使用带时区偏移的 ISO 8601 整秒时间，适用于活动、联系人和公司。 |
| `end_date` | 分片开始时间 | 可选时间上界，包含边界。 |
| `incremental_start` | 未配置 | 后续作业可提供上一轮游标。使用前须设置 `start_date`。 |
| `lookback_seconds` | `1` | 从上一轮游标向前回读的秒数，最早回到 `start_date`。 |
| `max_pages` | `10000` | 作业内可接受的分页与非分页响应总数，包含发现父记录的请求。 |
| `connect_timeout_ms` | `10000` | TCP 连接超时。 |
| `read_timeout_ms` | `30000` | 套接字读取空闲超时。 |
| `request_deadline_ms` | `60000` | 单次请求含等待和重试的总时间上限。 |
| `max_attempts` | `4` | 符合重试条件的 GET 请求的总尝试次数。 |
| `max_response_bytes` | `8388608` | 单次响应的网络字节数与解压后字节数上限。 |
| `tls_trust_certificate` | JVM 信任库 | 可选的引擎本地 PEM 或 DER 信任证书绝对路径，仍严格验证服务主机名。 |

Circa 使用 GET 请求的 JSON 正文传递列表过滤条件。分页资源每页最多 25 条，以服务返回的分页信息决定是否继续，缺失时根据当前页长度判断。团队和字段定义在单个响应中返回。请求固定使用 Circa 服务身份，不跟随重定向。

联系人关联会独立完整扫描父资源，包括旧父记录。checkpoint 恢复时重新获取已部分读取的页面，校验摘要后才跳过已处理的位置。页面发生变化时作业会失败，避免静默漏数。服务的分页结果可能在作业期间变化，跨页无法获得统一快照。后续作业可将上轮的 `cursor_timestamp` 作为 `incremental_start`。闭区间及回读窗口可能带来重复记录，下游可按资源 ID 或父级与联系人 ID 组合去重。

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

## 更新日志

<ChangeLog />
