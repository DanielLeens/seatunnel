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

## 描述

Capsule CRM Source 使用现有的个人访问令牌或已签发的 OAuth 访问令牌，通过 Capsule API v2 读取账号数据。连接器只发起已认证的 HTTPS GET 请求，并保留 API 返回的完整记录。可选取一个或多个资源执行有界扫描，也可为销售机会指定更新时间窗口。

## 支持的资源

以下路径以 `https://api.capsulecrm.com/api/v2` 为基准。

| 资源 | 路径 | 范围 |
| --- | --- | --- |
| `users` | `/users` | 账号用户 |
| `parties` | `/parties` | 个人和组织 |
| `tasks` | `/tasks` | 账号任务 |
| `employees` | `/parties/{id}/people` | 所发现组织下的联系人 |
| `projects` | `/kases` | 项目，沿用 API 的历史路径名 |
| `opportunities` | `/opportunities` | 销售机会，可选 `updatedAt` 时间窗口 |
| `pipelines` | `/pipelines` | 销售机会流程 |
| `milestones` | `/milestones` | 销售机会里程碑 |
| `site` | `/site` | 令牌对应账号的单个信息对象 |
| `tags` | `/{entity}/tags` | 指定实体的标签定义 |
| `custom_fields` | `/{entity}/fields/definitions` | 指定实体的自定义字段定义 |
| `lost_reasons` | `/lostreasons` | 销售机会丢单原因 |
| `board` | `/boards` | 项目看板 |
| `categories` | `/categories` | 任务类别 |
| `activity_types` | `/activitytypes` | 活动类型定义 |
| `stages` | `/stages` | 账号级项目阶段，记录中包含对应看板信息 |

选择 `employees` 后，即使输出中没有选择 `parties`，连接器仍遍历令牌可见的组织分页。连接器沿用 Capsule 的可见性和状态默认值，已归档或不可见记录仍受 API 的默认条件限制。

## 配置参数

| 参数 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `token` | string | 是 | | Capsule 访问令牌，填写时省略 `Bearer` 前缀。 |
| `resources` | list of strings | 是 | | 选择上表中至少一种资源，资源名不可重复。 |
| `entity` | string | 否 | `parties` | 标签和自定义字段所属的实体。可选 `parties`、`opportunities` 或 `kases`。 |
| `start_date` | string | 否 | | 销售机会 `updatedAt` 的含下界，使用带 UTC 偏移的 ISO-8601 时间戳。 |
| `end_date` | string | 否 | | 销售机会 `updatedAt` 的不含上界，使用带 UTC 偏移的 ISO-8601 时间戳。只设置 `start_date` 时，取本次扫描启动时的时间作为上界。 |
| `page_size` | integer | 否 | `100` | 对支持页大小的接口设置每页请求条数。范围 1–100。 |
| `max_parents` | integer | 否 | `10000` | 遍历员工时保留的组织 ID 上限。范围 1–50000。 |
| `max_requests` | integer | 否 | `100000` | 每次扫描允许成功处理的逻辑分页请求上限，包含父资源发现。范围 1–1000000。 |
| `max_attempts` | integer | 否 | `4` | 每页尝试次数，包含首次 GET。范围 1–10。 |
| `request_timeout_ms` | integer | 否 | `30000` | 每页请求、读取、重试和等待的总时间预算。范围 100–3700000。 |
| `max_response_bytes` | integer | 否 | `8388608` | 解码后单次响应字节数上限。范围 1024–67108864。 |
| `endpoint` | string | 否 | `https://api.capsulecrm.com` | 自定义 HTTPS 源，仅填写协议、主机和可选端口。设置时也需提供 `trust_certificate`。 |
| `trust_certificate` | string | 否 | | 自定义源的 PEM 信任证书路径，文件需在执行节点上可读。仍校验证书主机名。 |

日期仅用于 `opportunities`。未设置起点时，从令牌可见的最早记录读取。Capsule 的 `since` 参数排除边界时刻，因此实际请求向前重叠一秒，再按配置精确过滤纳秒精度的边界和同一时间戳的记录。收到 HTTP 429 时，如果等待仍在本页预算内，重试会等待至 `X-RateLimit-Reset` 的 UTC 秒和 `Retry-After` 中更晚的时刻。

## 输出与恢复

| 列 | 类型 | 说明 |
| --- | --- | --- |
| `resource` | string | 选中的资源名。 |
| `record_id` | string | API 记录 ID。对 `site` 则使用账号子域名。 |
| `parent_id` | nullable string | 员工对应的组织 ID，或标签和自定义字段定义所属的实体名。 |
| `updated_at` | nullable string | API 提供时保留原始 `updatedAt` 文本。 |
| `payload` | string | 完整记录的 JSON 字符串，保留嵌套结构和数值精度。 |

连接器执行一次有界扫描。根据 Capsule 的 `Link` 继续翻页，校验路径和过滤条件；空页有后续链接时仍继续。检查点保留当前页、组织遍历进度、固定时间窗口和页内已输出位置。如果部分输出页在恢复时发生变化，或后续页标记改变，作业会失败。恢复时应保持读取范围一致。重新创建扫描会使用配置的日期。由于 Capsule 分页在扫描期间可能变动，跨页记录可能重复。扫描没有跨页事务隔离，也不产生删除事件。

## 示例

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

参见 [Capsule API 分页](https://developer.capsulecrm.com/v2/overview/reading-from-the-api)、[销售机会接口](https://developer.capsulecrm.com/v2/operations/Opportunity)和 [API 限流](https://developer.capsulecrm.com/v2/overview/handling-api-responses)。
