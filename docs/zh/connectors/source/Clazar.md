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

# Clazar

Clazar Source 在有界批模式下读取实体集合以及 AWS、Azure、Google Cloud 云市场分析报表。Source 并行度设为 1。Worker 使用配置的客户端凭据换取 Bearer 令牌，并在较长的扫描中续期。

## 资源

| 资源 | GET 路径 |
| --- | --- |
| `buyers` | `/buyers/` |
| `listings` | `/listings/` |
| `contracts` | `/contracts/` |
| `opportunities` | `/opportunities/` |
| `private_offers` | `/private_offers/` |
| `analytics_aws_cosell_opportunities` | `/analytics/datasets/aws_cosell_opportunities` |
| `analytics_aws_marketplace_disbursements` | `/analytics/datasets/aws_marketplace_disbursements` |
| `analytics_aws_marketplace_revenue` | `/analytics/datasets/aws_marketplace_revenue` |
| `analytics_azure_cosell_opportunities` | `/analytics/datasets/azure_cosell_opportunities` |
| `analytics_azure_marketplace_customers` | `/analytics/datasets/azure_marketplace_customers` |
| `analytics_azure_marketplace_metered_usage` | `/analytics/datasets/azure_marketplace_metered_usage` |
| `analytics_azure_marketplace_orders` | `/analytics/datasets/azure_marketplace_orders` |
| `analytics_azure_marketplace_revenue` | `/analytics/datasets/azure_marketplace_revenue` |
| `analytics_gcp_marketplace_charges_and_usage` | `/analytics/datasets/gcp_marketplace_charges_and_usage` |
| `analytics_gcp_marketplace_daily_insights` | `/analytics/datasets/gcp_marketplace_daily_insights` |
| `analytics_gcp_marketplace_disbursements` | `/analytics/datasets/gcp_marketplace_disbursements` |
| `analytics_gcp_marketplace_disbursements_summary` | `/analytics/datasets/gcp_marketplace_disbursements_summary` |
| `analytics_gcp_marketplace_incremental_daily_insights` | `/analytics/datasets/gcp_marketplace_incremental_daily_insights` |
| `analytics_gcp_marketplace_incremental_monthly_insights` | `/analytics/datasets/gcp_marketplace_incremental_monthly_insights` |
| `analytics_gcp_marketplace_monthly_insights` | `/analytics/datasets/gcp_marketplace_monthly_insights` |

Clazar 已弃用表中前五个实体接口，并声明目前仍可调用。此 Source 直接读取这些集合。Clazar Query Platform 是另一套具有不同实体和字段投影契约的查询接口。各云市场决定账户可以读取的数据集和报表期间。

## 配置

| 参数 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `client_id` | string | 是 | | Clazar API 客户端 ID。 |
| `client_secret` | string | 是 | | Clazar API 客户端密钥。 |
| `resources` | list of strings | 是 | | 上表中一个或多个不重复的集合和数据集名称。 |
| `start_date` | string | 否 | | 含偏移量的 ISO 8601 起始时间，包含实体的 `last_modified_at` 边界。 |
| `end_date` | string | 否 | | 排除该实体结束时间边界。仅配置起始时间时，以分片创建时间为截止时间。 |
| `page_size` | integer | 否 | `100` | 请求每页行数，范围 1–5000。 |
| `max_requests` | integer | 否 | `10000` | 每次扫描提交的最大页数，范围 1–20000。 |
| `max_attempts` | integer | 否 | `4` | 数据页尝试次数，含首次 GET 和认证后的重试，范围 1–6。 |
| `request_timeout_ms` | integer | 否 | `30000` | 每页总时限，含令牌交换、传输、重试和解码，范围 100–300000。 |
| `max_response_bytes` | integer | 否 | `8388608` | 响应解码后字节上限，范围 1024–33554432。令牌响应另设 65536 字节上限。 |
| `endpoint` | string | 否 | `https://api.clazar.io` | 自定义 HTTPS 源地址，需同时配置信任文件。 |
| `trust_certificate` | string | 自定义源地址必填 | | Worker 本地可读的 X.509 PEM 文件路径，同时校验主机名。 |

配置日期范围时需选择至少一个实体集合。Worker 在本地过滤实体的修改时间以保留精确边界，并读取所选报表的全部行。页码遍历在推进前校验原生续页和总数；总数发生变化或相邻页面出现重复实体 ID 时读取失败。分析报表保留重复行。

Worker 向 `/authenticate/` 提交仅含 `client_id` 和 `client_secret` 的 JSON。正数 `expires_in` 按秒解释，数据 GET 收到 401 时可在预算内续期一次。令牌交换失败时结束当次获取。HTTP 429 数据请求在总时限内遵守服务端给出的等待时间。

## 输出和恢复

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `resource` | string | 集合或数据集名称。 |
| `record_id` | nullable string | Clazar 实体 UUID。分析报表为 null。 |
| `record_time` | nullable string | 转成 UTC 的实体修改时间。分析报表为 null。 |
| `payload` | string | 完整服务端 JSON 对象，保留精确数值、字符串、null 和嵌套结构。 |

检查点保留当前资源、页码、截止时间及页内输出偏移。恢复尚未读完的页面时，先核对指纹，再输出其余记录。Clazar 数据可能在翻页或报表期间发生更新，本 Source 提供有界读取。

## 示例

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  Clazar {
    client_id = ${CLAZAR_CLIENT_ID}
    client_secret = ${CLAZAR_CLIENT_SECRET}
    resources = ["contracts", "analytics_aws_marketplace_revenue"]
    start_date = "2026-10-01T00:00:00Z"
    end_date = "2026-11-01T00:00:00Z"
    plugin_output = "marketplace_records"
  }
}

sink {
  Console {
    plugin_input = "marketplace_records"
  }
}
```

[Clazar API 索引](https://developers.clazar.io/llms.txt)、[令牌交换文档](https://developers.clazar.io/reference/retrieve-an-access-token.md)。
