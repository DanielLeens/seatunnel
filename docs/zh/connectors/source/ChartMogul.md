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

import ChangeLog from "../changelog/connector-http-chartmogul.md";

# ChartMogul

`ChartMogul` 源在 `BATCH` 模式下通过 ChartMogul API 读取客户、订阅活动和四种客户数量时间序列。HTTPS Basic 身份验证使用 API key 作为用户名，密码为空。[身份验证](https://dev.chartmogul.com/docs/authentication/)。

| 资源 | GET 路径 | 记录标识 |
| --- | --- | --- |
| `customers` | `/v1/customers` | 内部数字 `id` |
| `activities` | `/v1/activities` | 活动 `uuid` |
| `customer_daily_count` | `/v1/metrics/customer-count`，`interval=day` | 周期结束日期 |
| `customer_weekly_count` | `/v1/metrics/customer-count`，`interval=week` | 周期结束日期 |
| `customer_monthly_count` | `/v1/metrics/customer-count`，`interval=month` | 周期结束日期 |
| `customer_quarterly_count` | `/v1/metrics/customer-count`，`interval=quarter` | 周期结束日期 |

客户和活动使用原生 `has_more` 和 `cursor`。当 `has_more` 为 true 时，短页或空页后仍继续读取。客户数量请求返回所选日历周期的完整指标序列。[客户](https://dev.chartmogul.com/reference/customers/list/)、[活动](https://dev.chartmogul.com/reference/activities/list/)和[客户数量](https://dev.chartmogul.com/reference/metrics/customer-count/)。

## 输出字段

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `resource` | STRING | 所选资源名 |
| `id` | STRING | 资源表中的记录标识。客户数字 ID 保留完整精度 |
| `uuid` | STRING | 客户或活动的原生 UUID，缺失时为空 |
| `event_time` | STRING | 客户 `customer-since` 或活动 `date` 转为 UTC，指标则使用原生 `YYYY-MM-DD` 周期结束日期 |
| `record` | STRING | 原生 JSON，保留自定义属性、数据来源和 null。活动金额保留原始的整数分单位 |

时间范围适用于活动和客户数量。选择这些资源时需配置 `start_date`。活动包含时间窗口的两个边界。客户数量请求使用对应的 UTC 日历日期，保留 ChartMogul 返回的周期结束日期，包括长短不同的月份及季度。客户使用全量读取。

## 参数

| 参数 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `token` | String | 是 | | ChartMogul API key |
| `resources` | 字符串列表 | 否 | `[]` | 使用资源表中的名称，空列表读取全部六种 |
| `start_date` | String | 选择活动或客户数量时 | | 含时区偏移的 ISO 8601 时间，包含下界 |
| `end_date` | String | 否 | | 含时区偏移的 ISO 8601 时间，包含上界。省略时使用扫描创建时间作为固定上界 |
| `tls_trust_certificate` | String | 否 | | 源连接器专用的 PEM 或 base64 DER X.509 信任证书。始终验证 HTTPS 主机名 |
| `page_size` | Integer | 否 | `200` | 客户或活动每页请求数，范围 1 至 200 |
| `max_pages_per_collection` | Integer | 否 | `10000` | 单个集合接收页面的上限，范围 1 至 100000 |
| `max_pages_per_scan` | Integer | 否 | `100000` | 一次扫描接收响应数的上限，范围 1 至 1000000 |
| `max_unpaginated_items` | Integer | 否 | `10000` | 单次客户数量响应接收记录数的上限，范围 1 至 100000 |
| `connect_timeout_ms` | Integer | 否 | `10000` | TCP 连接超时，范围 100 至 300000 毫秒 |
| `request_timeout_ms` | Integer | 否 | `30000` | 单次 HTTPS 请求期限，范围 100 至 300000 毫秒 |
| `operation_timeout_ms` | Integer | 否 | `120000` | 单次读取包含重试和等待的总期限。至少等于 `request_timeout_ms`，最多 600000 毫秒 |
| `request_interval_ms` | Integer | 否 | `30` | HTTPS 调用的最短间隔，范围 0 至 60000 毫秒 |
| `max_retries` | Integer | 否 | `3` | 临时失败的额外尝试次数，范围 0 至 8 |
| `max_retry_delay_ms` | Integer | 否 | `60000` | 可接受的最大重试等待，范围 250 至 600000 毫秒 |
| `max_response_bytes` | Integer | 否 | `8388608` | 传输及解压后响应体的字节上限，范围 256 至 16777216 |

一个活跃读取器按配置的最短间隔调用接口。账户或服务器还可能有计算量和并发限制。连接器在限定范围内退避，并遵守服务端提供的 `Retry-After`。HTTP 500 最多重试两次。[API 限速说明](https://dev.chartmogul.com/docs/rate-limits/)。

## 恢复

检查点保存当前资源、游标、页摘要、页内行号以及固定的时间上界。恢复时先校验已保存的页面，再输出剩余数据。使用相同的密钥、资源范围、页大小和日期配置。检查点中不保存密钥或 JSON 记录。ChartMogul 数据可能在扫描过程中变化，因此检查点无法提供整个账户的同一时间点快照。

## 示例

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  ChartMogul {
    token = "${CHARTMOGUL_API_KEY}"
    resources = ["customers", "activities", "customer_monthly_count"]
    start_date = "2026-01-01T00:00:00Z"
    end_date = "2026-03-31T23:59:59Z"
  }
}
sink {
  Console {}
}
```

<ChangeLog />
