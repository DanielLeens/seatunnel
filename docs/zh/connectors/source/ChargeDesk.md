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

import ChangeLog from "../changelog/connector-http-chargedesk.md";

# ChargeDesk

`ChargeDesk` 源通过 `https://api.chargedesk.com/v1` 读取四种资源。在 ChargeDesk 公司设置中启用 API，再配置公司的密钥。连接器使用 HTTPS Basic 身份验证，密钥作为用户名，密码为空。[ChargeDesk API 文档](https://blue.chargedesk.com/api-docs)。

| 资源 | 记录标识 | 事件时间 | 最大偏移量 |
| --- | --- | --- | --- |
| `charges` | `charge_id` | `occurred` | 50000 |
| `customers` | `customer_id` | `first_seen` | 50000 |
| `subscriptions` | `subscription_id` | `first_seen` | 10000 |
| `products` | `product_id` | `first_seen` | 10000 |

扣款记录的 JSON 会保留支付网关的 `transaction_id`。连接器根据实际返回的条数推进偏移量，短页后仍会继续读取。遇到空页时结束该集合。如果到达最大偏移量后仍返回数据，API 无法验证其后是否有更多记录，连接器会报错。

## 输出字段

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `resource` | STRING | 资源名 |
| `id` | STRING | 资源表中的 ChargeDesk 标识 |
| `event_time` | STRING | 原生 Unix 秒数转换成的 UTC 时间，缺失时为空 |
| `record` | STRING | 原生 JSON 对象，保留金额字符串、嵌套元数据和 null |

## 日期范围

`start_date` 和 `end_date` 适用于扣款、客户和订阅。包含下界，排除上界。扣款和客户分别使用服务端的 `occurred` 和 `first_seen` 参数缩小范围，再逐行精确筛选。API 的订阅 `created` 筛选参数没有明确对应 `first_seen`，因此订阅根据原生 `first_seen` 在读取后筛选。产品始终全量读取。配置日期筛选后，适用资源若缺少对应时间，作业会报错。

偏移量超限说明 API 尚未提供可确认的完整扫描结果。对扣款和客户，缩小日期范围可以减少需要遍历的偏移量。同一秒内记录过多时，仍有可能达到上限。

## 参数

| 参数 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `token` | String | 是 | | 公司 API 密钥 |
| `resources` | 字符串列表 | 否 | `[]` | 使用资源表中的名称，空列表读取全部四种资源 |
| `start_date` | Long | 否 | | 时间筛选下界，单位 Unix 秒，包含 |
| `end_date` | Long | 否 | | 时间筛选上界，单位 Unix 秒，排除。仅配置下界时，使用扫描创建时间作为固定上界 |
| `tls_trust_certificate` | String | 否 | | 源连接器专用的 PEM 或 base64 DER X.509 信任证书。始终校验 HTTPS 主机名 |
| `page_size` | Integer | 否 | `500` | 每页请求数，范围 1 至 500 |
| `max_pages_per_collection` | Integer | 否 | `10000` | 单个集合接收响应的上限，范围 1 至 100000 |
| `max_pages_per_scan` | Integer | 否 | `100000` | 一次扫描接收响应的上限，范围 1 至 1000000 |
| `connect_timeout_ms` | Integer | 否 | `10000` | TCP 连接超时，范围 100 至 300000 毫秒 |
| `request_timeout_ms` | Integer | 否 | `30000` | 单次 HTTPS 请求期限，范围 100 至 300000 毫秒 |
| `operation_timeout_ms` | Integer | 否 | `120000` | 单次读取的全部尝试和等待期限。至少等于 `request_timeout_ms`，最多 600000 毫秒 |
| `request_interval_ms` | Integer | 否 | `1050` | HTTPS 调用的最短间隔，范围 0 至 60000 毫秒 |
| `max_retries` | Integer | 否 | `3` | 可恢复失败的额外尝试次数，范围 0 至 8 |
| `max_retry_delay_ms` | Integer | 否 | `60000` | 可接受的最大重试等待，范围 250 至 600000 毫秒 |
| `max_response_bytes` | Integer | 否 | `8388608` | 传输及解压后响应体的字节上限，范围 256 至 16777216 |

默认的请求间隔考虑了 ChargeDesk 单个读取器每分钟 60 次的限制。HTTP 429 会在配置的期限内遵守服务器的 `Retry-After` 响应头。共用同一密钥的其他作业可能占用同一额度。

## 恢复

使用 `BATCH` 模式。一个活跃读取器保存集合位置、页内行号、已确认页摘要和固定日期上界。恢复时先校验尚需重读的页面，再输出其余记录。请使用相同的密钥、资源、页大小和日期范围。API 数据可以在两次调用间变化，恢复无法提供整个公司的同一时间点快照。

## 示例

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  ChargeDesk {
    token = "${CHARGEDESK_API_SECRET}"
    resources = ["charges", "customers"]
    start_date = 1775001600
    end_date = 1777593600
  }
}
sink {
  Console {}
}
```

<ChangeLog />
