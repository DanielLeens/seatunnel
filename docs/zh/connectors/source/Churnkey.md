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

import ChangeLog from "../changelog/connector-http-churnkey.md";

# Churnkey

`Churnkey` 源在 `BATCH` 模式下通过 Churnkey Data API 读取取消流程的会话明细和分组计数。在 Churnkey 账户设置中获取 Data API key 和 App ID。使用 `token` 配置密钥，使用 `access_key` 配置 App ID。连接器通过 HTTPS 向 `api.churnkey.co` 发送 `x-ck-api-key` 和 `x-ck-app` 请求头。[Churnkey Data API](https://docs.churnkey.co/data-integrations/data-api/)。

| 资源 | GET 路径 | 响应 |
| --- | --- | --- |
| `sessions` | `/v1/data/sessions` | 使用 `limit` 和 `skip` 分页的会话数组 |
| `session-aggregation` | `/v1/data/session-aggregation` | 完整的分组计数数组 |

会话分页按实际返回的行数推进，短页后继续读取，空页表示列表结束。`bounced` 参数向两个接口传递同一原生筛选值。默认 `false` 会排除在 24 小时内出现更相关会话后，被 Churnkey 标记为 bounced 的会话。

`breakdown` 按配置顺序组合下列分组维度。`month`、`trial`、`billingInterval`、`planId`、`aborted`、`canceled`、`offerType`、`saveType`、`couponId`、`pauseDuration`、`blueprintId`、`abtest`、`surveyResponse`。

## 输出字段

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `resource` | STRING | 资源名 |
| `id` | STRING | 会话的原生 `_id`。分组计数记录为空 |
| `event_time` | STRING | 会话的 `createdAt` 转为 UTC，或分组返回的 `YYYY-MM` 月份。缺失时为空 |
| `record` | STRING | 原生 JSON 对象，包含嵌套会话信息，或分组计数及各维度值 |

## 参数

| 参数 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `token` | String | 是 | | Churnkey Data API key |
| `access_key` | String | 是 | | Churnkey App ID |
| `resources` | 字符串列表 | 否 | `[]` | 使用资源表中的名称，空列表读取两种资源 |
| `bounced` | Boolean | 否 | `false` | 会话的原生 bounced 筛选值 |
| `breakdown` | 字符串列表 | 否 | `[]` | 有顺序的聚合维度，不能重复 |
| `tls_trust_certificate` | String | 否 | | 源连接器专用的可选 PEM 或 base64 DER X.509 信任证书。始终校验 HTTPS 主机名 |
| `page_size` | Integer | 否 | `100` | 每页会话请求数，范围 1 至 10000 |
| `max_pages_per_collection` | Integer | 否 | `10000` | 接收会话页面的上限，范围 1 至 100000 |
| `max_pages_per_scan` | Integer | 否 | `100000` | 两种资源总计接收的响应上限，范围 1 至 1000000 |
| `max_unpaginated_items` | Integer | 否 | `10000` | 单次聚合响应的分组数上限，范围 1 至 100000 |
| `connect_timeout_ms` | Integer | 否 | `10000` | TCP 连接超时，范围 100 至 300000 毫秒 |
| `request_timeout_ms` | Integer | 否 | `30000` | 单次 HTTPS 请求期限，范围 100 至 300000 毫秒 |
| `operation_timeout_ms` | Integer | 否 | `120000` | 单次读取包含重试和等待的总期限。至少等于 `request_timeout_ms`，最多 600000 毫秒 |
| `request_interval_ms` | Integer | 否 | `0` | HTTPS 请求间的最短间隔，范围 0 至 60000 毫秒 |
| `max_retries` | Integer | 否 | `3` | 临时失败的额外尝试次数，范围 0 至 8 |
| `max_retry_delay_ms` | Integer | 否 | `60000` | 可接受的最大重试等待，范围 250 至 600000 毫秒 |
| `max_response_bytes` | Integer | 否 | `8388608` | 传输和解压后响应体的字节上限，范围 256 至 16777216 |

## 恢复

一个读取器按相同的 App 作用域读取所选资源。检查点保存资源、偏移量、行号和响应摘要。连接器会在恢复时校验需重读的响应，再从已保存行号继续。恢复使用相同的 App、密钥、资源、页大小、筛选和聚合设置。检查点不保存凭据和 JSON 记录。会话列表可能在两次请求间发生变化，检查点只能保留扫描位置。

## 示例

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  Churnkey {
    token = "${CHURNKEY_DATA_API_KEY}"
    access_key = "${CHURNKEY_APP_ID}"
    resources = ["sessions", "session-aggregation"]
    breakdown = ["month", "surveyResponse", "billingInterval"]
  }
}
sink {
  Console {}
}
```

<ChangeLog />
