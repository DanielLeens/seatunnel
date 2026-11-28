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

import ChangeLog from "../changelog/connector-http-chameleon.md";

# Chameleon

`Chameleon` 源在 `BATCH` 模式下读取 Chameleon v3 API 的十种资源。使用 Chameleon 控制台提供的账户密钥。连接器通过 HTTPS 向 `api.chameleon.io` 发送 `X-Account-Secret` 请求头。[身份验证](https://developers.chameleon.io/concepts/authentication)。

| 资源 | `/v3` 下的 GET 路径 | 输出时间 |
| --- | --- | --- |
| `helpbars` | `/edit/searches` | `updated_at` |
| `segments` | `/edit/segments` | `updated_at` |
| `urls` | `/edit/urls` | `updated_at` |
| `url_groups` | `/edit/url_groups` | `updated_at` |
| `changes` | `/edit/changes` | `created_at` |
| `launchers` | `/edit/launchers` | `updated_at` |
| `tooltips` | `/edit/tooltips` | `updated_at` |
| `tours` | `/edit/tours` | `updated_at` |
| `surveys` | `/edit/surveys` | `updated_at` |
| `survey_responses` | `/analyze/responses` | `updated_at` |

HelpBar API 为每个账户返回一个完整列表。其他资源根据原生 `cursor.before` 继续扫描，短页和空页也适用。连接器读取每份已发现的微调查的回复，并请求完整用户及公司资料。选择 `surveys`，即可一起输出调查记录。[分页](https://developers.chameleon.io/concepts/pagination)和[微调查回复](https://developers.chameleon.io/apis/survey-responses)。

## 输出字段

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `resource` | STRING | 所选资源名 |
| `id` | STRING | 记录的原生 ID |
| `survey_id` | STRING | 调查和回复记录的调查 ID，其他资源为 null |
| `event_time` | STRING | 表中对应资源的时间，转为 UTC。未配置时间筛选时可以为空 |
| `record` | STRING | 原生 JSON 对象，保留嵌套数据和显式 null |

可通过 `start_date` 或 `end_date` 按时间筛选。连接器在读取后按记录时间判断是否输出，因此也能找到较早创建且在窗口内更新的记录。调查记录是否落在输出窗口内，不影响其回复的发现。配置时间范围后，如果所选资源的记录缺少对应时间，作业会报错。原始数据可能包含用户资料和调查自由文本。

## 参数

| 参数 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `token` | String | 是 | | Chameleon 账户密钥 |
| `resources` | 字符串列表 | 否 | `[]` | 使用资源表中的名称，空列表读取全部十种资源 |
| `start_date` | String | 否 | | 含时区偏移的 ISO 8601 时间，下界包含 |
| `end_date` | String | 否 | | 含时区偏移的 ISO 8601 时间，上界排除。只提供下界时，使用扫描创建时间作为上界 |
| `tls_trust_certificate` | String | 否 | | 源连接器专用的 PEM 或 base64 DER X.509 信任证书。始终校验 HTTPS 主机名 |
| `page_size` | Integer | 否 | `50` | 游标每页请求数，范围 1 至 500 |
| `max_pages_per_collection` | Integer | 否 | `10000` | 每个集合或每份调查回复的页数上限，范围 1 至 100000 |
| `max_pages_per_scan` | Integer | 否 | `100000` | 单次扫描接受的响应上限，范围 1 至 1000000 |
| `connect_timeout_ms` | Integer | 否 | `10000` | TCP 连接超时，范围 100 至 300000 毫秒 |
| `request_timeout_ms` | Integer | 否 | `30000` | 单次 HTTPS 请求期限，范围 100 至 300000 毫秒 |
| `operation_timeout_ms` | Integer | 否 | `120000` | 单次读取的全部尝试和等待期限。至少等于 `request_timeout_ms`，最多 600000 毫秒 |
| `request_interval_ms` | Integer | 否 | `1050` | HTTPS 调用的最短间隔，范围 0 至 60000 毫秒 |
| `max_retries` | Integer | 否 | `3` | 可恢复失败的额外尝试次数，范围 0 至 8 |
| `max_retry_delay_ms` | Integer | 否 | `60000` | 允许的最大重试等待，范围 250 至 600000 毫秒 |
| `max_response_bytes` | Integer | 否 | `8388608` | 传输及解压后响应体的字节上限，范围 256 至 16777216 |

遇到 429 时，连接器会同时考虑 Chameleon 的 `X-Retry-After`、`X-Ratelimit-Wait` 及标准 `Retry-After` 响应头。如果响应头均未提供正数等待时间，可以使用 JSON 响应的 `wait`。[限速规则](https://developers.chameleon.io/concepts/rate-limiting)。

## 恢复

一个读取器枚举资源并读取所选调查的回复。检查点保留续页值、页摘要、当前记录位置以及固定的时间上界。恢复前会重新验证需要回放的页，源连接器的检查点无需保存密钥或 JSON 记录。恢复使用同一密钥、资源范围、页大小和时间窗口配置。API 数据可能在两次调用间变化，检查点无法提供整个账户的同一时间点快照。

## 示例

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  Chameleon {
    token = "${CHAMELEON_ACCOUNT_SECRET}"
    resources = ["surveys", "survey_responses", "changes"]
    start_date = "2026-05-01T00:00:00Z"
    end_date = "2026-06-01T00:00:00Z"
  }
}
sink {
  Console {}
}
```

<ChangeLog />
