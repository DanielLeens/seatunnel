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

import ChangeLog from "../changelog/connector-http-care-quality-commission.md";

# Care Quality Commission

`CareQualityCommission` 源在 `BATCH` 模式下读取英格兰护理质量委员会（CQC）的公共 Syndication API。在 CQC 开发者门户中订阅该 API，并将主订阅密钥配置为 `token`。连接器通过 HTTPS 向 `api.service.cqc.org.uk` 发送 `Ocp-Apim-Subscription-Key` 请求头。[开发者门户](https://api-portal.service.cqc.org.uk/)和 [CQC 数据使用指南](https://www.cqc.org.uk/about-us/transparency/using-cqc-data)。

| 资源 | `/public/v1` 下的 GET 路径 | 记录 ID |
| --- | --- | --- |
| `inspection_areas` | `/inspection-areas` | `inspectionAreaId` |
| `locations` | `/locations` | `locationId` |
| `providers` | `/providers` | `providerId` |
| `provider_locations` | `/providers/{providerId}/locations` | `organisationId` 或 `locationId` |
| `locations_detailed` | `/locations/{locationId}` | `locationId` |
| `providers_detailed` | `/providers/{providerId}` | `providerId` |

服务地点和提供者列表通过 `page` 与 `perPage` 从第 1 页开始读取。选择子资源时，连接器会自动获取其父记录。需要父记录的摘要时，须同时选择相应的父资源。API 每日更新，包含有效和无效记录；缺失或未发布的评级按原始响应保留。

## 输出字段

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `resource` | STRING | 资源名 |
| `id` | STRING | 记录的原生 ID |
| `provider_id` | STRING | 关联提供者 ID。对提供者记录，该值为其自身 ID |
| `location_id` | STRING | 服务地点记录的 ID |
| `record` | STRING | 原生 JSON 记录，保留嵌套信息和显式 null |

使用 `resource` 和 `id` 区分资源。持久化提供者与服务地点的关系时，可将 `provider_id` 纳入标识。

CQC 依据开放政府许可提供这些数据。再次发布或展示时，请注明 CQC 是数据来源。[CQC 来源说明](https://www.cqc.org.uk/about-us/transparency/using-cqc-data)。

## 参数

| 参数 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `token` | String | 是 | | CQC Syndication 主订阅密钥 |
| `resources` | 字符串列表 | 否 | `[]` | 资源表中的名称，空列表表示全部六项 |
| `tls_trust_certificate` | String | 否 | | 源连接器专用的 PEM 或 base64 DER X.509 信任证书。使用 TLS 1.2 或更新版本，并始终验证主机名 |
| `page_size` | Integer | 否 | `1000` | 服务地点和提供者列表的每页请求数，范围 1 至 1000 |
| `max_pages_per_collection` | Integer | 否 | `10000` | 单个集合的分页上限，范围 1 至 100000 |
| `max_pages_per_scan` | Integer | 否 | `500000` | 单次扫描接受的响应上限，范围 1 至 1000000 |
| `max_unpaginated_items` | Integer | 否 | `10000` | 完整列表的记录数上限，范围 1 至 100000 |
| `connect_timeout_ms` | Integer | 否 | `10000` | TCP 连接超时，范围 100 至 300000 毫秒 |
| `request_timeout_ms` | Integer | 否 | `30000` | 单次 HTTPS 请求期限，范围 100 至 300000 毫秒 |
| `operation_timeout_ms` | Integer | 否 | `120000` | 单次读取的全部请求、重试和等待的总期限。至少等于 `request_timeout_ms`，最多 600000 毫秒 |
| `request_interval_ms` | Integer | 否 | `0` | HTTPS 调用的最短间隔，范围 0 至 60000 毫秒 |
| `max_retries` | Integer | 否 | `3` | 可恢复失败后的额外尝试次数，范围 0 至 8 |
| `max_retry_delay_ms` | Integer | 否 | `60000` | 允许的最大重试等待，范围 250 至 600000 毫秒 |
| `max_response_bytes` | Integer | 否 | `8388608` | 传输及解压后响应体的字节上限，范围 256 至 16777216 |

## 恢复

一个读取器按顺序读取所选资源，并为详情请求保留当前父记录位置。检查点保存已确认页面的摘要和扫描位置。恢复前校验订阅密钥作用域、资源选择与页大小，并核验需要重读的已保存页面。若某对象在读取详情前已消失，作业会保留 HTTP 404 诊断。检查点可恢复相同配置下的扫描，CQC API 的数据仍可能在请求间更新。

## 示例

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  CareQualityCommission {
    token = "${CQC_SUBSCRIPTION_KEY}"
    resources = ["providers", "provider_locations", "providers_detailed"]
  }
}
sink {
  Console {}
}
```

<ChangeLog />
