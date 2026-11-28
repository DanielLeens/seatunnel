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

import ChangeLog from "../changelog/connector-http-cisco-meraki.md";

# Cisco Meraki

`CiscoMeraki` 源在 `BATCH` 模式下从全球站点 `https://api.meraki.com` 读取 Dashboard 和 Secure Connect。请将 `token` 配置为有权读取所选组织和接口的 Cisco Meraki Dashboard API key。Dashboard 使用 `Authorization: Bearer`；Secure Connect 数据中心接口使用 `X-Cisco-Meraki-API-Key`。Secure Connect 的组织访问还取决于产品授权。 [Cisco Meraki 认证](https://developer.cisco.com/meraki/api-v1/authorization/)。

## 资源

| 资源 | GET 路径 | `id` |
| --- | --- | --- |
| `organizations` | `/api/v1/organizations` | `id` |
| `datacenters` | `/api/secureConnect/v1/organizations/{organizationId}/datacenters` | 空 |
| `organization_networks` | `/api/v1/organizations/{organizationId}/networks` | `id` |
| `organization_network_settings` | `/api/v1/networks/{networkId}/settings` | 空 |
| `organization_devices` | `/api/v1/organizations/{organizationId}/wirelessController/overview/byDevice` | 空 |
| `organization_apiRequests` | `/api/v1/organizations/{organizationId}/apiRequests` | 空 |
| `organization_admins` | `/api/v1/organizations/{organizationId}/admins` | `id` |
| `organization_saml` | `/api/v1/organizations/{organizationId}/saml` | 空 |

选择子资源时，连接器在内部查找父组织。选择网络设置时还会查找所属网络。作业只输出已选资源。无线控制器每个 API 页面输出一个原生概览对象，保留 `items` 和元数据。Secure Connect 数据中心、SAML 和网络设置保留完整原生对象。

分页接口按照供应商的 `Link` 继续读取，短页和空页也遵循该规则。后续链接须保持相同接口、全球站点、页大小和时间条件。连接器支持全球站点接口，遇到重定向会报错。 [Cisco Meraki 站点说明](https://documentation.meraki.com/Platform_Management/Dashboard_Administration/Troubleshooting_and_Support/Troubleshooting/Meraki_Dashboard_URL_Update_-_PCI_Compliance)。

## 时间窗口

API 请求日志发送原生 `t0` 与 `t1`，保留 `ts` 在 UTC 窗口内的记录，包含两端。默认窗口为开始扫描前的 30 天。供应商提供最多 31 天的请求历史。日期配置也会在本地筛选管理员的 `lastActive`，未配置筛选时保留无活动时间的管理员。其他资源和父对象查找不受该窗口影响。 [API 请求日志](https://developer.cisco.com/meraki/api-v1/get-organization-api-requests/)。

## 输出

| 列 | 类型 | 说明 |
| --- | --- | --- |
| `resource` | STRING | 资源名 |
| `id` | STRING | 原生文本标识。原响应无标识时为空。 |
| `organization_id` | STRING | 所属组织。组织记录使用自身标识。 |
| `network_id` | STRING | 网络概要和设置的网络标识。组织级记录为空。 |
| `record` | STRING | 原生 JSON 对象，保留未识别的字段、空值和数值精度。 |

## 选项

| 名称 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `token` | String | 是 |  | Meraki Dashboard API key。 |
| `resources` | 字符串列表 | 否 | `[]` | 使用上表中的名称。空列表读取全部八项。 |
| `start_date` | String | 否 |  | 请求日志或管理员活动的 ISO 8601 起点，含边界。 |
| `end_date` | String | 否 |  | 请求日志或管理员活动的 ISO 8601 终点，含边界。省略时使用本次扫描开始的时间。 |
| `tls_trust_certificate` | String | 否 |  | 连接器专用的可选 PEM 或 base64 DER X.509 信任证书。始终校验 HTTPS 主机名。 |
| `page_size` | Integer | 否 | `100` | 分页请求的条数，范围 3 至 1000。 |
| `max_unpaginated_items` | Integer | 否 | `10000` | 单个响应的管理员数量上限，范围 1 至 100000。 |
| `max_pages_per_collection` | Integer | 否 | `10000` | 每个集合或父对象的最大页数，范围 1 至 100000。 |
| `max_pages_per_scan` | Integer | 否 | `100000` | 扫描接受的最大响应数，范围 1 至 1000000。 |
| `connect_timeout_ms` | Integer | 否 | `10000` | TCP 连接超时，范围 100 至 300000 毫秒。 |
| `request_timeout_ms` | Integer | 否 | `30000` | 每次 HTTPS 尝试的期限，范围 100 至 300000 毫秒。 |
| `operation_timeout_ms` | Integer | 否 | `120000` | 该次读取含重试和等待的总期限。至少为 `request_timeout_ms`，最多为 600000 毫秒。 |
| `request_interval_ms` | Integer | 否 | `100` | HTTPS 尝试的最短时间间隔，范围 0 至 60000 毫秒。 |
| `max_retries` | Integer | 否 | `3` | 临时错误的额外尝试次数，范围 0 至 8。 |
| `max_retry_delay_ms` | Integer | 否 | `60000` | 重试的最大等待时间，范围 250 至 600000 毫秒。 |
| `max_response_bytes` | Integer | 否 | `8388608` | 传输及解压后响应体的最大字节数，范围 256 至 16777216。 |

## 恢复

单个读取器遍历组织及所选子资源。检查点固定日期窗口及分页、页内位置。恢复时，读取器核验保存的组织、网络和当前响应，再继续处理。供应商数据变化可能使检查点失效。检查点中没有原始凭据或记录。恢复时请使用相同 API key、资源、日期和页大小。请求日志恢复窗口可能超出供应商的 31 天历史范围。

## 示例

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  CiscoMeraki {
    token = "${MERAKI_API_KEY}"
    resources = ["organizations", "organization_networks", "organization_network_settings"]
  }
}
sink {
  Console {}
}
```

<ChangeLog />
