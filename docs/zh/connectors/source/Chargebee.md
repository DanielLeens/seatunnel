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

import ChangeLog from '../changelog/connector-http-chargebee.md';

# Chargebee

安装 `connector-http-chargebee` 后，可批量读取 Chargebee API v2。每个 Source 配置块选择一个资源，源端并行度设为 `1`。`site` 指定 `https://<site>.chargebee.com/api/v2` 的 DNS 标签。鉴权使用站点 API key 作为 Basic 用户名，密码为空。该 key 需要具备所选资源及父集合的读取权限。 [Chargebee API](https://apidocs.chargebee.com/docs/api/getting-started)

## 资源

| 资源 | 目录 | 父资源 | 游标 | 读取方式 |
| --- | --- | --- | --- | --- |
| `addon` | 1.0 | — | `updated_at` | UTC 日历月时间窗口 |
| `attached_item` | 2.0 | `item` | `updated_at` | 每个 item 使用一个时间窗口 |
| `customer` | 1.0 和 2.0 | — | `updated_at` | UTC 日历月时间窗口 |
| `coupon` | 1.0 和 2.0 | — | `updated_at` | UTC 日历月时间窗口 |
| `contact` | 1.0 和 2.0 | `customer` | — | 全量读取 |
| `credit_note` | 1.0 和 2.0 | — | `updated_at` | UTC 日历月时间窗口 |
| `event` | 1.0 和 2.0 | — | `occurred_at` | UTC 日历月时间窗口 |
| `gift` | 1.0 和 2.0 | — | `updated_at` | 列出资源后在本地过滤时间 |
| `order` | 1.0 和 2.0 | — | `updated_at` | UTC 日历月时间窗口 |
| `hosted_page` | 1.0 和 2.0 | — | `updated_at` | UTC 日历月时间窗口 |
| `invoice` | 1.0 和 2.0 | — | `updated_at` | UTC 日历月时间窗口 |
| `item` | 2.0 | — | `updated_at` | UTC 日历月时间窗口 |
| `item_price` | 2.0 | — | `updated_at` | UTC 日历月时间窗口 |
| `plan` | 1.0 | — | `updated_at` | UTC 日历月时间窗口 |
| `payment_source` | 1.0 和 2.0 | — | `updated_at` | UTC 日历月时间窗口 |
| `promotional_credit` | 1.0 和 2.0 | — | `created_at` | UTC 日历月时间窗口 |
| `subscription` | 1.0 和 2.0 | — | `updated_at` | UTC 日历月时间窗口 |
| `subscription_with_scheduled_changes` | 1.0 和 2.0 | `subscription` | — | 全量读取 |
| `transaction` | 1.0 和 2.0 | — | `updated_at` | UTC 日历月时间窗口 |
| `unbilled_charge` | 1.0 和 2.0 | — | `updated_at` | 列出资源后在本地过滤时间 |
| `virtual_bank_account` | 1.0 和 2.0 | — | `updated_at` | UTC 日历月时间窗口 |
| `quote` | 1.0 和 2.0 | — | `updated_at` | UTC 日历月时间窗口 |
| `quote_line_group` | 1.0 和 2.0 | `quote` | — | 全量读取 |
| `site_migration_detail` | 1.0 和 2.0 | — | `migrated_at` | 列出资源后在本地过滤时间 |
| `comment` | 1.0 和 2.0 | — | `created_at` | UTC 日历月时间窗口 |
| `item_family` | 2.0 | — | `updated_at` | UTC 日历月时间窗口 |
| `differential_price` | 2.0 | — | `updated_at` | 列出资源后在本地过滤时间 |

`product_catalog` 选择该站点在 API v2 下的商品目录版本。如果资源在所选版本不可用，配置阶段会失败。两版都支持的资源仍可能依赖站点功能及权限。 [商品目录和 API 版本](https://apidocs.chargebee.com/docs/api/versioning)

## 参数

| 参数 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `site` | string | 必填 | Chargebee 站点的 DNS 标签，不含 .chargebee.com。 |
| `token` | string | 必填 | 为该站点签发的 Chargebee API key。 |
| `resource` | string | 必填 | 选择一个已支持的资源。 |
| `product_catalog` | string | `2.0` | 站点的商品目录版本，1.0 或 2.0。 |
| `start_date` | string | 必填 | ISO 8601 初始下界，包含该秒，需为整秒。 |
| `end_date` | string | 首次读取时间 | ISO 8601 读取上界，包含该秒。 |
| `incremental_start` | string | 未设置 | 后续批次可传入上次游标时间。 |
| `lookback_seconds` | int | `300` | 相对上次游标回读的秒数。 |
| `page_size` | int | `100` | 分页请求行数，范围 1–100。 |
| `max_pages` | int | `10000` | 总页数上限，包含父页、空页和各日期窗口。 |
| `connect_timeout_ms` | int | `10000` | TCP 连接上限，单位毫秒。 |
| `read_timeout_ms` | int | `30000` | 套接字读取上限，单位毫秒。 |
| `request_deadline_ms` | int | `60000` | 单次 HTTPS 读取和重试总时限，单位毫秒。 |
| `max_attempts` | int | `4` | GET 最大尝试次数，包含首次请求。 |
| `max_response_bytes` | int | `8388608` | 每个响应编码及解压后的字节上限。 |
| `tls_trust_certificate` | string | JVM 信任库 | Worker 本地 PEM 或 DER 证书文件的绝对路径。 |

`max_pages` 上限 20000，`lookback_seconds` 上限 31536000，`max_attempts` 上限 11，`max_response_bytes` 上限 33554432。连接和读取超时范围为 1–120000 毫秒，总请求时限范围为 1–300000 毫秒。配置证书信任锚后，仍需通过 TLS 主机名校验。

## 时间窗口、分页与恢复

游标资源使用以秒为单位的 UTC 时间窗口，包含上下界。上界在首次读取时确定，恢复时沿用。按月读取的窗口从当前下界向后加一个日历月，下一窗口的起点比上一窗口的结束时间大一秒。附加商品在每个 item 下使用整个配置窗口。接口没有日期过滤参数的资源在列出后使用本地窗口过滤。服务端返回游标为空的记录时，仍保留该记录，输出时间为空。

后续批次可通过 `incremental_start` 传入上次高水位，按配置回看，最早到 `start_date`。重叠区间中的记录可能再次输出。Source 使用各资源文档支持的排序和删除记录参数，保留 API 返回的资源版本及删除标记。列表为最终一致，事件资源可读取该站点仍可访问的事件。 [列表操作](https://apidocs.chargebee.com/docs/api/list-ops)

`next_offset` 按原值作为不透明游标使用，即使它看起来像 JSON。Source 读完当前父资源下的全部子页，再移动到下一个父资源。父资源发现不使用子资源的日期过滤。排定变更的订阅详情使用单对象接口，只发现带有排定变更的订阅。如果订阅在查询时已消失，或服务端明确说明该订阅没有排定变更，则结束该单项查询。

检查点保留当前 UTC 窗口、父子游标、原始偏移和页摘要。恢复时当前父页和数据页需匹配原摘要，才会从相应位置继续。已完成的分片不再发送 HTTPS 请求。Source 在总时限内重试可重试的 HTTP 响应，并遵守 `Retry-After`。连接始终校验 HTTPS 主机名，并拒绝重定向。

## 输出

| STRING 列 | 说明 |
| --- | --- |
| `id` | 资源标识。迁移详情使用 `entity_id`，排定变更的订阅详情使用发起查询的订阅 ID。 |
| `parent_id` | 子资源所属父资源的标识，无父资源时为空。 |
| `cursor_timestamp` | 从资源游标字段提取的 UTC 时间，无值时为空。 |
| `data` | 完整 JSON 数据，保留数值精度、嵌套对象和空值。顶层 `cf_*` 字段会转换到 `custom_fields` 数组，每项包含 `name` 和 `value`。 |

联系人、报价行分组和排定变更的订阅详情还会根据请求范围，在 `data` 中分别提供 `customer_id`、`quote_id` 和 `subscription_id`。现有的 `custom_fields` 项继续保留，嵌套对象中的 `cf_` 字段名保持原样。

## 示例

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  Chargebee {
    site = "my-subscriptions"
    token = ${CHARGEBEE_API_KEY}
    resource = "invoice"
    product_catalog = "2.0"
    start_date = "2025-01-01T00:00:00Z"
  }
}
sink {
  Console {}
}
```

<ChangeLog />
