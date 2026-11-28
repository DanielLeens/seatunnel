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

import ChangeLog from '../changelog/connector-http-cart-com.md';

# CartCom

安装 `connector-http-cart-com` 后，可批量读取基于 AmeriCommerce 的 Cart.com Online Store REST API。每个 Source 配置块选择一个资源，源端并行度设为 `1`。 [Online Store API](https://github.com/AmeriCommerce/rest-api/blob/master/docs/README.md)

## 资源

| 资源 | 响应集合键 | 记录 |
| --- | --- | --- |
| `customers` | customers | 客户 |
| `orders` | orders | 订单 |
| `order_payments` | order_payments / payments | 订单支付记录 |
| `order_items` | order_items / items | 订单条目 |
| `order_statuses` | order_statuses | 订单状态，全量读取 |
| `products` | products | 商品 |
| `addresses` | addresses | 地址 |

两个订单接口同时兼容当前集合名和简写集合名。如果同一响应包含两个集合，两者的内容必须一致。

## 参数

| 参数 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `resource` | string | 必填 | 上表中的一个资源。 |
| `store_host` | string | 单店模式 | 商户 DNS 主机名，不含协议、端口或路径。 |
| `token` | string | 单店模式 | 为所选商户签发的 Cart.com 访问令牌。 |
| `site_id` | string | 中央路由 | 中央路由分配的站点 ID。 |
| `username` | string | 中央路由 | 已开通的中央路由用户名。 |
| `secret_key` | string | 中央路由 | 中央路由为该用户签发的签名密钥。 |
| `start_date` | string | `1970-01-01T00:00:00Z` | 有时间游标的资源的下界，包含该时刻。 |
| `end_date` | string | 首次读取时间 | 时间窗口上界，不含该时刻。恢复时沿用首次确定的值。 |
| `incremental_start` | string | 未设置 | 新批处理可选的上次读取高水位。 |
| `lookback_seconds` | int | `0` | 从高水位向前回读的秒数，最早至 start_date，范围 0–31536000。 |
| `page_size` | int | `1000` | 请求页大小，范围 1–1000。 |
| `max_pages` | int | `2000` | 允许读取的页数，含空页，范围 1–20000。 |
| `connect_timeout_ms` | int | `10000` | TCP 连接超时，毫秒，范围 1–120000。 |
| `read_timeout_ms` | int | `30000` | 套接字读取超时，毫秒，范围 1–120000。 |
| `request_deadline_ms` | int | `60000` | 请求和重试的总时限，毫秒，范围 1–300000。 |
| `max_attempts` | int | `4` | GET 最大尝试次数，包含首次请求，范围 1–10。 |
| `max_response_bytes` | int | `8388608` | 每个响应编码前后的字节上限，范围 1024–33554432。 |
| `tls_trust_certificate` | string | JVM 信任库 | Worker 本地 PEM 或 DER 信任锚文件的绝对路径。始终校验主机名。 |

## 鉴权

单店模式配置 `store_host` 和 `token`。Worker 只会向该 HTTPS 主机发送 `X-AC-Auth-Token`。令牌需要具备所选资源的读取权限，可以从管理后台获取，也可以由独立配置的 OAuth 应用签发。 [请求说明](https://support.americommerce.com/hc/en-us/articles/202836750-Making-API-Requests)

中央路由模式配置 `site_id`、`username` 和 `secret_key`。Worker 固定访问 HTTPS 主机 `public.americommerce.com`，以 HMAC-SHA256 签署每个请求的路径和编码后的查询参数。每个 Source 配置块选择一种鉴权方式。

## 分页与输出

六个资源通过 `updated_at` 读取时间窗口，包含 `start_date`，不包含 `end_date`。后续批次可将上次高水位作为 `incremental_start`，使用 `lookback_seconds` 向前回读，最早到 `start_date`。请求按 `updated_at,id` 排序，时间相同的多条记录仍在窗口内。跨批次回读重叠窗口可能重复输出部分记录。 `order_statuses` 全量扫描。若服务端返回缺少 `updated_at` 或该字段为空的记录，输出时间列为空。不带时区的 ISO 8601 时间按 UTC 处理。 [查询语法](https://github.com/AmeriCommerce/rest-api/blob/master/docs/query_syntax.md) [时间格式](https://github.com/AmeriCommerce/rest-api/blob/master/docs/datetimes.md)

| STRING 列 | 说明 |
| --- | --- |
| `id` | 转换为字符串的精确记录 ID。 |
| `updated_at` | 归一化后的 UTC 时间，或空值。 |
| `data` | 完整 JSON 记录，保留嵌套字段、空值及数值精度。 |

Source 只从 `next_page` 链接提取连续页号。后续请求沿用配置的主机、资源、排序、页大小及读取时段。Worker 校验 TLS 主机名、拒绝重定向，并将重试延迟限制在总时限内。检查点记录页号、原始偏移量、时间范围和页摘要。恢复时如果部分读取的页面发生变化，Source 会在继续输出前报错。已完成的分片恢复时无需重新发送请求。分页期间服务端可能发生更新，同一批次可能读到期间发生的变化。 [分页说明](https://support.americommerce.com/hc/en-us/articles/202524814-API-Quick-Start)

## 示例

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  CartCom {
    resource = "orders"
    store_host = "store.example.com"
    token = ${CART_STORE_TOKEN}
    start_date = "2025-01-01T00:00:00Z"
  }
}
sink {
  Console {}
}
```

<ChangeLog />
