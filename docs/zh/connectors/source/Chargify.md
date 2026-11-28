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

# Chargify

## 描述

Chargify Source 从 Maxio Advanced Billing 读取客户、订阅、发票、优惠券和旧版交易。连接器使用一个区域内的账单站点 API 密钥，只发起 HTTPS GET 请求。

## 资源

| 资源 | 路径 | 记录标识 |
| --- | --- | --- |
| `customers` | `/customers.json` | 客户 `id` |
| `subscriptions` | `/subscriptions.json` | 订阅 `id` |
| `invoices` | `/invoices.json` | 发票 `uid` |
| `coupons` | `/coupons.json` | 优惠券 `id` |
| `transactions` | `/transactions.json` | 旧版交易 `id` |

发票使用服务端列表接口提供的标准字段，包含总额。优惠券和发票的可见范围沿用账单站点的 API 默认值。只有使用 Legacy Statement Billing 的订阅才适合读取 `transactions`。基于发票的支付模型属于独立接口。旧版交易接口不可用时，作业会报出资源及 HTTP 状态。

## 参数

| 参数 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `access_key` | string | 是 | | Advanced Billing 站点 API 密钥。HTTP Basic 用户名为该密钥，密码为 `x`。 |
| `site` | string | 是 | | 单段站点 DNS 标签，不含后缀，连接器会转为小写。 |
| `resources` | list of strings | 是 | | 上表中至少一种资源，名称不可重复。 |
| `region` | string | 否 | `us` | `us` 使用 `<site>.chargify.com`，`eu` 使用 `<site>.ebilling.maxio.com`。 |
| `page_size` | integer | 否 | `200` | 每页请求条数，范围 1–200。 |
| `max_requests` | integer | 否 | `100000` | 单次扫描允许处理的成功页数上限，包含终止空页。范围 1–1000000。 |
| `max_attempts` | integer | 否 | `4` | 单页最大尝试次数，包含首次 GET。范围 1–10。 |
| `request_timeout_ms` | integer | 否 | `240000` | 单页读取和重试的总时间预算，范围 100–600000。单次 HTTP 尝试同时受 120 秒上限约束。 |
| `max_response_bytes` | integer | 否 | `8388608` | 解码后单次响应字节数上限。范围 1024–67108864。 |
| `endpoint` | string | 否 | 区域 HTTPS 源 | 可选的自定义源地址，只填写协议、主机和可选端口。必须同时提供 `trust_certificate`。 |
| `trust_certificate` | string | 否 | | 自定义 HTTPS 源的 PEM 信任证书文件，需在执行节点可读。仍校验证书主机名。 |

连接器按页号依次读取，直到返回空集合。收到 HTTP 429 且响应包含 `Retry-After` 时，重试至少等待该响应头指定的时间，否则等待两分钟。如果等待超出当页预算，作业会在发送下一次请求前失败。

## 输出和恢复

| 列 | 类型 | 说明 |
| --- | --- | --- |
| `resource` | string | 所选资源名称。 |
| `record_id` | string | API 返回的精确 ID，无数值截断。 |
| `site` | string | 转为小写的账单站点标签。 |
| `region` | string | 配置的区域代码，US 或 EU。 |
| `payload` | string | API 返回的完整记录 JSON，保留嵌套字段、空值和数值精度。 |

检查点保存页号，以及部分输出页的内容指纹和进度。恢复时，如果该页内容变化，作业会在输出其余记录之前失败。相邻两页如果包含同一组记录 ID，也会明确失败。更换读取范围时需新建扫描。API 页之间可能发生数据变动，因此扫描不提供事务快照或删除事件。

## 示例

```hocon
env {
  job.mode = "BATCH"
}

source {
  Chargify {
    access_key = ${CHARGIFY_ACCESS_KEY}
    site = "billing-sandbox"
    region = "us"
    resources = ["customers", "subscriptions", "invoices", "coupons"]
    plugin_output = "billing"
  }
}

sink {
  Console {
    plugin_input = "billing"
  }
}
```

参见 [区域与认证](https://developers.maxio.com/http/getting-started/how-to-get-started)、[发票列表](https://developers.maxio.com/http/advanced-billing-api/api-endpoints/invoices/list-invoices)、[Legacy Statement Billing 交易](https://developers.maxio.com/legacy/http/api-endpoints/legacy-transactions/list-transactions)和 [API 限流](https://developers.maxio.com/http/getting-started/about-the-api/error-handling-rate-limiting)。
