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

import ChangeLog from "../changelog/connector-http-cin7.md";

# Cin7 Core

`Cin7` 源使用 [Cin7 Core V2 API](https://coreapi.cin7.com/) 在 `BATCH` 模式下读取库存、会计、客户和交易记录。将 API Application key 配置为 `token`，将对应 Account ID 配置为 `access_key`。连接器通过供应商指定的请求头将两项凭据发送到 `https://inventory.dearsystems.com/ExternalApi/v2/`。

## 资源

| 资源 | GET 路径 | 标识 |
| --- | --- | --- |
| `bank_accounts` | `ref/accountBank` | `AccountID` |
| `attribute_sets` | `ref/attributeset` | `ID` |
| `accounts` | `ref/account` | `Code` |
| `brands` | `ref/brand` | `ID` |
| `carriers` | `ref/carrier` | `CarrierID` |
| `customers` | `customer` | `ID` |
| `deals` | `reference/deals` | `ID` |
| `locations` | `ref/location` | `ID` |
| `products` | `product` | `ID` |
| `purchase_list` | `purchaseList` | `ID` |
| `purchases` | `advanced-purchase` | `ID` |
| `suppliers` | `supplier` | `ID` |
| `opportunities` | `crm/opportunity` | `ID` |
| `sale_list` | `saleList` | `SaleID` |
| `sales` | `sale` | `ID` |
| `taxes` | `ref/tax` | `ID` |
| `product_categories` | `ref/category` | `ID` |
| `product_family` | `productFamily` | `ID` |
| `leads` | `crm/lead` | `ID` |
| `product_availability` | `ref/productavailability` | `[ID, Location, Bin, Batch, ExpiryDate]` |

普通集合使用响应的记录总数和页码。Deals 每页最多 25 条，收到空页时完成。采购和销售明细使用对应的概要集合发现明细 ID。只选择明细时，连接器在内部读取父集合，只输出明细。两者都选择时，每个概要页只读取一次。

客户和供应商读取包含已停用记录。产品读取也包含已停用记录，并请求物料清单、库存移动、供应商、附件、自定义价格和补货级别。附件 URL 作为记录字段保留。采购和销售明细合并附加费用，销售明细包含交易。库存可用量沿用 API 默认库存筛选。

## 输出

| 列 | 类型 | 说明 |
| --- | --- | --- |
| `resource` | STRING | 资源名称 |
| `id` | STRING | 原生文本标识，或序列化的库存标识数组 |
| `parent_id` | STRING | 采购或销售明细对应的父概要 ID。集合记录为空 |
| `record` | STRING | 原生 JSON 对象，保留数值精度、空值、嵌套字段和源日期 |

库存标识缺失的成分在数组中保留为 null，空字符串保留原值。

## 选项

| 名称 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `token` | String | 是 | | API Application key，发送于 `api-auth-applicationkey` |
| `access_key` | String | 是 | | Account ID，发送于 `api-auth-accountid` |
| `resources` | 字符串列表 | 否 | `[]` | 使用资源表中的名称。空列表读取全部 20 项 |
| `tls_trust_certificate` | String | 否 | | 可选的源连接器专用 PEM 或 base64 DER X.509 信任证书。始终校验 HTTPS 主机名 |
| `page_size` | Integer | 否 | `100` | 每页请求的记录数，范围为 1 至 1000。Deals 最多 25 条 |
| `max_pages_per_collection` | Integer | 否 | `10000` | 集合接收页数的上限，范围为 1 至 100000 |
| `max_pages_per_scan` | Integer | 否 | `100000` | 接收集合页和明细响应的总上限，范围为 1 至 1000000 |
| `connect_timeout_ms` | Integer | 否 | `10000` | TCP 连接超时，范围为 100 至 300000 毫秒 |
| `request_timeout_ms` | Integer | 否 | `30000` | 单次 HTTPS 连接尝试的期限，范围为 100 至 300000 毫秒 |
| `operation_timeout_ms` | Integer | 否 | `120000` | 含重试和等待的单次读取总期限。至少等于 `request_timeout_ms`，最多 600000 毫秒 |
| `request_interval_ms` | Integer | 否 | `1000` | 当前读取器两次 HTTPS 尝试之间的最短间隔，范围为 0 至 60000 毫秒 |
| `max_retries` | Integer | 否 | `3` | 符合条件的临时读失败额外尝试数，范围为 0 至 8 |
| `max_retry_delay_ms` | Integer | 否 | `60000` | 可接受的最大重试等待，范围为 250 至 600000 毫秒 |
| `max_response_bytes` | Integer | 否 | `8388608` | 传输及解压后响应体的字节上限，范围为 256 至 16777216 |

读取器按应用配额控制请求间隔。使用同一个 key 的其他作业或外部应用共用配额。连接器有限重试 HTTP 429 和临时服务错误，并遵守落在配置上限内的有效 `Retry-After`。

## 恢复

由一个读取器遍历所选资源。检查点记录集合、页码、页内位置、待完成明细和响应指纹。恢复页内位置或请求待完成明细前，连接器校验保存的页面。恢复时须使用相同的 Account ID、Application key、资源集合和页大小。检查点中没有原始凭据或记录。

这是一次有界集合扫描。Cin7 Core 在两次请求间可能发生记录变更。当分页元数据或保存的响应与扫描位置冲突时，连接器会报告相应资源并使作业失败。

## 示例

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  Cin7 {
    token = "${CIN7_APPLICATION_KEY}"
    access_key = "${CIN7_ACCOUNT_ID}"
    resources = ["products", "product_availability", "sale_list", "sales"]
  }
}
sink {
  Console {}
}
```

<ChangeLog />
