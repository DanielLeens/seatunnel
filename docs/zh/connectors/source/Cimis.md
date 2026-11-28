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

# Cimis

## 描述

Cimis Source 通过加州水资源部的 CIMIS Web API 3.0 读取天气观测和参考数据。每次 HTTPS 请求都通过 `Ocp-Apim-Subscription-Key` 请求头发送订阅密钥。旧版 CIMIS API 已于 2026 年 11 月 16 日退役。

## 资源

| 资源 | 路径 | 数据 |
| --- | --- | --- |
| `stations` | `/StationWeb/GetAllStations` | 气象站参考信息，包括停用站点 |
| `spatial_zipcodes` | `/SpatialWeb/GetAllSpatialZipCodes` | 空间数据的邮编覆盖与状态 |
| `station_zipcodes` | `/StationWeb/GetAllStationsZipCodes` | 气象站与邮编的对应关系 |
| `data` | 根据目标类型选择 | 每日或每小时观测 |

读取观测时，指定一个目标类型，并选择该类型的一个或多个目标。

| `target_type` | 观测路径 | 目标值 |
| --- | --- | --- |
| `station_numbers` | `/StationWeb/GetDataByStationNumber` | 正整数站号，如 `17`。 |
| `zip_codes` | `/GeoStationWeb/GetDataByGeoStationZipCodes` | 五位邮编字符串。每日请求使用配置的数据源偏好，每小时请求使用气象站网络。 |
| `coordinates` | `/SpatialWeb/GetDataBySpatialCoordinates` | 十进制度数 `lat=...,lng=...`，只读取每日空间数据。 |
| `addresses` | `/SpatialWeb/GetDataByAddresses` | `addr-name=...,addr=...`，街道地址允许包含逗号，只读取每日空间数据。 |

## 参数

| 参数 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `access_key` | string | 是 | | 当前 CIMIS API 订阅密钥。 |
| `resources` | list of strings | 是 | | 从上表选择至少一种资源，名称不可重复。 |
| `target_type` | string | 选择 `data` 时必填 | | 观测目标类型。 |
| `targets` | list of strings | 选择 `data` 时必填 | | 同一类型的目标，1–100 个，不能重复。 |
| `start_date` | string | 选择 `data` 时必填 | | CIMIS 起始日，含当天，格式为 `yyyy-MM-dd`。 |
| `end_date` | string | 选择 `data` 时必填 | | CIMIS 结束日，含当天，格式为 `yyyy-MM-dd`。 |
| `unit_of_measure` | string | 否 | `E` | `E` 为英制，`M` 为公制。 |
| `zip_preference` | string | 否 | `SCS` | 邮编每日数据的首选数据源，`SCS` 或 `WSN`。 |
| `daily_data_items` | list of strings | 否 | | CIMIS 3.0 支持的每日指标。坐标和地址请求可使用 `day-asce-eto` 和 `day-sol-rad-avg`。 |
| `hourly_data_items` | list of strings | 否 | | CIMIS 3.0 支持的气象站每小时指标。 |
| `slice_days` | integer | 否 | `31` | 每个目标最初切片的日历天数，范围 1–31。 |
| `max_requests` | integer | 否 | `100000` | 单次扫描的成功响应和容量拆分次数上限，范围 1–1000000。 |
| `max_attempts` | integer | 否 | `4` | 每次请求允许尝试的次数，包含首次 GET，范围 1–10。 |
| `request_timeout_ms` | integer | 否 | `30000` | 单次请求加重试的整体时限，范围 100–300000。 |
| `max_response_bytes` | integer | 否 | `8388608` | 单次响应解码后字节数上限，范围 1024–67108864。 |
| `endpoint` | string | 否 | `https://et.water.ca.gov` | 自定义 HTTPS 源，只填写协议、主机和可选端口。必须同时设置 `trust_certificate`。 |
| `trust_certificate` | string | 否 | | 自定义源的 PEM 信任证书路径，执行节点需可读。仍校验证书主机名。 |

两个指标列表均省略时，请求供应商默认的每日数据。两个列表都提供时，连接器分别发起每日与每小时请求。配置项接受 3.0 文档中列出的指标。当前表已没有旧指标名 `day-eto` 和 `hly-eto`，请按所需的数据计算方法选择指标。

连接器逐个目标读取，每个日期切片均包含首尾日。如果 HTTP 400 响应以文档中定义的 `ERR2112` 容量错误开头，连接器会尝试缩小切片。切片已经输出数据或已缩至一天时，作业会报错。HTTP 429 重试在请求预算内遵守有效的 `Retry-After`。

## 输出和恢复

| 列 | 类型 | 说明 |
| --- | --- | --- |
| `resource` | string | 所选资源名称。 |
| `record_id` | string | 站号、邮编、站号与邮编组合，或天气观测标识的摘要。 |
| `target` | nullable string | 天气观测对应的规范化请求目标。 |
| `date` | nullable string | CIMIS 返回的日期。 |
| `hour` | nullable string | CIMIS 提供的小时值，保留 `2400`。 |
| `provider_name` | nullable string | 天气观测供应方名称。 |
| `provider_type` | nullable string | 气象站或空间数据供应方类型。 |
| `provider_owner` | nullable string | 天气观测供应方所有者。 |
| `payload` | string | 完整记录 JSON，保留原始数值、缺测值、质量标记和单位。 |

连接器保留 CIMIS 原始日期和小时，不做时区转换或跨日调整。检查点保存当前目标、日期范围、切片宽度和已输出位置。恢复时会先校验活跃切片，再输出剩余记录。CIMIS 可能修订历史观测数据，此 Source 不提供跨请求事务快照或变更流。

## 示例

```hocon
env {
  job.mode = "BATCH"
}

source {
  Cimis {
    access_key = ${CIMIS_ACCESS_KEY}
    resources = ["stations", "data"]
    target_type = "station_numbers"
    targets = ["17"]
    start_date = "2026-10-01"
    end_date = "2026-10-07"
    unit_of_measure = "M"
    daily_data_items = ["day-asce-eto", "day-sol-rad-avg"]
    plugin_output = "weather"
  }
}

sink {
  Console {
    plugin_input = "weather"
  }
}
```

参见 [CIMIS Web API 概览](https://cimis.water.ca.gov/web-api/overview)和 [当前 REST 文档](https://cimis.water.ca.gov/web-api/rest-api/latest)。
