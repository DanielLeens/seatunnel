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

# Chift

## 描述

Chift Source 读取一个 Chift 账号下的消费者、连接、同步定义和执行记录。连接器使用该账号的凭据发送 JSON 请求获取令牌，再通过 HTTPS 读取已授权的数据。

## 资源

| 资源 | GET 路径 | 范围 |
| --- | --- | --- |
| `consumers` | `/consumers` | 账号可见的消费者 |
| `connections` | `/consumers/{consumer_id}/connections` | 各消费者的活跃和非活跃连接 |
| `syncs` | `/syncs` | 账号配置的同步 |
| `executions` | `/syncs/{syncid}/executions` | 各同步的执行记录 |

即使输出未选择父资源，连接器仍先发现消费者和同步，再遍历相应的连接和执行记录。执行列表沿用 Chift 默认行为，排除预览执行。任一请求的父资源访问失败时，作业会报出资源名和 HTTP 状态后停止。

## 参数

| 参数 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `auth_client_id` | string | 是 | | Chift API 客户端 ID。 |
| `auth_client_secret` | string | 是 | | Chift API 客户端密钥。 |
| `auth_account_id` | string | 是 | | 与 API 凭据关联的账号 UUID。 |
| `resources` | list of strings | 是 | | 上表中至少一种资源，名称不可重复。 |
| `start_date` | string | 否 | | 执行 `start` 的含下界，使用带偏移量的 ISO-8601 时间戳。 |
| `end_date` | string | 否 | | 执行 `start` 的不含上界。只设置起点时，启动扫描的时间固定为上界。 |
| `legacy_timezone` | string | 否 | | API 返回的执行时间没有偏移量时，使用所配置的 IANA 时区或 UTC 偏移。 |
| `page_size` | integer | 否 | `50` | 执行列表每页请求条数。范围 1–500。 |
| `max_parents` | integer | 否 | `10000` | 单次保留的消费者或同步 ID 数量上限。范围 1–50000。 |
| `max_requests` | integer | 否 | `100000` | 每次扫描最多成功处理的数据响应数。范围 1–1000000。 |
| `max_attempts` | integer | 否 | `4` | 一次查询或令牌交换最多尝试的次数。范围 1–10。 |
| `request_timeout_ms` | integer | 否 | `30000` | 获取单个数据响应的总时限，包含令牌交换与重试。范围 100–300000。 |
| `max_response_bytes` | integer | 否 | `8388608` | 单次数据响应解码后的字节上限。范围 1024–67108864。 |
| `endpoint` | string | 否 | `https://api.chift.eu` | 可选的自定义 HTTPS 源，只填写协议、主机和可选端口。必须同时设置 `trust_certificate`。 |
| `trust_certificate` | string | 否 | | 自定义源的 PEM 信任证书路径，需在执行节点可读。仍校验证书主机名。 |

时间窗口精确比较执行记录返回的 `start`。Chift 执行列表文档提供日期查询参数，但未说明比较字段和边界规则。连接器完整分页后再按配置窗口过滤。如果执行时间没有偏移量，必须显式设置 `legacy_timezone`。当地时间在该时区内无法唯一解析时，作业会失败。

连接器使用 JSON `POST /token` 获取 bearer 令牌，并缓存到接近供应商声明的到期时间。符合重试条件的首次 HTTP 401 会触发一次续期。令牌和凭据不会进入检查点。重试在配置的预算内遵守响应的 `Retry-After`。如果令牌 POST 的结果因传输中断而无法确定，连接器会明确失败。

## 输出和恢复

| 列 | 类型 | 说明 |
| --- | --- | --- |
| `resource` | string | 所选资源名称。 |
| `record_id` | string | 保留原值的供应商标识符。 |
| `parent_id` | nullable string | 连接所属的消费者 UUID，或执行记录所属的同步 UUID。 |
| `started_at` | nullable string | 转为 UTC 的精确执行开始时间。 |
| `payload` | string | 完整记录 JSON，保留嵌套字段和数值精度。 |

连接器验证响应的页号、页大小与总数，并拒绝在相邻两页重复出现的执行 ID。检查点保留当前父项、页号、页内输出位置和冻结的时间上界。恢复时会先校验活跃页内容和元数据，再输出剩余记录。重新扫描可能看到不同数据，API 不提供跨页事务视图或删除事件。

## 示例

```hocon
env {
  job.mode = "BATCH"
}

source {
  Chift {
    auth_client_id = ${CHIFT_CLIENT_ID}
    auth_client_secret = ${CHIFT_CLIENT_SECRET}
    auth_account_id = ${CHIFT_ACCOUNT_ID}
    resources = ["consumers", "connections", "syncs", "executions"]
    start_date = "2026-01-01T00:00:00Z"
    end_date = "2026-02-01T00:00:00Z"
    plugin_output = "chift"
  }
}

sink {
  Console {
    plugin_input = "chift"
  }
}
```

参见 [Chift 认证](https://docs.chift.eu/home/authentication.md)、[执行列表](https://docs.chift.eu/api-reference/endpoints/syncs/get-executions-for-a-sync.md)和 [重试时间](https://docs.chift.eu/home/rate-limits.md)。
