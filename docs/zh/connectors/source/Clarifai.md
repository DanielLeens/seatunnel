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

import ChangeLog from '../changelog/connector-http-clarifai.md';

# Clarifai

Clarifai Source 通过 Clarifai v2 API 读取已有资源的列表。每个有界作业读取一种资源，Source 并行度为 1。

| 资源 | 范围和返回内容 |
| --- | --- |
| `applications` | 指定用户或组织的应用。 |
| `datasets` | 各应用的数据集。 |
| `models` | 各应用的模型。 |
| `model_versions` | 各应用内每个模型的版本。 |
| `workflows` | 各应用的流程。历史拼写 `worklows` 可作别名。 |
| `app_inputs` | 各应用的输入记录，媒体与链接字段原样输出。 |
| `app_inputs_jobs` | 已存在的输入添加任务及其进度和状态。 |
| `app_concepts` | 各应用的概念定义。 |

输出有六个字符串列，分别为 `id`、`user_id`、`app_id`、`model_id`、`cursor_timestamp` 和 `data`。前两项是资源 ID 和配置的应用归属者，父级 ID 来自请求路径。应用记录的 `app_id` 为空，只有模型版本记录填写 `model_id`。同名资源可以通过这些范围列区分。`data` 保存完整 JSON，包括嵌套状态、未知字段、明确的 null 和精确数值。连接器只读取列表，不下载媒体链接或修改供应商数据。

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `token` | 必填 | 具有对应资源读取权限的个人访问令牌，以 `Authorization: Key <token>` 发送。 |
| `user_id` | 必填 | 要读取的 Clarifai 应用归属者。 |
| `resource` | 必填 | 选择上表中的一种资源。 |
| `page_size` | 随资源而定 | 可选，范围 1 到 1000。应用、输入、输入添加任务和概念默认 20，其余默认 100。显式值也用于隐藏的父级分页。 |
| `start_date` | 未配置 | 应用、数据集或模型按 `modified_at` 过滤的下界，包含边界。使用带偏移的 ISO 8601 时间。 |
| `end_date` | 首次读取时间 | 可选的时间过滤上界，包含边界。 |
| `incremental_start` | 未配置 | 后续作业可提供上一轮的游标。使用前要设置 `start_date`。 |
| `lookback_seconds` | `1` | 从上一轮游标回读的秒数，最早回到 `start_date`。 |
| `max_pages` | `10000` | 作业内可接受的列表页总数，包含父级应用和模型分页。 |
| `connect_timeout_ms` | `10000` | TCP 连接超时。 |
| `read_timeout_ms` | `30000` | 套接字读取空闲超时。 |
| `request_deadline_ms` | `60000` | 单次请求含等待和重试的总时间上限。 |
| `max_attempts` | `4` | 符合重试条件的 HTTP 或供应商临时状态的总尝试数。 |
| `max_response_bytes` | `8388608` | 单次响应的网络字节数和解压后字节数上限。 |
| `tls_trust_certificate` | JVM 信任库 | 可选的引擎本地 PEM 或 DER 信任证书绝对路径，仍校验 API 主机名。 |

支持时间过滤的三种资源在有 `modified_at` 时把它转换成 UTC 游标。缺少时间的记录仍会输出，游标为空。后续作业可以把上一轮最新的已知游标设为 `incremental_start`。闭区间和回读窗口可能导致跨作业重复读。

GET 列表将 `page` 和 `per_page` 放在查询参数。短页表示当前列表结束。HTTP 成功后还需校验供应商数字状态码。官方 API 或客户端弃用警告只在返回了明确的列表时接受。路由或权限错误会使作业失败。请求只发往 `api.clarifai.com`，不跟随重定向。

发现父级时会遍历完整列表，选中资源的时间范围不会限制应用和模型父级。checkpoint 恢复时先校验当前应用、模型和结果页的摘要，再跳过已确认的记录。页面变化时作业会失败，避免跳过未知数据。远端列表可能在读取期间变化，无法跨页取得一致快照。

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  Clarifai {
    token = "<personal-access-token>"
    user_id = "example-user"
    resource = "datasets"
    start_date = "2026-01-01T00:00:00Z"
  }
}
sink {
  Console {}
}
```

## 更新日志

<ChangeLog />
