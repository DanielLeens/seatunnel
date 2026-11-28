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

# CastorEdc

## 描述

Castor EDC Source 通过区域 Castor EDC/CDMS API 有界读取研究元数据、用户记录和审计事件。连接器使用所选区域的 OAuth 客户端凭据，遵守该凭据的既有访问权限。

## 资源

以下路径以区域源地址下的 `/api` 为基准。

| 资源 | GET 路径 | 输出 |
| --- | --- | --- |
| `user` | `/user` | 凭据可见的用户 |
| `study` | `/study` | 可见的研究 |
| `audit_trial` | `/study/{study_id}/audit-trail` | 指定时间窗口内的审计事件 |
| `country` | `/country` | 国家参考数据 |
| `study_survey` | `/study/{study_id}/survey` | 问卷定义 |
| `study_survey_package` | `/study/{study_id}/survey-package` | 问卷包定义 |
| `study_site` | `/study/{study_id}/site` | 研究中心 |
| `study_fields` | `/study/{study_id}/field` | 研究字段定义 |
| `study_field_dependency` | `/study/{study_id}/field-dependency` | 字段依赖 |
| `study_field_validation` | `/study/{study_id}/field-validation` | 字段验证规则 |
| `study_form` | `/study/{study_id}/form` | 研究表单 |
| `study_role` | `/study/{study_id}/role` | 研究角色 |
| `study_fieldoption_groups` | `/study/{study_id}/field-optiongroup` | 字段选项组 |
| `study_statistics` | `/study/{study_id}/statistics` | 每项研究的单个统计对象 |
| `study_user` | `/study/{study_id}/user` | 分配到研究的用户 |
| `study_visit` | `/study/{study_id}/visit` | 访视定义 |

选择任一研究子资源后，即使未选择输出 `study`，或研究的创建时间未落在输出窗口内，连接器仍会发现凭据可见的研究并遍历其子资源。权限不足时，作业报出资源名和 HTTP 状态后停止。供应商当前将审计接口标为 Beta，其内容与历史范围受供应商的权限和保存规则约束。

## 配置

| 参数 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `auth_client_id` | string | 是 | | Castor OAuth 客户端 ID。 |
| `auth_client_secret` | string | 是 | | Castor OAuth 客户端密钥。 |
| `resources` | list of strings | 是 | | 上表中至少一种资源，名称不可重复。 |
| `url_region` | string | 否 | `uk` | `uk`、`us`、`au`、`eu` 或 `nl`，后两项使用 `data.castoredc.com`。 |
| `start_date` | string | 选择 `audit_trial` 时必填 | | 含下界，使用带 UTC 偏移量的 ISO-8601 时间戳。 |
| `end_date` | string | 否 | | 不含上界，使用带 UTC 偏移量的 ISO-8601 时间戳。只配置起点时，将扫描起始时间固定为上界。 |
| `legacy_timezone` | string | 否 | `UTC` | API 未提供时区或偏移量时采用的 IANA 时区或 UTC 偏移量。 |
| `audit_page_size` | integer | 否 | | 审计每页请求条数，范围 1–1000。省略时采用供应商默认值。 |
| `request_interval_ms` | integer | 否 | `1100` | 同一读取器两次请求开始的最小间隔，包含 OAuth 和重试。范围 1000–60000。 |
| `max_studies` | integer | 否 | `10000` | 子资源遍历最多保留的研究 ID，范围 1–50000。 |
| `max_requests` | integer | 否 | `100000` | 单次扫描允许处理成功的逻辑响应上限，包含研究发现。范围 1–1000000。 |
| `max_attempts` | integer | 否 | `4` | 每次数据读取或令牌交换的最大尝试次数，范围 1–10。 |
| `request_timeout_ms` | integer | 否 | `30000` | 获取单个数据响应的总时间预算，包含限速等待、OAuth 和重试。范围 100–650000。 |
| `max_response_bytes` | integer | 否 | `8388608` | 单次数据响应解码后的字节数上限，范围 1024–67108864。 |
| `endpoint` | string | 否 | 区域 HTTPS 源 | 自定义源时只填写协议、主机及可选端口，同时提供 `trust_certificate`。 |
| `trust_certificate` | string | 否 | | 自定义源的 PEM 信任证书路径，需在执行节点可读。仍校验证书主机名。 |

时间窗口作用于研究的 `created_on`、用户与研究用户的 `last_login`，以及审计事件时间。未设置日期过滤时，保留从未登录的用户记录。审计接口接收日期参数，连接器将日期向窗口两端各扩展一天，再在本地精确过滤时间戳。如果无偏移量的本地时间在给定时区内无法唯一解析，作业会明确失败。

连接器向当前区域的 `/oauth/token` 获取令牌，将在后续请求前刷新即将过期的令牌，并在满足重试条件的首次 HTTP 401 后刷新一次。重试等待遵守响应中的 `Retry-After`。HTTP 429 未提供该响应头时，使用 Castor 文档给出的十分钟默认窗口。如果等待超过当页剩余预算，作业会失败。连接器按单个读取器限速，Castor 的配额仍对共用客户端凭据的所有调用生效。

## 输出和检查点恢复

| 列 | 类型 | 说明 |
| --- | --- | --- |
| `resource` | string | 所选资源名。 |
| `record_id` | string | API 记录 ID。没有 ID 的角色使用角色名。没有 ID 的审计事件使用内容摘要，相同事件可能得到同一值。 |
| `study_id` | nullable string | 研究和研究子资源的大写 UUID。 |
| `record_time` | nullable string | 带时间戳资源的精确 UTC 时间。 |
| `payload` | string | 完整 JSON 记录，保留嵌套结构、空值和数值精度。 |

连接器校验后续页仍位于同一区域源和相同的研究路径，并保持固定查询条件。检查点保留固定窗口、研究遍历进度、当前页和已输出位置。恢复部分输出页时，先校验记录和后续页信息，再继续输出。更换读取范围时需新建扫描。结果分页可能在扫描期间变化，API 不提供跨页事务快照或删除事件。

## 示例

```hocon
env {
  job.mode = "BATCH"
}

source {
  CastorEdc {
    auth_client_id = ${CASTOR_CLIENT_ID}
    auth_client_secret = ${CASTOR_CLIENT_SECRET}
    url_region = "eu"
    resources = ["study", "study_fields", "study_statistics"]
    plugin_output = "castor"
  }
}

sink {
  Console {
    plugin_input = "castor"
  }
}
```

参见 [Castor EDC API](https://data.castoredc.com/api)、[区域、OAuth 和权限](https://helpdesk.castoredc.com/hc/en-us/articles/27149233860509-Castor-EDC-CDMS-Application-Programming-Interface-API)、[审计接口](https://helpdesk.castoredc.com/hc/en-us/articles/27127424188317-Audit-trail-in-EDC-CDMS)及 [API 使用规则](https://helpdesk.castoredc.com/hc/en-us/articles/28549477683741-API-Developer-Policy)。
