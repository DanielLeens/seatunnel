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

import ChangeLog from '../changelog/connector-http-chroma.md';

# Chroma

Chroma Sink 通过 Chroma v2 HTTP API 将 SeaTunnel 数据行写成文档、元数据和向量。租户和数据库应提前建好，写入时可以复用已有集合或创建新集合。

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `endpoint` | 必填 | Chroma 服务的 HTTP(S) 地址，可携带反向代理路径前缀。 |
| `tenant`、`database` | `default_tenant`、`default_database` | 已有租户和数据库。使用 Chroma Cloud 时请填写账户对应的信息。 |
| `collection` | 必填 | Chroma 集合名，目标不存在时创建。 |
| `token` | 空 | Chroma Cloud API 令牌，要求 HTTPS。 |
| `username`、`password` | 空 | 支持 Basic 的自建服务或反向代理凭证，应同时配置两项，并使用 HTTPS 保护凭证。 |
| `tls_trust_certificate` | JVM 信任库 | 可选的引擎本地绝对路径，支持 PEM 或 DER X.509 信任证书。对配置的 Chroma 与向量服务生效，仍验证主机名。 |
| `mode` | `append` | `append`、`overwrite` 或 `append_dedup`。 |
| `namespace`、`stream` | 输入库名和表名 | 固定的文档范围，输入没有库名时命名空间为空。 |
| `namespace_field`、`stream_field` | 空 | 通过字符串字段动态取值，使用确定的字段路径。覆盖模式要求固定范围。 |
| `primary_keys` | 空列表 | 去重写入必填。按顺序给出非空标量主键字段的确定路径。 |
| `text_fields`、`metadata_fields` | 输入行全部字段 | 使用点分字段路径，支持数组下标和通配符。缺失的字段路径会跳过。 |
| `field_name_mappings` | 空映射 | 将匹配的文本或元数据字段路径映射为目标名称。 |
| `omit_raw_text` | `false` | 生成向量后，将文档正文保存为空字符串。 |
| `splitter` | `separator` | 可选字面分隔符 `separator`、Markdown 标题 `markdown` 或代码结构 `code`。 |
| `separators` | `["\n\n", "\n", " ", ""]` | 按顺序尝试的字面分隔符。 |
| `keep_separator` | `false` | 将字面分隔符留在下一个分块边界。 |
| `markdown_level` | `1` | 优先用于切分的最深标题层级，范围 1 到 6。 |
| `code_language` | 空 | 选择 `code` 时必填，例如 `java`、`javascript`、`python` 或 `go`。 |
| `chunk_size`、`chunk_overlap` | `400`、`0` | 每块的 GPT-2 token 上限和相邻分块最多重复的 token 数。 |
| `tokenizer_data_file` | 空 | 可选的本地 GPT-2 `r50k_base.tiktoken` 排名表，校验 SHA-256。留空时首次需要分词时下载固定版本的官方文件。 |
| `embedding_mode` | 必填 | 向量生成方式，见下表。 |
| `embedding_url` | 适用时使用供应商默认值 | Azure 地址或 OpenAI 兼容 API 基础地址。OpenAI 和 Cohere 使用各自的官方主机名。携带向量服务凭证时要求 HTTPS。 |
| `embedding_secret_key` | 空 | OpenAI、Azure OpenAI 和 Cohere 必填，OpenAI 兼容服务可选。与目标库凭证独立。 |
| `embedding_model` | `text-embedding-ada-002` | OpenAI 兼容服务的模型名。 |
| `embedding_deployment` | 空 | Azure OpenAI 模式必填，指定部署名。 |
| `embedding_field` | 空 | `from_field` 模式必填，指定向量字段的确定路径。 |
| `embedding_dimensions` | `0` | `from_field` 和 `openai_compatible` 要求填写 1 到 4096。其他固定维度模式可保留 0。 |
| `batch_size` | `128` | 单次向量请求的文档上限，范围 1 到 128。服务批量上限较小时请调低。 |
| `max_row_bytes`、`max_buffer_bytes`、`max_body_bytes` | `262144`、`4194304`、`4194304` | 单行序列化、待写内容估算、JSON 请求或响应的字节限制。 |
| `max_chunks_per_row` | `100` | 单行最大分块数。 |
| `connect_timeout_ms`、`read_timeout_ms` | `10000`、`15000` | 套接字连接超时和读取空闲超时。 |
| `request_timeout_ms`、`flush_timeout_ms`、`max_attempts` | `30000`、`120000`、`3` | 单次操作总时间、单批写出总时间和符合条件的重试次数。不跟随重定向。 |

| 模式 | 向量规则 |
| --- | --- |
| `from_field` | 每个分块复用原行向量。数组的值必须是有限数值，维度必须与配置相等。 |
| `openai` | 使用 `text-embedding-ada-002`，维度 1536。 |
| `azure_openai` | 通过 Azure OpenAI v1 API 请求已部署的模型，维度 1536。 |
| `cohere` | 使用 `embed-english-v3.0`，维度 1024。集合若已有停用的 v2 模型向量，需要重新生成向量后再混合数据。 |
| `openai_compatible` | 向兼容 OpenAI 向量响应格式的服务发送配置的模型名。 |
| `fake` | 测试用的可复现合成向量，维度 1536。 |

元数据中的字符串、布尔值和可表示的数值保持标量类型，数组、对象及明确的 null 序列化为 JSON 字符串。连接器保留可读的 `_ab_stream`、精确范围哈希 `_st_scope` 和去重键 `_ab_record_id`。文本从选定的字段名和值生成，按完整 Unicode 字符切分。边界输入可能产生与其他分词器不同的分块。

追加模式支持并行写入，保留不同运行产生的数据行。覆盖模式只清除属于当前命名空间和流且带精确标记的记录，再写入新内容。去重模式会替换同一范围内该主键的所有分块，并处理 DELETE、UPDATE_BEFORE 以及输入中的 `_ab_cdc_deleted_at` 删除标记。未带范围标记的数据保持原样。覆盖和去重需要并行度 1，请避免多个作业同时写相同范围。

写入结果在 checkpoint 提交前可见。checkpoint 成功时会冲刷批次，保存集合 UUID、写入序号和覆盖进度。连接器会报告结果未知的写入，避免自动重放。若 checkpoint 恢复时输入相同，可按稳定的分块 ID 重新 upsert。删除和随后的 upsert 是两次独立操作，中断期间文档可能暂时缺失，无法提供端到端 Exactly-once 可见性。当前支持服务端地址和显式向量，暂不支持嵌入式本地存储和进程内 MiniLM 向量生成。

```hocon
sink {
    Chroma {
        endpoint = "http://chroma:8000"
        collection = "product-documents"
        namespace = "catalog"
        stream = "descriptions"
        mode = "append_dedup"
        primary_keys = ["product_id"]
        text_fields = ["title", "description"]
        metadata_fields = ["product_id", "category"]
        embedding_mode = "from_field"
        embedding_field = "vector"
        embedding_dimensions = 384
    }
}
```

## 更新日志

<ChangeLog />
