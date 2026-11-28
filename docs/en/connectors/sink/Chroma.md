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

The Chroma sink writes SeaTunnel rows as documents, metadata and explicit vectors to the Chroma v2 HTTP API. It can create a collection or use an existing one in a provisioned tenant and database.

| Option | Default | Description |
| --- | --- | --- |
| `endpoint` | Required | Chroma HTTP(S) origin, optionally with an API reverse proxy path prefix. |
| `tenant`, `database` | `default_tenant`, `default_database` | Existing Chroma tenant and database. For Chroma Cloud, use the tenant and database from your account. |
| `collection` | Required | Chroma collection name. If absent on the server, the writer creates it. |
| `token` | Empty | Chroma Cloud API token. Requires HTTPS. |
| `username`, `password` | Empty | Optional Basic credentials for a supported self-hosted server or reverse proxy. Supply both. Use HTTPS to protect Basic credentials. |
| `tls_trust_certificate` | JVM trust store | Optional absolute engine-local PEM or DER X.509 trust anchor file for the configured Chroma and embedding origins. Hostname verification remains enabled. |
| `mode` | `append` | `append`, `overwrite` or `append_dedup`. |
| `namespace`, `stream` | Input database and table name | Fixed document scope. An absent input database yields an empty namespace. |
| `namespace_field`, `stream_field` | Empty | Literal paths to string-valued row fields for dynamic scope. `overwrite` requires a fixed scope. |
| `primary_keys` | Empty list | Required for `append_dedup`. Use literal paths to non-null scalar key components in order. |
| `text_fields`, `metadata_fields` | All row fields | Dot paths. Array indices and wildcard selectors are supported. Missing selected paths are omitted. |
| `field_name_mappings` | Empty map | Renames exact selected field paths before their values reach text or metadata. |
| `omit_raw_text` | `false` | Store an empty document string after generating the vector. |
| `splitter` | `separator` | Literal `separator`, heading-aware `markdown`, or structure-aware `code`. |
| `separators` | `["\n\n", "\n", " ", ""]` | Ordered literal fallbacks for separator splitting. |
| `keep_separator` | `false` | Keep a separator at the next chunk boundary. |
| `markdown_level` | `1` | Deepest Markdown heading level used as a preferred boundary, from 1 to 6. |
| `code_language` | Empty | Required for `code`, for example `java`, `javascript`, `python` or `go`. |
| `chunk_size`, `chunk_overlap` | `400`, `0` | GPT-2 token ceiling and maximum repeated context. |
| `tokenizer_data_file` | Empty | Optional local copy of the GPT-2 `r50k_base.tiktoken` ranks. The writer verifies SHA-256 and otherwise fetches the pinned official ranks on first need. |
| `embedding_mode` | Required | Explicit embedding method from the table below. |
| `embedding_url` | Provider default where available | Azure origin or OpenAI-compatible base. OpenAI and Cohere use their official hostname. Credentialed requests require HTTPS. |
| `embedding_secret_key` | Empty | Required for OpenAI, Azure OpenAI and Cohere. Optional for an OpenAI-compatible endpoint. Destination and embedding credentials are independent. |
| `embedding_model` | `text-embedding-ada-002` | Model name for an OpenAI-compatible endpoint. |
| `embedding_deployment` | Empty | Required Azure OpenAI deployment name. |
| `embedding_field` | Empty | Required literal row path for `from_field`. |
| `embedding_dimensions` | `0` | Set to 1–4096 for `from_field` and `openai_compatible`. Fixed modes infer their dimensions when this value is 0. |
| `batch_size` | `128` | Maximum documents per vector request, from 1 to 128. Lower this for a server with a smaller batch limit. |
| `max_row_bytes`, `max_buffer_bytes`, `max_body_bytes` | `262144`, `4194304`, `4194304` | Row serialization, buffered data estimate and JSON request or response byte limits. |
| `max_chunks_per_row` | `100` | Limits expansion during splitting. |
| `connect_timeout_ms`, `read_timeout_ms` | `10000`, `15000` | Socket connection and read inactivity limits. |
| `request_timeout_ms`, `flush_timeout_ms`, `max_attempts` | `30000`, `120000`, `3` | Total time per operation, total time per flush and attempts for eligible retries. Redirects are disabled. |

| Embedding mode | Vector contract |
| --- | --- |
| `from_field` | Uses an existing finite numeric array with exactly the configured dimension for every chunk in that row. |
| `openai` | Uses `text-embedding-ada-002` with 1536 dimensions. |
| `azure_openai` | Uses your 1536-dimensional deployment through the Azure OpenAI v1 API. |
| `cohere` | Uses `embed-english-v3.0` with 1024 dimensions. Collections with vectors from the retired v2 model require re-embedding before mixing data. |
| `openai_compatible` | Sends the configured model to an endpoint implementing the OpenAI embeddings response contract. |
| `fake` | Generates deterministic 1536-dimensional synthetic vectors for testing. |

The connector stores strings, booleans and supported numbers as scalar metadata. Arrays, objects and explicit null become JSON strings. It adds the readable `_ab_stream` label, a hashed `_st_scope` and, for deduplication, `_ab_record_id`. The connector owns these fields. Text is formed from the selected field names and values and split at Unicode boundaries. Corner cases can produce different chunk boundaries from other splitters.

`append` accepts parallel writers and keeps distinct rows from independent runs. `overwrite` deletes only records carrying the current namespace and stream's exact connector scope, then writes new records. `append_dedup` replaces all chunks for the same scoped primary key and handles delete and update-before events, including an incoming `_ab_cdc_deleted_at` marker. Records without the connector scope marker are left alone. Use one writer for these two modes and avoid concurrent jobs modifying the same scope.

Writes become visible before checkpoint completion. A successful checkpoint flushes and stores the collection UUID, writer sequence and overwrite fence. The connector surfaces ambiguous writes instead of retrying them automatically. Checkpoint replay can upsert stable chunk IDs for unchanged input. A deletion and subsequent upsert are separate operations, so interrupted replacement can temporarily leave a missing document. Complete exactly-once visibility is unavailable. The connector uses a server endpoint and explicit vectors. Embedded local storage and in-process MiniLM embeddings are currently unavailable.

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

## Changelog

<ChangeLog />
