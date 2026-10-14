---
name: api-bronze-pipeline-builder
description: Scrape and parse API documentation (OpenAPI, Swagger, Redoc, RPC, HTML, or cURL) to recommend an extraction strategy and generate an enterprise-grade, pure Python Bronze-layer ingestion pipeline with explicit key-value mappings. Use when the user asks to extract data from an API, analyze API docs, map API endpoints/variables, or build an API data ingestion pipeline.
---
# API Bronze Pipeline Builder

Automate the design and implementation of enterprise-grade, pure Python API extraction pipelines targeting the Bronze layer of a Medallion architecture. Parse API documentation across diverse formats (REST, OData, RPC/form-urlencoded, GraphQL), map key-value parameters and response payloads, recommend the optimal ingestion pattern, and deliver production-ready extraction code.

## When to Use

- The user provides an API documentation URL, OpenAPI/Swagger file, HTML snippet, or cURL command and wants to extract data.
- An AI or Data Engineer needs to identify endpoints, query/header/path/form parameters, OAuth2/token lifecycles, pagination rules, error codes, and payload schemas from API documentation.
- The user asks to suggest an ingestion architecture (Batch Full Load, Incremental Watermark, Date Window, N+1 Detail Enrichment, Async Polling) and generate the Bronze extraction pipeline in pure Python.

## Workflow Steps

### Step 1: Scrape and Parse the API Documentation

For a URL or a local spec/HTML file, start with the bundled inspector (stdlib only, prints JSON):

```bash
python scripts/doc_scraper.py <docs-url-or-file> > api_map.json
```

It probes common OpenAPI paths (`/openapi.json`, `/swagger.json`, `/v3/api-docs`, ...) and links embedded in the page. When it finds a JSON spec, the output (`doc_type: "OpenAPI/Swagger"`) already lists endpoints, parameters, auth schemes, response keys and a `suggested_pattern` per endpoint. Otherwise it falls back to HTML analysis: parameter tables, error-code tables, JSON samples, rate-limit headers, and REST/RPC endpoints (`.php`, `.asmx`).

Then adapt:

1. **Index page with no endpoints** (one page per endpoint, e.g. Tiny ERP `api-docs/api` linking to `api2-*` pages): re-run with `--crawl-depth 1` (raise `--max-pages`, default 50, for large portals). The crawler only follows links on the same host under the start URL's directory. Read the merged `discovered_endpoints` (each one carries `found_on`), then open the `pages[]` entry of each endpoint the user wants for its parameter and error tables.
2. **JS-rendered portals** (Swagger UI, Redoc, Stoplight, ReadMe with no spec found, empty `headings`/`tables`): the HTML has no content to parse. Look for the spec URL in the browser's network tab or ask the user for the exported spec. `unparsed_yaml_spec_links` means a YAML spec exists, but the stdlib cannot parse it, so ask for the JSON version or read the YAML directly.
3. **Raw text or cURL**: if the user pastes raw documentation text, Markdown, or cURL commands, parse the HTTP method, URL path, query/form parameters, headers, request body keys, and sample JSON responses directly. The script is not needed.

Treat the scraper output as leads, not truth. Confirm each endpoint, date format, and error code against the page text before mapping it.

### Step 2: Map Key-Value Variables

Create a clear, structured inventory of all key-value pairs required to interact with the API and land the data:

- **Authentication & Token Lifecycle (`AUTH_KEY_MAP`)**:
  - Map static auth keys (`Authorization: Bearer`, `X-API-Key`, or body/query `token`).
  - When the API uses **OAuth2 (`client_credentials` or `refresh_token`)**, map `token_url`, `client_id_env`, `client_secret_env`, `refresh_token_env`, and response keys (`access_token`, `expires_in`) so `TokenManager` can proactively refresh tokens before expiration or upon `HTTP 401`.
  - Always bind secrets to environment variables and strip them from `_request_params` before writing Bronze metadata.
- **Request Parameters (Path, Query, Form, Body)**: Map every parameter name (`key`), data type, format (`dd/mm/yyyy` vs `YYYY-MM-DD`, timezone requirements), and role (static filter, pagination cursor, date window, sorting).
- **Application-Level Status & Error Semantics (HTTP 200 RPC Pattern)**: Many APIs return `HTTP 200 OK` even on failure, embedding status inside the payload (e.g., `retorno.status = "Erro"`, `codigo_erro = 6`). Explicitly map:
  - `root_key`, `status_key`, `status_ok_value`, `error_code_key`, `errors_list_key`.
  - Classify error codes into `rate_limited` (trigger backoff), `transient` (trigger retry), `end_of_data` (stop pagination cleanly), and `fatal`.
- **Pagination & Rate Limit Keys**:
  - Identify page size keys (`limit`, `per_page`, `$top`), offset/cursor/page keys (`page`, `pagina`, `$skip`, `next_cursor`), and total page keys (`numero_paginas`, `total_pages`, `has_more`).
  - Identify dynamic rate-limit response headers (e.g., `x-limit-api`, `X-RateLimit-Limit`, `Retry-After`) to adjust throughput at runtime.
- **Payload Response & Nested Item Keys**:
  - Identify the root container (`retorno`, `data`), list key (`pedidos`, `value`, `items`), and optional nested item wrapper key (`item_key`, e.g., when a list contains `[{"pedido": {"id": 1}}]`).
  - Map primary business key(s) (`id`, `uuid`) and timestamp keys (`updated_at`, `dataAtualizacao`) for deduplication and watermarking.

### Step 3: Recommend the Pipeline Architecture

Evaluate the mapped keys and API capabilities for each endpoint to recommend the optimal ingestion pattern (including hybrid modes where incremental and full load differ):

| Pattern | Criteria in Documentation | Execution Mechanism |
| :--- | :--- | :--- |
| **Incremental (`datetime_watermark`)** | Endpoint supports a timestamp/date filter (`updated_since`, `dataAtualizacao`, `dataMinimaAtualizacao`). | Stateful extraction from `last_watermark` minus a safety lookback window (e.g., 15 minutes) to prevent late-arriving data loss. |
| **Chunked Date Window (`date_window`)** | Endpoint only filters by start/end dates (`dataInicial` / `dataFinal`) or enforces a maximum date range per call. | Slice the extraction range into fixed-day windows (e.g., 30 days) with lookback overlap, committing files and advancing the watermark **after each window**. |
| **Batch Full Snapshot (`full`)** | Small/static reference tables (`vendedores`, `produtos`, categories) lacking date filters. | Full paginated sweep written atomically to the current `ingestion_date` partition. |
| **N+1 Detail Enrichment (`with_details`)** | List endpoint returns summary records (`pedidos.pesquisa`) requiring a detail call per ID (`pedido.obter`). | Micro-batched concurrent extraction (`ThreadPoolExecutor` capped at a safe fraction of the rate limit) flushing details per batch/window with **Dead-Letter Queue (DLQ)** isolation for poison-pill IDs. |
| **Async Job Polling** | Report/export endpoints returning a `job_id` or `status_url` (`HTTP 202 Accepted`). | 3-stage pipeline: trigger job (`POST`), poll status loop with full-jitter backoff (`GET`), and download raw result. |

Before generating code, show the user the key-value map and the pattern chosen for each endpoint, and confirm them. A wrong date format or a misclassified error code is cheap to fix here and expensive after a multi-hour load.

### Step 4: Generate the Enterprise Pure Python Bronze Pipeline

Generate a complete, modular, ready-to-run pure Python script following the patterns in `references/bronze_pipeline_templates.md`. Use the finished examples as reference implementations: `examples/tiny_bronze_pipeline.py` (RPC, form-urlencoded, HTTP 200 errors, date windows, N+1) and `examples/openlibrary_bronze_pipeline.py` (REST, recent-changes feed, N+1). Ship a matching `.env.example` (see `examples/.env.example`).

- **Zero External Dependencies**: Use only the Python standard library (`urllib.request`, `urllib.parse`, `gzip`, `random`, `json`, `time`, `os`, `datetime`, `pathlib`, `hashlib`, `logging`, `threading`, `concurrent.futures`, `argparse`) plus a built-in minimal `.env` loader.
- **Thread-Safe OAuth2 / Token Lifecycle (`TokenManager`)**: Automatically manage static tokens or OAuth2 token refresh (`client_credentials` / `refresh_token`) with expiry buffer and thread-safe locking on `HTTP 401` recovery.
- **Thread-Safe Adaptive Rate Limiter & Full-Jitter Backoff**:
  - Protect both `wait()` and `set_limit()` with `threading.Lock()`.
  - Use **Exponential Backoff with Full Jitter** (`random.uniform(0, min(cap, base * 2 ** attempt))`) to prevent thundering-herd collisions across worker threads.
- **Atomic Compressed Storage (`.jsonl.gz` or `.jsonl`) & Window Checkpointing**:
  - Support native `gzip` compression (`compress_gzip: True`) writing to `.jsonl.gz.tmp` and atomically renaming on close (`BronzeWriter`).
  - Commit data and advance the watermark **per completed date window or micro-batch** so long historical loads never lose progress.
- **Dead-Letter Queue (DLQ) for Poison-Pill Records**: In N+1 detail extraction or batch loops, isolate non-retryable per-record failures into `bronze/{source_name}/_dlq/{endpoint}/ingestion_date=YYYY-MM-DD/dlq_{run_id}.jsonl.gz` with error context instead of crashing the entire job.
- **Structured Run Telemetry (`_state/manifests/<source_name>_<run_id>_manifest.json`)**: Emit a machine-readable manifest at the end of every run recording `run_id`, `duration_seconds`, `http_requests_total`, `retries_total`, `records_written`, `dlq_records`, and watermarks per endpoint for observability in Airflow, Prefect, Dagster, or Databricks.

### Step 5: Verify Before Handing Off

- `python -m py_compile <pipeline>.py` and `python <pipeline>.py --help` must succeed.
- If the user has credentials available, run a smoke extraction limited to one endpoint and a short window (`--endpoints <one> --start-date <recent>`). Then check that one Bronze file, the watermark, and the manifest were written. If no credentials are available, say that the pipeline was not run against the live API.
- **Bronze Medallion Envelope**: Preserve the unmodified API record inside `raw_payload` alongside `_ingestion_timestamp`, `_run_id`, `_source_name`, `_source_endpoint`, `_request_params`, `_record_id`, and `_payload_hash` (SHA-256).

## Gotchas

- **HTTP 200 Error Payloads**: Never rely solely on `urllib.error.HTTPError` for RPC-style APIs. Always inspect the payload's status and error code fields before treating a response as valid data.
- **Thundering Herd on Retries**: Never use deterministic sleep intervals when running multi-threaded `ThreadPoolExecutor` extractions. Always apply `random.uniform` full jitter.
- **Mid-Run Token Expiration**: Long historical loads or N+1 runs often exceed 1-hour OAuth2 token TTLs. Always route auth headers through `TokenManager` and trigger `force_refresh()` on `HTTP 401`.
- **Unbounded N+1 Memory & Poison Pills**: Process N+1 details in bounded micro-batches per window and route failed individual IDs to the DLQ writer rather than aborting a multi-hour run.
- **Scraper False Positives**: Endpoint discovery skips `<script>` bodies, but URLs in visible text, such as a third-party SDK in a code sample, still show up. Drop endpoints whose host is not the API host.
- **Credential Leakage in Bronze Metadata**: Strip any auth keys (`token`, `access_token`, `api_key`) from `_request_params` before persisting Bronze rows or DLQ entries.