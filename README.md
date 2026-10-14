# 🥉 API Bronze Pipeline Builder — AI Agent Skill

[![Pure Python](https://img.shields.io/badge/Python-3.9%2B%20(Zero%20Deps)-blue.svg)](https://www.python.org/)
[![Medallion Architecture](https://img.shields.io/badge/Architecture-Medallion%20Bronze-CD7F32.svg)](#)
[![Agent Skill](https://img.shields.io/badge/Agent%20Skill-Claude%20Code%20%7C%20Cursor%20%7C%20Windsurf-purple.svg)](#)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

> **🇺🇸 English** | **[🇧🇷 Português abaixo](#-api-bronze-pipeline-builder--skill-para-agentes-de-ia)**

An enterprise-grade **AI Agent Skill & Documentation Scraper** that transforms any API documentation (OpenAPI, Swagger, Redoc, Legacy RPC/ERP HTML, or cURL) into a **production-ready, pure-Python Bronze ingestion pipeline** with zero external dependencies.

---

## 🎯 Why This Skill Exists

When you ask a standard LLM to *"build an extraction script for API X"*, it typically writes a fragile `requests.get()` loop that breaks in production. Real-world APIs—especially ERPs, financial portals, and legacy systems—have complex edge cases:
- **HTTP 200 Error Payloads:** RPC APIs that return `200 OK` with `"status": "Erro"` and custom error codes in the body.
- **Silent Pagination Truncation & Date Windows:** Endpoints that require chunked 30-day windows or cursor loops.
- **Mid-Run OAuth2 Expiration:** Multi-hour historical loads or `N+1` detail extractions failing when a 3600s token expires.
- **Thundering Herd Rate Limits:** Concurrent worker threads hitting HTTP `429` simultaneously without jitter or dynamic header inspection (`x-limit-api`, `Retry-After`).
- **State Loss on Crash:** Losing 10 hours of `N+1` extraction because watermarks and files were only committed at the end of the script.

**`api-bronze-pipeline-builder`** equips your AI Coding Agent with a deterministic **4-step engineering workflow**, a **zero-dependency documentation inspector (`doc_scraper.py`)**, and **battle-tested Bronze templates**.

---

## ✨ Enterprise Features Included in Generated Pipelines

- **📦 100% Pure Python Standard Library:** Runs anywhere (local, Docker, AWS Lambda, Airflow, GitHub Actions) without `pip install` conflicts.
- **🔐 Thread-Safe `TokenManager` (OAuth2 + Static):** Automatically handles static tokens (`Header`, `Query`, `POST Form`) and OAuth2 (`client_credentials`, `refresh_token`) with proactive expiry buffers and automatic `HTTP 401` recovery.
- **⚡ Adaptive Rate Limiter & Full-Jitter Backoff:** Thread-safe `AdaptiveRateLimiter` dynamically adjusts throughput from response headers (e.g., `x-limit-api`) combined with AWS-style Exponential Backoff + Full Jitter.
- **🧱 Atomic Gzip Bronze Writer (`.jsonl.gz`):** Writes to `.jsonl.gz.tmp` and atomically renames on close, preventing corrupted files and reducing Bronze storage footprint by up to 85%.
- **🛡️ Per-Window Checkpointing & Micro-Batched `N+1`:** Commits data files and advances watermarks after every date window or micro-batch.
- **☣️ Dead-Letter Queue (DLQ) Isolation:** Poison-pill records during `N+1` detail calls are routed to `_dlq/{endpoint}/` with error context instead of crashing multi-hour jobs.
- **📊 Machine-Readable Run Telemetry (`_manifest.json`):** Emits execution metrics (`http_requests_total`, `retries_total`, `records_written`, `dlq_records`, `watermarks_before_after`) ready for Airflow/Prefect/Dagster observability.

---

## 🚀 Quick Installation

### 1. Claude Code / Hermes Agent
Clone directly into your project's `.claude/skills/` directory (or global `~/.claude/skills/`):

```bash
mkdir -p .claude/skills
git clone https://github.com/YOUR_USERNAME/api-bronze-pipeline-builder.git .claude/skills/api-bronze-pipeline-builder
```

### 2. Cursor IDE (`.cursor/rules`)
Copy the skill and template into your project rules:

```bash
mkdir -p .cursor/rules
git clone https://github.com/YOUR_USERNAME/api-bronze-pipeline-builder.git .skills/api-bronze-pipeline-builder
cp .skills/api-bronze-pipeline-builder/SKILL.md .cursor/rules/api-bronze-pipeline-builder.mdc
```

### 3. Windsurf IDE
Copy or append `SKILL.md` into `.windsurf/rules/api-bronze-pipeline-builder.md` or `.windsurfrules`.

### 4. Standalone CLI Inspector (`doc_scraper.py`)
You can inspect any API documentation URL or local OpenAPI file directly from your terminal using Python 3.9+:

```bash
python3 scripts/doc_scraper.py https://petstore3.swagger.io/api/v3/openapi.json > api_map.json
```

---

## 🛠️ How It Works (The 4-Step Agent Workflow)

```text
[API Docs URL / HTML / OpenAPI / cURL]
          │
          ▼
  1. scripts/doc_scraper.py   ──► Probes OpenAPI specs, parses HTML param/error tables,
          │                       detects RPC endpoints (.php), & infers JSON schemas.
          ▼
  2. Explicit Key-Value Map   ──► Maps Auth/OAuth2, Pagination, Error Codes, & Nested Keys.
          │
          ▼
  3. Architecture Decision    ──► Selects Incremental Watermark, Date Window, Full Snapshot,
          │                       Micro-batched N+1 Detail, or Async Job Polling.
          ▼
  4. Bronze Code Generation   ──► Outputs a standalone pure-Python pipeline writing
                                  partitioned .jsonl.gz + _state watermarks + _manifest.json.
```

### Output Directory Structure (Medallion Bronze)
Every generated pipeline organizes raw data, DLQ records, state watermarks, and manifests cleanly:

```text
data/bronze/
├── <source_name>/
│   ├── <endpoint_name>/
│   │   └── ingestion_date=YYYY-MM-DD/
│   │       └── part_20261013T200000Z_w001.jsonl.gz
│   └── _dlq/
│       └── <detail_endpoint>/
│           └── ingestion_date=YYYY-MM-DD/
│               └── dlq_20261013T200000Z_w001_b001.jsonl.gz
└── _state/
    ├── <source_name>_<endpoint>_watermark.json
    └── manifests/
        └── <source_name>_20261013T200000Z_manifest.json
```

---
---

# 🇧🇷 API Bronze Pipeline Builder — Skill para Agentes de IA

Uma **Skill de Engenharia de Dados (Nível Enterprise)** e **Scraper de Documentação** que transforma qualquer documentação de API (OpenAPI, Swagger, Redoc, HTML de ERPs/RPC ou cURL) em um **pipeline de ingestão Medallion Bronze em Python puro** (zero dependências externas) pronto para produção.

---

## 🎯 O Problema que esta Skill Resolve

Pedir para uma IA comum *"gerar um script para extrair dados da API X"* geralmente resulta em um código frágil com `requests.get()` que falha em produção. APIs do mundo real — especialmente ERPs brasileiros (**Tiny ERP / Olist, Bling, Omie, ContaAzul, Totvs**) e portais governamentais (**Banco Central Olinda/SGS**) — possuem armadilhas críticas:
- **Erros embutidos em HTTP 200 (Estilo RPC):** APIs que sempre respondem `HTTP 200 OK`, mas enviam `"status": "Erro"` e códigos de controle (`codigo_erro = 6` para rate limit, `20` para fim de página) dentro do JSON.
- **Expiração de Token OAuth2 no meio da carga:** Extrações históricas ou chamadas `N+1` que duram horas e quebram quando o `access_token` vence após 3600 segundos.
- **Colisão de Threads no Rate Limit (*Thundering Herd*):** Múltiplos workers batendo no limite da API no mesmo segundo sem *Full Jitter* ou sem ler headers dinâmicos como `x-limit-api`.
- **Perda de Progresso em Cargas Longas:** Acumular milhares de IDs na memória durante horas e perder tudo se a rede oscilar no final.

---

## ✨ O que o Pipeline Gerado Entrega

1. **Zero Dependências Externas (100% Python Stdlib):** Roda direto em qualquer ambiente (máquina local, Docker enxuto, Airflow, GitHub Actions, Cloud Functions) sem conflitos de pacotes.
2. **`TokenManager` Thread-Safe (Estático + OAuth2):** Gerencia chaves estáticas (Header, Query ou POST Form) e renova tokens OAuth2 (`client_credentials` / `refresh_token`) preventivamente ou ao receber `HTTP 401`.
3. **`AdaptiveRateLimiter` + Backoff Exponencial com *Full Jitter*:** Ajusta a velocidade em tempo de execução lendo headers da API (`x-limit-api`) e distribui esperas de retry aleatoriamente entre threads.
4. **Escrita Atômica Compactada (`.jsonl.gz`):** Grava em arquivo temporário `.jsonl.gz.tmp` e renomeia atomicamente ao concluir, economizando até 85% de armazenamento e garantindo zero arquivos corrompidos.
5. **Checkpointing por Janela & Enriquecimento `N+1` em Micro-Lotes:** Salva os dados e avança o *watermark* a cada janela de datas concluída (ex.: janelas de 30 dias).
6. **Dead-Letter Queue (`_dlq`) Nativa:** Se um ID específico falhar no enriquecimento `N+1` (ex.: `pedido.obter.php`), ele é isolado na pasta `_dlq/` com a mensagem de erro sem derrubar o pipeline.
7. **Manifesto de Telemetria (`_manifest.json`):** Ao final de cada execução, gera um JSON estruturado com duração, total de requisições HTTP, retries, refreshes de token, registros gravados e evolução dos watermarks.

---

## 💬 Como Usar no seu Agente de IA

Basta acionar a skill no **Claude Code**, **Cursor** ou **Windsurf** passando a URL da documentação ou os endpoints desejados:

> **Exemplo de Prompt 1 (ERP / RPC):**
> *"Use a skill `api-bronze-pipeline-builder` para analisar a documentação da API V2 do Tiny ERP (`https://tiny.com.br/api-docs/api`) e gerar o pipeline Bronze para os endpoints de pedidos (com enriquecimento N+1 de detalhes), notas fiscais, produtos e contas a receber."*

> **Exemplo de Prompt 2 (Dados Abertos / OData & REST):**
> *"Use a skill `api-bronze-pipeline-builder` para extrair as Expectativas de Mercado Focus (API Olinda OData) e as séries da Selic e IPCA (API SGS) do Banco Central na camada Bronze."*

### Envelope Padrão de Cada Registro na Camada Bronze
Cada linha do arquivo `.jsonl.gz` preserva o dado bruto original em `raw_payload` acompanhado dos metadados de auditoria e linhagem:

```json
{
  "_ingestion_timestamp": "2026-10-13T23:15:04.192831+00:00",
  "_run_id": "20261013T231500Z",
  "_source_name": "tiny_erp",
  "_source_endpoint": "https://api.tiny.com.br/api2/pedidos.pesquisa.php",
  "_request_params": {
    "sort": "ASC",
    "dataAtualizacao": "13/10/2026 10:00:00",
    "pagina": 1
  },
  "_record_id": "894512309",
  "_payload_hash": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
  "raw_payload": {
    "pedido": {
      "id": "894512309",
      "numero": "10492",
      "data_pedido": "13/10/2026",
      "valor": "459.90",
      "situacao": "Aprovado"
    }
  }
}
```

---

## 📂 Estrutura deste Repositório

```text
api-bronze-pipeline-builder/
├── README.md                          # Documentação (EN / PT-BR)
├── SKILL.md                           # Instruções principais e fluxo de decisão da Skill
├── references/
│   └── bronze_pipeline_templates.md   # Templates Enterprise em Python puro
├── scripts/
│   └── doc_scraper.py                 # Inspetor OpenAPI + HTML/RPC zero-dependências
└── examples/                          # Exemplos reais gerados pela skill
    ├── tiny_bronze_pipeline.py        # Pipeline Tiny ERP V2 (RPC + N+1 + Rate Limit)
    └── openlibrary_bronze_pipeline.py # Pipeline Open Library (REST + Recent Changes + N+1)
```

---

## 🤝 Contribuindo

Contribuições são muito bem-vindas! Se você testou a skill em uma API com alguma particularidade nova (ex.: GraphQL com paginação Relay, SOAP/XML legado ou novos fluxos de autenticação), abra uma Issue ou envie um Pull Request enriquecendo o `doc_scraper.py` ou os templates em `references/`.

## 📄 Licença

Distribuído sob a licença **MIT**. Veja `LICENSE` para mais informações.