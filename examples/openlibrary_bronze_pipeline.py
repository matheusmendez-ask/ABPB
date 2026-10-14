#!/usr/bin/env python3
"""
Enterprise Bronze Layer Ingestion Pipeline
Source API: Open Library (Internet Archive) - https://openlibrary.org/developers/api
Patterns:   Incremental Date Window (Recent Changes Feed) + Search Catalog
            + Subject Extraction + Micro-batched N+1 Detail Enrichment (DLQ-isolated)
Features:   Adaptive RateLimiter (180 req/min identified tier), Full-Jitter Backoff,
            Atomic Gzip (.jsonl.gz), Window Checkpointing, Dead-Letter Queue (DLQ),
            Structured Telemetry Manifest (_run_manifest.json).
Dependencies: Pure Python Standard Library only.

Particularidades da API do Open Library:
  * Sem necessidade de token secreto para leitura de dados públicos.
  * Requer/recomenda cabeçalho User-Agent com email de contato para liberar
    o limite de 3 req/segundo (180 req/min) em vez do limite anônimo (1 req/s).
  * Rate limit HTTP 429 padrão, respeitando cabeçalho Retry-After ou backoff exponencial com jitter.
  * Recent Changes API expõe feed de auditoria com IDs monotônicos e timestamps UTC.
  * Search APIs utilizam paginação via limit/offset ou page (1-indexed).
"""

from __future__ import annotations

import argparse
import datetime as dt
import gzip
import hashlib
import json
import logging
import os
import random
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, IO, Iterator, List, Optional, Tuple

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("openlibrary_bronze")

# =====================================================================
# 1. DOCUMENTATION KEY-VALUE MAPPING
# =====================================================================
API_CONFIG: Dict[str, Any] = {
    "source_name": "open_library",
    "base_url": "https://openlibrary.org",
    "method": "GET",
    "timeout_seconds": 30,
    "max_retries": 5,
    "default_rate_limit_per_minute": 180,  # 3 req/s com User-Agent identificado
    "bronze_output_dir": "./data/bronze",
    "state_dir": "./data/bronze/_state",
    "compress_gzip": True,                 # Grava .jsonl.gz atômico via gzip nativo
    "default_start_date": "2026-10-01",
    "detail_batch_size": 100,              # Lote de IDs para busca concorrente de detalhes
}

AUTH_KEY_MAP: Dict[str, Any] = {
    "mode": "identified_user_agent",
    "contact_email_env_var": "OPENLIBRARY_CONTACT_EMAIL",
    "default_contact_email": "data-pipeline@example.org",
    "app_name": "OpenLibraryBronzePipeline/2.0",
}

ENDPOINTS: Dict[str, Dict[str, Any]] = {
    "recent_changes": {
        "pattern": "date_window",
        "url_template": "recentchanges/{year}/{month:02d}/{day:02d}.json",
        "data_key": None,                 # Resposta é lista direta no nó raiz
        "primary_key": "id",
        "timestamp_key": "created",
        "static_params": {"limit": 100},
        "pagination": {
            "style": "offset",
            "page_param": "offset",
            "size_param": "limit",
            "page_size": 100,
        },
    },
    "search_books": {
        "pattern": "search_pagination",
        "path": "search.json",
        "data_key": "docs",
        "primary_key": "key",
        "static_params": {
            "q": "artificial intelligence",
            "limit": 100,
        },
        "pagination": {
            "style": "page",
            "page_param": "page",
            "size_param": "limit",
            "first_page": 1,
            "page_size": 100,
            "total_count_key": "numFound",
        },
        "detail": {
            "name": "work_details",
            "url_template": "{key}.json",   # key já vem como "/works/OL45804W"
            "data_key": None,
        },
    },
    "search_authors": {
        "pattern": "search_pagination",
        "path": "search/authors.json",
        "data_key": "docs",
        "primary_key": "key",
        "static_params": {
            "q": "asimov",
            "limit": 100,
        },
        "pagination": {
            "style": "page",
            "page_param": "page",
            "size_param": "limit",
            "first_page": 1,
            "page_size": 100,
            "total_count_key": "numFound",
        },
        "detail": {
            "name": "author_details",
            "url_template": "authors/{key}.json",
            "data_key": None,
        },
    },
    "subjects": {
        "pattern": "subject_pagination",
        "path_template": "subjects/{subject}.json",
        "default_subject": "science_fiction",
        "data_key": "works",
        "primary_key": "key",
        "static_params": {"details": "true", "limit": 100},
        "pagination": {
            "style": "offset",
            "page_param": "offset",
            "size_param": "limit",
            "page_size": 100,
            "total_count_key": "work_count",
        },
        "detail": {
            "name": "work_details",
            "url_template": "{key}.json",
            "data_key": None,
        },
    },
}


# =====================================================================
# 2. RUN TELEMETRY & ADAPTIVE RATE LIMITER
# =====================================================================
class RunTelemetry:
    """Coletor thread-safe de métricas gravadas em _run_manifest.json."""

    def __init__(self, run_id: str, mode: str) -> None:
        self._lock = threading.Lock()
        self.run_id = run_id
        self.mode = mode
        self.started_at_utc = dt.datetime.now(dt.timezone.utc).isoformat()
        self.http_requests = 0
        self.retries = 0
        self.endpoints: Dict[str, Dict[str, Any]] = {}

    def incr_request(self) -> None:
        with self._lock:
            self.http_requests += 1

    def incr_retry(self) -> None:
        with self._lock:
            self.retries += 1

    def record_endpoint(
        self,
        endpoint: str,
        records: int,
        detail_records: int,
        dlq_records: int,
        watermark_before: Optional[str],
        watermark_after: Optional[str],
        status: str = "success",
        error: Optional[str] = None,
    ) -> None:
        with self._lock:
            self.endpoints[endpoint] = {
                "status": status,
                "records_written": records,
                "detail_records_written": detail_records,
                "dlq_records_written": dlq_records,
                "watermark_before": watermark_before,
                "watermark_after": watermark_after,
                "error": error,
            }

    def write_manifest(self) -> Path:
        manifest_dir = Path(API_CONFIG["state_dir"]) / "manifests"
        manifest_dir.mkdir(parents=True, exist_ok=True)
        manifest_path = manifest_dir / f"{API_CONFIG['source_name']}_{self.run_id}_manifest.json"
        finished_at = dt.datetime.now(dt.timezone.utc)
        started_dt = dt.datetime.fromisoformat(self.started_at_utc)
        payload = {
            "source_name": API_CONFIG["source_name"],
            "run_id": self.run_id,
            "mode": self.mode,
            "started_at_utc": self.started_at_utc,
            "finished_at_utc": finished_at.isoformat(),
            "duration_seconds": round((finished_at - started_dt).total_seconds(), 2),
            "http_requests_total": self.http_requests,
            "retries_total": self.retries,
            "endpoints": self.endpoints,
        }
        manifest_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        return manifest_path


class AdaptiveRateLimiter:
    """Garante intervalo de tempo entre requisições respeitando as cotas do Open Library."""

    def __init__(self, per_minute: int) -> None:
        self._lock = threading.Lock()
        self._next_slot = 0.0
        self.per_minute = max(1, int(per_minute))
        self.interval = 60.0 / self.per_minute

    def set_limit(self, per_minute: int) -> None:
        per_minute = max(1, int(per_minute))
        with self._lock:
            if per_minute != self.per_minute:
                self.per_minute = per_minute
                self.interval = 60.0 / per_minute
                logger.info("Rate limit ajustado para %s req/min (intervalo %.2fs)", per_minute, self.interval)

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            if self._next_slot > now:
                sleep_for = self._next_slot - now
                self._next_slot += self.interval
            else:
                sleep_for = 0.0
                self._next_slot = now + self.interval
        if sleep_for > 0:
            time.sleep(sleep_for)


RATE_LIMITER = AdaptiveRateLimiter(API_CONFIG["default_rate_limit_per_minute"])


def _to_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _full_jitter_backoff(attempt: int, base: float = 2.0, cap: float = 60.0) -> float:
    """Exponential Backoff com Full Jitter para mitigar thundering herd."""
    ceiling = min(cap, base * (2 ** (attempt - 1)))
    return random.uniform(0, ceiling)


def build_user_agent() -> str:
    email = os.environ.get(AUTH_KEY_MAP["contact_email_env_var"], AUTH_KEY_MAP["default_contact_email"])
    return f"{AUTH_KEY_MAP['app_name']} ({email})"


def call_openlibrary(
    url_path: str,
    params: Optional[Dict[str, Any]],
    telemetry: RunTelemetry,
) -> Any:
    """Executa requisição GET ao Open Library com rate limiting e tratamento de 429/5xx."""
    base = API_CONFIG["base_url"].rstrip("/")
    path = url_path.lstrip("/")
    url = f"{base}/{path}"
    if params:
        clean_p = {k: v for k, v in params.items() if v is not None}
        if clean_p:
            url = f"{url}?{urllib.parse.urlencode(clean_p, doseq=True)}"

    headers = {
        "Accept": "application/json",
        "User-Agent": build_user_agent(),
    }

    for attempt in range(1, API_CONFIG["max_retries"] + 1):
        RATE_LIMITER.wait()
        telemetry.incr_request()
        req = urllib.request.Request(url, headers=headers, method="GET")

        try:
            with urllib.request.urlopen(req, timeout=API_CONFIG["timeout_seconds"]) as resp:
                raw_bytes = resp.read()
                return json.loads(raw_bytes.decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            if e.code == 429 or e.code >= 500:
                telemetry.incr_retry()
                retry_after = e.headers.get("Retry-After")
                wait = float(retry_after) if (retry_after and retry_after.isdigit()) else _full_jitter_backoff(attempt)
                logger.warning("HTTP %s em %s. Retry em %.2fs (%s/%s)", e.code, url, wait, attempt, API_CONFIG["max_retries"])
                time.sleep(wait)
                continue
            raise RuntimeError(f"HTTP {e.code} não-recuperável em {url}: {e.read().decode('utf-8', 'replace')[:300]}") from e
        except (urllib.error.URLError, TimeoutError, ConnectionError, json.JSONDecodeError) as e:
            telemetry.incr_retry()
            wait = _full_jitter_backoff(attempt)
            logger.warning("Erro de rede/parse em %s (%s). Retry em %.2fs", url, e, wait)
            time.sleep(wait)
            continue

    raise RuntimeError(f"Esgotadas {API_CONFIG['max_retries']} tentativas para {url}")


# =====================================================================
# 3. STATE MANAGEMENT & ATOMIC BRONZE WRITER (.JSONL.GZ)
# =====================================================================
def _state_path(endpoint: str) -> Path:
    return Path(API_CONFIG["state_dir"]) / f"{API_CONFIG['source_name']}_{endpoint}_watermark.json"


def load_watermark(endpoint: str) -> Optional[str]:
    path = _state_path(endpoint)
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8")).get("last_watermark")
    return None


def save_watermark(endpoint: str, value: str) -> None:
    path = _state_path(endpoint)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(
        json.dumps(
            {
                "last_watermark": value,
                "updated_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    tmp.replace(path)


class BronzeWriter:
    """Escritor particionado atômico com suporte nativo a Gzip (.jsonl.gz.tmp -> .jsonl.gz)."""

    def __init__(self, endpoint: str, run_ts: dt.datetime, chunk_suffix: str, is_dlq: bool = False) -> None:
        sub_path = f"_dlq/{endpoint}" if is_dlq else endpoint
        self.dir = (
            Path(API_CONFIG["bronze_output_dir"])
            / API_CONFIG["source_name"]
            / sub_path
            / f"ingestion_date={run_ts.astimezone(dt.timezone.utc):%Y-%m-%d}"
        )
        ext = ".jsonl.gz" if API_CONFIG.get("compress_gzip", True) else ".jsonl"
        prefix = "dlq" if is_dlq else "part"
        self.final = self.dir / f"{prefix}_{chunk_suffix}{ext}"
        self.tmp = self.dir / f"{prefix}_{chunk_suffix}{ext}.tmp"
        self.count = 0
        self._fh: Optional[IO[str]] = None

    def __enter__(self) -> "BronzeWriter":
        self.dir.mkdir(parents=True, exist_ok=True)
        if API_CONFIG.get("compress_gzip", True):
            self._fh = gzip.open(self.tmp, "wt", encoding="utf-8")
        else:
            self._fh = self.tmp.open("w", encoding="utf-8")
        return self

    def write(self, row: Dict[str, Any]) -> None:
        assert self._fh is not None
        self._fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        self.count += 1

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._fh is not None:
            self._fh.close()
        if exc_type is not None or self.count == 0:
            self.tmp.unlink(missing_ok=True)
        else:
            self.tmp.replace(self.final)


def wrap_bronze_record(
    record: Any,
    source_endpoint: str,
    request_params: Dict[str, Any],
    run_id: str,
    record_id: Optional[Any],
) -> Dict[str, Any]:
    raw_serialized = json.dumps(record, sort_keys=True, ensure_ascii=False)
    payload_hash = hashlib.sha256(raw_serialized.encode("utf-8")).hexdigest()
    return {
        "_ingestion_timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
        "_run_id": run_id,
        "_source_name": API_CONFIG["source_name"],
        "_source_endpoint": f"{API_CONFIG['base_url'].rstrip('/')}/{source_endpoint.lstrip('/')}",
        "_request_params": request_params,
        "_record_id": record_id,
        "_payload_hash": payload_hash,
        "raw_payload": record,
    }


def wrap_dlq_record(
    record_id: Any,
    source_endpoint: str,
    request_params: Dict[str, Any],
    run_id: str,
    error_msg: str,
) -> Dict[str, Any]:
    return {
        "_ingestion_timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
        "_run_id": run_id,
        "_source_name": API_CONFIG["source_name"],
        "_source_endpoint": f"{API_CONFIG['base_url'].rstrip('/')}/{source_endpoint.lstrip('/')}",
        "_request_params": request_params,
        "_record_id": record_id,
        "_error_message": error_msg,
    }


# =====================================================================
# 4. EXTRACTION WORKFLOWS
# =====================================================================
def fetch_details_batch(
    cfg_detail: Dict[str, Any],
    ids: List[str],
    run_ts: dt.datetime,
    run_id: str,
    chunk_suffix: str,
    telemetry: RunTelemetry,
) -> Tuple[int, int]:
    """Busca detalhes N+1 concorrentemente com isolamento de registros ausentes ou com erro na DLQ."""
    detail_name = cfg_detail["name"]
    url_template = cfg_detail["url_template"]
    workers = min(4, max(1, RATE_LIMITER.per_minute // 45))  # Concorrência segura ~3-4 workers

    def _fetch_one(item_key: str) -> Tuple[str, Optional[Dict[str, Any]], Optional[str]]:
        # Trata chaves no formato "/works/OL123W" ou "OL123W"
        clean_key = item_key.lstrip("/")
        if clean_key.startswith("works/"):
            clean_key = clean_key[len("works/") :]
        elif clean_key.startswith("authors/"):
            clean_key = clean_key[len("authors/") :]

        path = url_template.format(key=clean_key)
        try:
            res = call_openlibrary(path, None, telemetry)
            if res is None:
                return item_key, None, "404 Not Found"
            return item_key, res, None
        except Exception as exc:
            return item_key, None, str(exc)

    with (
        BronzeWriter(detail_name, run_ts, chunk_suffix) as writer,
        BronzeWriter(detail_name, run_ts, chunk_suffix, is_dlq=True) as dlq_writer,
        ThreadPoolExecutor(max_workers=workers) as pool,
    ):
        for rec_id, payload, err in pool.map(_fetch_one, ids):
            if err is not None:
                dlq_writer.write(wrap_dlq_record(rec_id, url_template, {"key": rec_id}, run_id, err))
            elif payload is not None:
                writer.write(wrap_bronze_record(payload, url_template, {"key": rec_id}, run_id, rec_id))

    return writer.count, dlq_writer.count


def run_recent_changes(
    endpoint: str,
    cfg: Dict[str, Any],
    mode: str,
    run_ts: dt.datetime,
    run_id: str,
    start_date: Optional[dt.date],
    telemetry: RunTelemetry,
) -> int:
    """Extrai feed diário de auditoria do Recent Changes com checkpoint de watermark por dia."""
    wm_before = load_watermark(endpoint)
    today = run_ts.date()
    default_start = start_date or dt.date.fromisoformat(API_CONFIG["default_start_date"])

    if mode == "incremental" and wm_before:
        cursor_date = dt.date.fromisoformat(wm_before)
    else:
        cursor_date = default_start

    total_records = 0
    wm_after = wm_before

    while cursor_date <= today:
        date_str = cursor_date.isoformat()
        chunk_suffix = f"{run_id}_{date_str.replace('-', '')}"
        url_path = cfg["url_template"].format(year=cursor_date.year, month=cursor_date.month, day=cursor_date.day)
        logger.info("[recent_changes] Processando data %s (%s)...", date_str, url_path)

        offset = 0
        limit = cfg["pagination"]["page_size"]
        day_records = 0

        with BronzeWriter(endpoint, run_ts, chunk_suffix) as writer:
            while True:
                params = {"limit": limit, "offset": offset}
                data = call_openlibrary(url_path, params, telemetry)
                if not data or not isinstance(data, list):
                    break

                for item in data:
                    rec_id = item.get(cfg["primary_key"])
                    row = wrap_bronze_record(item, url_path, params, run_id, rec_id)
                    writer.write(row)
                    day_records += 1

                if len(data) < limit:
                    break
                offset += len(data)

        total_records += writer.count
        logger.info("[recent_changes] Data %s finalizada: %s registros gravados", date_str, writer.count)

        # Checkpoint do watermark para cada dia concluído
        save_watermark(endpoint, date_str)
        wm_after = date_str
        cursor_date += dt.timedelta(days=1)

    telemetry.record_endpoint(endpoint, total_records, 0, 0, wm_before, wm_after, "success")
    return total_records


def run_search_or_subject(
    endpoint: str,
    cfg: Dict[str, Any],
    run_ts: dt.datetime,
    run_id: str,
    with_details: bool,
    subject_name: Optional[str],
    search_query: Optional[str],
    telemetry: RunTelemetry,
) -> int:
    """Extrai catálogo de livros, autores ou temas via paginação limit/page."""
    wm_before = load_watermark(endpoint)
    params = dict(cfg.get("static_params", {}))

    if search_query:
        params["q"] = search_query

    if endpoint == "subjects":
        sub = subject_name or cfg.get("default_subject", "science_fiction")
        path = cfg["path_template"].format(subject=sub)
    else:
        path = cfg["path"]

    pag_cfg = cfg["pagination"]
    style = pag_cfg["style"]
    page_size = pag_cfg["page_size"]
    page = pag_cfg.get("first_page", 1)
    offset = 0

    total_records = 0
    total_details = 0
    total_dlq = 0
    seen_ids: set[str] = set()
    detail_batch_size = API_CONFIG.get("detail_batch_size", 100)

    logger.info("=== %s | path=%s | params=%s ===", endpoint, path, params)

    with BronzeWriter(endpoint, run_ts, run_id) as writer:
        while True:
            call_params = dict(params)
            if style == "page":
                call_params[pag_cfg["page_param"]] = page
            elif style == "offset":
                call_params[pag_cfg["page_param"]] = offset
            if pag_cfg.get("size_param"):
                call_params[pag_cfg["size_param"]] = page_size

            logger.info("-> %s %s", path, call_params)
            response = call_openlibrary(path, call_params, telemetry)
            if not response or not isinstance(response, dict):
                break

            data_key = cfg.get("data_key")
            items = response.get(data_key, []) if data_key else response
            if not isinstance(items, list) or not items:
                break

            for item in items:
                rec_id = item.get(cfg["primary_key"])
                row = wrap_bronze_record(item, path, call_params, run_id, rec_id)
                writer.write(row)
                if with_details and cfg.get("detail") and rec_id and rec_id not in seen_ids:
                    seen_ids.add(str(rec_id))

            total_records += len(items)

            # Paginação de parada
            total_count = _to_int(response.get(pag_cfg.get("total_count_key", "")))
            if total_count and total_records >= total_count:
                break
            if len(items) < page_size:
                break

            if style == "page":
                page += 1
            elif style == "offset":
                offset += len(items)

    logger.info("[%s] %s registros salvos no arquivo %s", endpoint, writer.count, writer.final)

    # Detalhes N+1
    if with_details and cfg.get("detail") and seen_ids:
        ids_list = list(seen_ids)
        logger.info("[%s] Iniciando enriquecimento N+1 para %s registros...", endpoint, len(ids_list))
        for b_idx in range(0, len(ids_list), detail_batch_size):
            batch = ids_list[b_idx : b_idx + detail_batch_size]
            b_suffix = f"{run_id}_b{b_idx // detail_batch_size + 1:03d}"
            d_ok, d_dlq = fetch_details_batch(cfg["detail"], batch, run_ts, run_id, b_suffix, telemetry)
            total_details += d_ok
            total_dlq += d_dlq

    wm_after = run_ts.isoformat()
    save_watermark(endpoint, wm_after)
    telemetry.record_endpoint(endpoint, total_records, total_details, total_dlq, wm_before, wm_after, "success")
    return total_records


def load_dotenv(paths: List[Path]) -> None:
    """Parser mínimo de arquivo .env (sem bibliotecas externas)."""
    for path in paths:
        if path.exists() and path.is_file():
            for line in path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def main() -> int:
    parser = argparse.ArgumentParser(description="Open Library API -> Enterprise Bronze Ingestion (JSONL.GZ)")
    parser.add_argument("--endpoints", nargs="+", choices=list(ENDPOINTS), default=list(ENDPOINTS),
                        help="Endpoints a extrair (default: todos)")
    parser.add_argument("--mode", choices=["incremental", "full"], default="incremental",
                        help="Modo para recent_changes (incremental com checkpoint ou full)")
    parser.add_argument("--start-date", type=dt.date.fromisoformat,
                        help="Data inicial YYYY-MM-DD para o feed de recent_changes")
    parser.add_argument("--with-details", action="store_true",
                        help="Ativa busca N+1 de detalhes de livros/autores com proteção DLQ")
    parser.add_argument("--query", type=str,
                        help="Termo de pesquisa para search_books e search_authors (ex: 'data science')")
    parser.add_argument("--subject", type=str,
                        help="Nome do assunto para o endpoint subjects (ex: 'programming', 'history')")
    parser.add_argument("--no-gzip", action="store_true",
                        help="Desativa compressão gzip (grava .jsonl sem compactação)")
    parser.add_argument("--output-dir", help="Sobrescreve diretório base da camada Bronze")
    parser.add_argument("--contact-email", help="Email para o User-Agent (garante cota de 180 req/min)")
    args = parser.parse_args()

    script_dir = Path(__file__).resolve().parent
    load_dotenv([script_dir / ".env", Path.cwd() / ".env", script_dir.parent / ".env"])

    if args.contact_email:
        os.environ[AUTH_KEY_MAP["contact_email_env_var"]] = args.contact_email

    if args.no_gzip:
        API_CONFIG["compress_gzip"] = False
    if args.output_dir:
        API_CONFIG["bronze_output_dir"] = args.output_dir
        API_CONFIG["state_dir"] = str(Path(args.output_dir) / "_state")

    run_ts = dt.datetime.now(dt.timezone.utc)
    run_id = run_ts.strftime("%Y%m%dT%H%M%SZ")
    telemetry = RunTelemetry(run_id=run_id, mode=args.mode)

    logger.info("Iniciando Pipeline Bronze Open Library (run_id=%s, mode=%s, gzip=%s, agent=%s)",
                run_id, args.mode, API_CONFIG["compress_gzip"], build_user_agent())

    failures: List[str] = []
    for endpoint in args.endpoints:
        cfg = ENDPOINTS[endpoint]
        try:
            if endpoint == "recent_changes":
                run_recent_changes(endpoint, cfg, args.mode, run_ts, run_id, args.start_date, telemetry)
            else:
                run_search_or_subject(endpoint, cfg, run_ts, run_id, args.with_details,
                                      args.subject, args.query, telemetry)
        except Exception as exc:
            logger.error("[%s] Falha na extração: %s", endpoint, exc)
            failures.append(endpoint)

    manifest_path = telemetry.write_manifest()
    logger.info("Manifesto de telemetria gravado em: %s", manifest_path)

    if failures:
        logger.error("Execução finalizada com erros nos endpoints: %s", ", ".join(failures))
        return 1
    logger.info("Execução finalizada com sucesso!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
