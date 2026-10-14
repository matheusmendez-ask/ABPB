#!/usr/bin/env python3
"""
Enterprise Bronze Layer Ingestion Pipeline
Source API: Tiny ERP / Olist ERP - API V2 (https://tiny.com.br/api-docs/api)
Patterns:   Incremental (datetime watermark / chunked date windows)
            + Batch Full Snapshot + Micro-batched N+1 Detail Enrichment (DLQ-isolated)
Features:   TokenManager, Adaptive RateLimiter, Full-Jitter Backoff,
            Atomic Gzip (.jsonl.gz), Window Checkpointing, Dead-Letter Queue (DLQ),
            Structured Telemetry Manifest (_run_manifest.json).
Dependencies: Pure Python Standard Library only.

Particularidades da API V2 do Tiny ERP (Olist):
  * Estilo RPC: Cada serviço é um endpoint `<servico>.php`, chamado via POST
    application/x-www-form-urlencoded com `token` + `formato=json` + parâmetros.
  * HTTP 200 Always: Sucesso/erro vem no envelope `retorno.status` ("OK"/"Erro")
    e `retorno.codigo_erro` (6 = rate limit por minuto, 11 = conexões concorrentes,
    20/23 = fim dos dados / sem registros, 35/99 = transitório).
  * Paginação fixa em 100 registros por chamada via `pagina` / `numero_paginas`.
  * Quota por plano (30/60/120 req/min) informada no header `x-limit-api`. Concorrência
    máxima recomendada = 1/4 do limite.
  * Datas em horário de Brasília (UTC-03:00) nos formatos dd/mm/yyyy [hh:mm:ss].
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
logger = logging.getLogger("tiny_bronze")

# Fuso horário oficial de Brasília (sem horário de verão desde 2019)
BRT = dt.timezone(dt.timedelta(hours=-3), name="America/Sao_Paulo")

# =====================================================================
# 1. DOCUMENTATION KEY-VALUE MAPPING
# =====================================================================
API_CONFIG: Dict[str, Any] = {
    "source_name": "tiny_erp",
    "base_url": "https://api.tiny.com.br/api2",
    "method": "POST",
    "request_encoding": "form",            # application/x-www-form-urlencoded
    "timeout_seconds": 60,
    "max_retries": 6,
    "default_rate_limit_per_minute": 30,   # Plano "Crescer" (ajustado em runtime por x-limit-api)
    "rate_limit_header": "x-limit-api",
    "bronze_output_dir": "./data/bronze",
    "state_dir": "./data/bronze/_state",
    "compress_gzip": True,                 # Gera .jsonl.gz diretamente via gzip nativo
    "default_start_date": "2024-01-01",
    "incremental_lookback_minutes": 15,    # Janela de segurança para datetime watermark
    "window_lookback_days": 3,             # Janela de sobreposição para date_window
    "detail_batch_size": 200,              # Lotes de enriquecimento N+1 por janela
}

AUTH_KEY_MAP: Dict[str, Any] = {
    "mode": "body_or_query",
    "token_param": "token",
    "token_env_var": "TINY_API_TOKEN",
    "format_param": "formato",
    "format_value": "json",
}

RESPONSE_KEY_MAP: Dict[str, str] = {
    "root": "retorno",
    "status": "status",                    # "OK" | "Erro"
    "status_ok": "OK",
    "status_processamento": "status_processamento",
    "error_code": "codigo_erro",
    "errors": "erros",
    "error_message": "erro",
    "page": "pagina",
    "total_pages": "numero_paginas",
}

PAGINATION: Dict[str, Any] = {
    "style": "page",
    "page_param": "pagina",
    "first_page": 1,
    "page_size": 100,                      # Fixo pela API V2
    "send_size_param": False,
}

# Tabela "Códigos de Erros" (api2-tabelas-processamento)
ERROR_CODE_MAP: Dict[str, Any] = {
    "rate_limited": {6: 60, 11: 5},        # código -> tempo base de espera (segundos)
    "transient": {35, 99},                 # 35=erro inesperado, 99=sistema em manutenção
    "end_of_data": {20, 23},               # 20=consulta sem registros, 23=página inexistente
}

DATE_FMT = "%d/%m/%Y"
DATETIME_FMT = "%d/%m/%Y %H:%M:%S"

ENDPOINTS: Dict[str, Dict[str, Any]] = {
    "pedidos": {
        "path": "pedidos.pesquisa.php",
        "data_key": "pedidos",
        "item_key": "pedido",
        "primary_key": "id",
        "static_params": {"sort": "ASC"},
        "incremental": {
            "type": "datetime_watermark",
            "param": "dataAtualizacao",
            "format": DATETIME_FMT,
        },
        "full": {
            "type": "date_window",
            "start_param": "dataInicial",
            "end_param": "dataFinal",
            "format": DATE_FMT,
            "window_days": 30,
        },
        "detail": {
            "name": "pedidos_detalhe",
            "path": "pedido.obter.php",
            "id_param": "id",
            "data_key": "pedido",
        },
    },
    "contatos": {
        "path": "contatos.pesquisa.php",
        "data_key": "contatos",
        "item_key": "contato",
        "primary_key": "id",
        "static_params": {"pesquisa": ""},
        "incremental": {
            "type": "datetime_watermark",
            "param": "dataMinimaAtualizacao",
            "format": DATETIME_FMT,
        },
        "full": {"type": "none"},
    },
    "produtos": {
        "path": "produtos.pesquisa.php",
        "data_key": "produtos",
        "item_key": "produto",
        "primary_key": "id",
        "static_params": {"pesquisa": ""},
        "incremental": None,
        "full": {"type": "none"},
    },
    "notas_fiscais": {
        "path": "notas.fiscais.pesquisa.php",
        "data_key": "notas_fiscais",
        "item_key": "nota_fiscal",
        "primary_key": "id",
        "static_params": {},
        "incremental": {
            "type": "date_window",
            "start_param": "dataInicial",
            "end_param": "dataFinal",
            "format": DATE_FMT,
            "window_days": 30,
        },
        "full": {
            "type": "date_window",
            "start_param": "dataInicial",
            "end_param": "dataFinal",
            "format": DATE_FMT,
            "window_days": 30,
        },
    },
    "contas_receber": {
        "path": "contas.receber.pesquisa.php",
        "data_key": "contas",
        "item_key": "conta",
        "primary_key": "id",
        "static_params": {},
        "incremental": {
            "type": "date_window",
            "start_param": "data_ini_emissao",
            "end_param": "data_fim_emissao",
            "format": DATE_FMT,
            "window_days": 30,
        },
        "full": {
            "type": "date_window",
            "start_param": "data_ini_emissao",
            "end_param": "data_fim_emissao",
            "format": DATE_FMT,
            "window_days": 30,
        },
    },
    "contas_pagar": {
        "path": "contas.pagar.pesquisa.php",
        "data_key": "contas",
        "item_key": "conta",
        "primary_key": "id",
        "static_params": {},
        "incremental": {
            "type": "date_window",
            "start_param": "data_ini_emissao",
            "end_param": "data_fim_emissao",
            "format": DATE_FMT,
            "window_days": 30,
        },
        "full": {
            "type": "date_window",
            "start_param": "data_ini_emissao",
            "end_param": "data_fim_emissao",
            "format": DATE_FMT,
            "window_days": 30,
        },
    },
    "vendedores": {
        "path": "vendedores.pesquisa.php",
        "data_key": "vendedores",
        "item_key": "vendedor",
        "primary_key": "id",
        "static_params": {"pesquisa": ""},
        "incremental": None,
        "full": {"type": "none"},
    },
}

# =====================================================================
# 2. RUN TELEMETRY, TOKEN MANAGER & ADAPTIVE RATE LIMITER
# =====================================================================
class RunTelemetry:
    """Coletor thread-safe de métricas de execução persistidas em _run_manifest.json."""

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


class TokenManager:
    """Gerenciador thread-safe de token com proteção contra vazamento em logs/metadados."""

    def __init__(self) -> None:
        self._lock = threading.Lock()

    def get_token(self) -> str:
        token = os.environ.get(AUTH_KEY_MAP["token_env_var"], "")
        if not token:
            raise RuntimeError(
                f"Variável de ambiente {AUTH_KEY_MAP['token_env_var']} não definida. "
                f"Preencha o arquivo .env ou configure o ambiente."
            )
        return token


class AdaptiveRateLimiter:
    """Limitador thread-safe com adaptação em tempo real ao header x-limit-api."""

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
                logger.info("Rate limit ajustado dinamicamente para %s req/min (intervalo %.2fs)", per_minute, self.interval)

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


class TinyApiError(RuntimeError):
    def __init__(self, service: str, code: Optional[int], messages: List[str]) -> None:
        super().__init__(f"[{service}] codigo_erro={code}: {'; '.join(messages) or 'sem mensagem'}")
        self.code = code


def _to_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _full_jitter_backoff(attempt: int, base: float = 2.0, cap: float = 120.0) -> float:
    """Exponential Backoff com Full Jitter para mitigar colisões thundering-herd."""
    ceiling = min(cap, base * (2 ** (attempt - 1)))
    return random.uniform(0, ceiling)


def call_tiny(
    service_path: str,
    params: Dict[str, Any],
    token_mgr: TokenManager,
    telemetry: RunTelemetry,
) -> Optional[Dict[str, Any]]:
    """
    Executa POST na API V2 do Tiny ERP, tratando envelope HTTP 200 de erro e rate limit.
    Retorna None quando a API indica fim dos dados (códigos 20 ou 23).
    """
    url = f"{API_CONFIG['base_url'].rstrip('/')}/{service_path.lstrip('/')}"
    token = token_mgr.get_token()

    form_payload: Dict[str, Any] = {
        AUTH_KEY_MAP["token_param"]: token,
        AUTH_KEY_MAP["format_param"]: AUTH_KEY_MAP["format_value"],
        **{k: v for k, v in params.items() if v is not None},
    }
    body = urllib.parse.urlencode(form_payload).encode("utf-8")
    headers = {
        "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
        "Accept": "application/json",
        "User-Agent": f"{API_CONFIG['source_name']}-bronze-pipeline/2.0",
    }
    rk = RESPONSE_KEY_MAP

    for attempt in range(1, API_CONFIG["max_retries"] + 1):
        RATE_LIMITER.wait()
        telemetry.incr_request()
        req = urllib.request.Request(url, data=body, headers=headers, method=API_CONFIG["method"])

        try:
            with urllib.request.urlopen(req, timeout=API_CONFIG["timeout_seconds"]) as resp:
                limit_hdr = _to_int(resp.headers.get(API_CONFIG["rate_limit_header"]))
                if limit_hdr:
                    RATE_LIMITER.set_limit(limit_hdr)
                payload = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 429 or e.code >= 500:
                telemetry.incr_retry()
                retry_after = e.headers.get("Retry-After")
                wait = float(retry_after) if (retry_after and retry_after.isdigit()) else _full_jitter_backoff(attempt)
                logger.warning("HTTP %s em %s. Retry em %.2fs (%s/%s)", e.code, service_path, wait, attempt, API_CONFIG["max_retries"])
                time.sleep(wait)
                continue
            raise TinyApiError(service_path, e.code, [e.read().decode("utf-8", "replace")[:500]]) from e
        except (urllib.error.URLError, TimeoutError, ConnectionError, json.JSONDecodeError) as e:
            telemetry.incr_retry()
            wait = _full_jitter_backoff(attempt)
            logger.warning("Falha de rede/parse em %s (%s). Retry em %.2fs", service_path, e, wait)
            time.sleep(wait)
            continue

        retorno = payload.get(rk["root"], {}) if isinstance(payload, dict) else {}
        if str(retorno.get(rk["status"], "")).upper() == rk["status_ok"].upper():
            return retorno

        code = _to_int(retorno.get(rk["error_code"]))
        raw_errors = retorno.get(rk["errors"]) or []
        messages = [
            str(err.get(rk["error_message"], err)) if isinstance(err, dict) else str(err)
            for err in (raw_errors if isinstance(raw_errors, list) else [raw_errors])
        ]

        if code in ERROR_CODE_MAP["end_of_data"]:
            return None
        if code in ERROR_CODE_MAP["rate_limited"]:
            telemetry.incr_retry()
            base_wait = ERROR_CODE_MAP["rate_limited"][code]
            wait = _full_jitter_backoff(attempt, base=float(base_wait), cap=300.0)
            logger.warning("Quota da API atingida (codigo_erro=%s). Aguardando %.2fs...", code, wait)
            time.sleep(wait)
            continue
        if code in ERROR_CODE_MAP["transient"]:
            telemetry.incr_retry()
            wait = _full_jitter_backoff(attempt, base=5.0)
            logger.warning("Erro transitório (codigo_erro=%s). Retry em %.2fs", code, wait)
            time.sleep(wait)
            continue
        raise TinyApiError(service_path, code, messages)

    raise TinyApiError(service_path, None, [f"Esgotadas {API_CONFIG['max_retries']} tentativas"])


# =====================================================================
# 3. STATE MANAGEMENT, WINDOW PLANNING & PAGINATION
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


def date_windows(start: dt.date, end: dt.date, days: int) -> List[Tuple[dt.date, dt.date]]:
    windows, cursor = [], start
    while cursor <= end:
        w_end = min(end, cursor + dt.timedelta(days=days - 1))
        windows.append((cursor, w_end))
        cursor = w_end + dt.timedelta(days=1)
    return windows


def build_request_plan(
    cfg: Dict[str, Any], endpoint: str, mode: str, run_ts: dt.datetime, start_date: Optional[dt.date]
) -> List[Tuple[Dict[str, Any], Optional[str]]]:
    """Retorna lista de tuplas (parâmetros_de_chamada, watermark_a_persistir_ao_concluir_a_janela)."""
    static = dict(cfg.get("static_params", {}))
    inc = cfg.get("incremental")
    today = run_ts.date()
    default_start = start_date or dt.date.fromisoformat(API_CONFIG["default_start_date"])

    if mode == "incremental" and inc:
        last = load_watermark(endpoint)
        if inc["type"] == "datetime_watermark":
            since = (
                dt.datetime.fromisoformat(last)
                if last
                else dt.datetime.combine(default_start, dt.time.min, tzinfo=BRT)
            )
            since -= dt.timedelta(minutes=API_CONFIG["incremental_lookback_minutes"])
            params = {**static, inc["param"]: since.astimezone(BRT).strftime(inc["format"])}
            return [(params, run_ts.isoformat())]

        if inc["type"] == "date_window":
            since_d = dt.date.fromisoformat(last) if last else default_start
            since_d -= dt.timedelta(days=API_CONFIG["window_lookback_days"])
            return [
                (
                    {
                        **static,
                        inc["start_param"]: s.strftime(inc["format"]),
                        inc["end_param"]: e.strftime(inc["format"]),
                    },
                    e.isoformat(),
                )
                for s, e in date_windows(since_d, today, inc["window_days"])
            ]

    full = cfg.get("full", {"type": "none"})
    if full["type"] == "date_window":
        return [
            (
                {
                    **static,
                    full["start_param"]: s.strftime(full["format"]),
                    full["end_param"]: e.strftime(full["format"]),
                },
                e.isoformat() if inc else None,
            )
            for s, e in date_windows(default_start, today, full["window_days"])
        ]

    final_wm = run_ts.isoformat() if (inc and inc["type"] == "datetime_watermark") else None
    return [(static, final_wm)]


def fetch_paginated(
    service_path: str,
    data_key: str,
    params: Dict[str, Any],
    token_mgr: TokenManager,
    telemetry: RunTelemetry,
) -> Iterator[Tuple[Any, Dict[str, Any]]]:
    """Itera todas as páginas do Tiny ERP (pagina 1..numero_paginas) e devolve (item, params_utilizados)."""
    page = PAGINATION["first_page"]
    rk = RESPONSE_KEY_MAP

    while True:
        page_params = {**params, PAGINATION["page_param"]: page}
        logger.info("-> %s %s", service_path, page_params)
        retorno = call_tiny(service_path, page_params, token_mgr, telemetry)
        if retorno is None:
            break

        items = retorno.get(data_key) or []
        for item in items:
            yield item, page_params

        total_pages = _to_int(retorno.get(rk["total_pages"])) or page
        if not items or page >= total_pages:
            break
        page += 1


# =====================================================================
# 4. ATOMIC BRONZE WRITER (.JSONL.GZ) & DEAD-LETTER QUEUE (DLQ)
# =====================================================================
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
    item: Any,
    cfg: Dict[str, Any],
    service_path: str,
    params: Dict[str, Any],
    run_id: str,
    item_key: Optional[str],
) -> Dict[str, Any]:
    """Envelopa o registro bruto em Bronze preservando raw_payload intacto e mascarando credenciais."""
    inner = item.get(item_key, item) if (item_key and isinstance(item, dict)) else item
    record_id = inner.get(cfg["primary_key"]) if isinstance(inner, dict) else None
    raw = json.dumps(item, sort_keys=True, ensure_ascii=False)
    # Garante que nenhum token apareça nos metadados de requisição
    safe_params = {k: v for k, v in params.items() if k != AUTH_KEY_MAP["token_param"]}
    return {
        "_ingestion_timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
        "_run_id": run_id,
        "_source_name": API_CONFIG["source_name"],
        "_source_endpoint": f"{API_CONFIG['base_url']}/{service_path}",
        "_request_params": safe_params,
        "_record_id": record_id,
        "_payload_hash": hashlib.sha256(raw.encode("utf-8")).hexdigest(),
        "raw_payload": item,
    }


def wrap_dlq_record(
    record_id: Any,
    service_path: str,
    params: Dict[str, Any],
    run_id: str,
    error_msg: str,
) -> Dict[str, Any]:
    safe_params = {k: v for k, v in params.items() if k != AUTH_KEY_MAP["token_param"]}
    return {
        "_ingestion_timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
        "_run_id": run_id,
        "_source_name": API_CONFIG["source_name"],
        "_source_endpoint": f"{API_CONFIG['base_url']}/{service_path}",
        "_request_params": safe_params,
        "_record_id": record_id,
        "_error_message": error_msg,
    }


# =====================================================================
# 5. ORCHESTRATION WITH CHECKPOINTING & DLQ ISOLATION
# =====================================================================
def fetch_details_batch(
    cfg: Dict[str, Any],
    ids: List[Any],
    run_ts: dt.datetime,
    run_id: str,
    chunk_suffix: str,
    token_mgr: TokenManager,
    telemetry: RunTelemetry,
) -> Tuple[int, int]:
    """Busca detalhes N+1 com concorrência adaptativa e isolamento de falhas em DLQ."""
    detail = cfg["detail"]
    workers = max(1, RATE_LIMITER.per_minute // 4)
    logger.info("Buscando %s detalhes em %s com %s workers...", len(ids), detail["path"], workers)

    def _one(record_id: Any) -> Tuple[Any, Optional[Dict[str, Any]], Optional[str]]:
        try:
            retorno = call_tiny(detail["path"], {detail["id_param"]: record_id}, token_mgr, telemetry)
            payload = (retorno or {}).get(detail["data_key"]) if retorno else None
            return record_id, payload, None
        except Exception as exc:
            return record_id, None, str(exc)

    with (
        BronzeWriter(detail["name"], run_ts, chunk_suffix) as writer,
        BronzeWriter(detail["name"], run_ts, chunk_suffix, is_dlq=True) as dlq_writer,
        ThreadPoolExecutor(max_workers=workers) as pool,
    ):
        for record_id, payload, err_msg in pool.map(_one, ids):
            req_p = {detail["id_param"]: record_id}
            if err_msg is not None:
                logger.error("Roteado para DLQ id=%s em %s: %s", record_id, detail["path"], err_msg)
                dlq_writer.write(wrap_dlq_record(record_id, detail["path"], req_p, run_id, err_msg))
            elif payload is not None:
                writer.write(wrap_bronze_record(payload, cfg, detail["path"], req_p, run_id, None))

    return writer.count, dlq_writer.count


def run_endpoint(
    endpoint: str,
    mode: str,
    run_ts: dt.datetime,
    run_id: str,
    start_date: Optional[dt.date],
    with_details: bool,
    token_mgr: TokenManager,
    telemetry: RunTelemetry,
) -> int:
    cfg = ENDPOINTS[endpoint]
    wm_before = load_watermark(endpoint)
    wm_after = wm_before
    plans = build_request_plan(cfg, endpoint, mode, run_ts, start_date)
    effective_mode = mode if cfg.get("incremental") else "full (snapshot)"
    logger.info("=== %s | modo=%s | %s janela(s)/lote(s) ===", endpoint, effective_mode, len(plans))

    total_written = 0
    total_details = 0
    total_dlq = 0
    seen_ids: set[Any] = set()
    batch_size = API_CONFIG.get("detail_batch_size", 200)

    try:
        for idx, (params, chunk_wm) in enumerate(plans, start=1):
            chunk_suffix = f"{run_id}_w{idx:03d}" if len(plans) > 1 else run_id
            window_ids: List[Any] = []

            with BronzeWriter(endpoint, run_ts, chunk_suffix) as writer:
                for item, page_params in fetch_paginated(
                    cfg["path"], cfg["data_key"], params, token_mgr, telemetry
                ):
                    row = wrap_bronze_record(item, cfg, cfg["path"], page_params, run_id, cfg.get("item_key"))
                    writer.write(row)
                    rec_id = row["_record_id"]
                    if with_details and cfg.get("detail") and rec_id is not None and rec_id not in seen_ids:
                        seen_ids.add(rec_id)
                        window_ids.append(rec_id)

            total_written += writer.count
            logger.info("[%s] janela %s/%s: %s registros gravados", endpoint, idx, len(plans), writer.count)

            # Enriquecimento N+1 em micro-lotes por janela com DLQ
            if with_details and cfg.get("detail") and window_ids:
                for b_idx in range(0, len(window_ids), batch_size):
                    batch_ids = window_ids[b_idx : b_idx + batch_size]
                    b_suffix = f"{chunk_suffix}_b{b_idx // batch_size + 1:03d}"
                    d_ok, d_dlq = fetch_details_batch(
                        cfg, batch_ids, run_ts, run_id, b_suffix, token_mgr, telemetry
                    )
                    total_details += d_ok
                    total_dlq += d_dlq

            # Checkpoint do watermark por janela concluída com sucesso
            if chunk_wm:
                save_watermark(endpoint, chunk_wm)
                wm_after = chunk_wm
                logger.info("[%s] watermark comitado: %s", endpoint, chunk_wm)

        telemetry.record_endpoint(endpoint, total_written, total_details, total_dlq, wm_before, wm_after, "success")
        return total_written
    except Exception as exc:
        telemetry.record_endpoint(
            endpoint, total_written, total_details, total_dlq, wm_before, wm_after, "failed", str(exc)
        )
        raise


def load_dotenv(paths: List[Path]) -> None:
    """Parser mínimo de .env (sem dependências externas)."""
    for path in paths:
        if path.exists() and path.is_file():
            for line in path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def main() -> int:
    parser = argparse.ArgumentParser(description="Tiny ERP (Olist) API V2 -> Enterprise Bronze (JSONL.GZ)")
    parser.add_argument("--endpoints", nargs="+", choices=list(ENDPOINTS), default=list(ENDPOINTS),
                        help="Lista de endpoints para extração (default: todos)")
    parser.add_argument("--mode", choices=["incremental", "full"], default="incremental",
                        help="Modo de carga: incremental ou full")
    parser.add_argument("--start-date", type=dt.date.fromisoformat,
                        help="Data inicial YYYY-MM-DD para full load ou primeiro incremental")
    parser.add_argument("--with-details", action="store_true",
                        help="Ativa busca N+1 de detalhes (pedido.obter.php) com proteção DLQ")
    parser.add_argument("--no-gzip", action="store_true",
                        help="Desativa compressão gzip (grava .jsonl sem compactação)")
    parser.add_argument("--output-dir", help="Sobrescreve diretório base da camada Bronze")
    parser.add_argument("--rate-limit", type=int, help="Req/min inicial (antes de ler x-limit-api)")
    args = parser.parse_args()

    # Procura .env no diretório do script, no CWD e no diretório pai
    script_dir = Path(__file__).resolve().parent
    load_dotenv([script_dir / ".env", Path.cwd() / ".env", script_dir.parent / ".env"])

    if args.no_gzip:
        API_CONFIG["compress_gzip"] = False
    if args.output_dir:
        API_CONFIG["bronze_output_dir"] = args.output_dir
        API_CONFIG["state_dir"] = str(Path(args.output_dir) / "_state")
    if args.rate_limit:
        RATE_LIMITER.set_limit(args.rate_limit)

    run_ts = dt.datetime.now(BRT)
    run_id = run_ts.astimezone(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    telemetry = RunTelemetry(run_id=run_id, mode=args.mode)
    token_mgr = TokenManager()

    logger.info("Iniciando Pipeline Bronze Tiny ERP (run_id=%s, mode=%s, gzip=%s)",
                run_id, args.mode, API_CONFIG["compress_gzip"])

    failures: List[str] = []
    for endpoint in args.endpoints:
        try:
            run_endpoint(endpoint, args.mode, run_ts, run_id, args.start_date, args.with_details, token_mgr, telemetry)
        except Exception as exc:
            logger.error("[%s] Falha na extração: %s", endpoint, exc)
            failures.append(endpoint)

    manifest_path = telemetry.write_manifest()
    logger.info("Manifesto de telemetria gravado em: %s", manifest_path)

    if failures:
        logger.error("Execução finalizada com erros nos endpoints: %s", ", ".join(failures))
        return 1
    logger.info("Execução finalizada com 100%% de sucesso!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
