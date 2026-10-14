# Enterprise Pure Python Bronze Ingestion Pipeline Templates

Use these production-tested, pure-Python templates to generate the final Bronze ingestion script. Always populate `API_CONFIG`, `AUTH_KEY_MAP`, `RESPONSE_KEY_MAP`, `ERROR_CODE_MAP`, and `ENDPOINTS` with the exact keys discovered in the API documentation.

---

## Template 1: Enterprise Multi-Endpoint Bronze Pipeline (OAuth2 TokenManager, Full Jitter, Gzip Atomic Writer, DLQ & Run Manifest)

Supports standard REST (`GET` query / `POST` JSON), OData, and RPC/Legacy ERP APIs (`POST` form-urlencoded, HTTP 200 error codes, nested `item_key` wrappers, adaptive rate-limit headers, OAuth2 auto-refresh, per-window checkpointing, Dead-Letter Queue isolation, and `_run_manifest.json` telemetry).

```python
#!/usr/bin/env python3
"""
Enterprise Bronze Layer Ingestion Pipeline
Source API: <API_NAME> (<DOCS_URL>)
Patterns:   Incremental (datetime watermark / chunked date windows)
            + Batch Full Load + Micro-batched N+1 Detail Enrichment
Features:   OAuth2 TokenManager, Adaptive RateLimiter, Full-Jitter Backoff,
            Atomic Gzip (.jsonl.gz), Dead-Letter Queue (DLQ), Telemetry Manifest
Dependencies: Pure Python Standard Library only
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
logger = logging.getLogger("bronze_pipeline")

LOCAL_TZ = dt.timezone(dt.timedelta(hours=-3), name="America/Sao_Paulo")

# =====================================================================
# 1. DOCUMENTATION KEY-VALUE MAPPING
# =====================================================================
API_CONFIG: Dict[str, Any] = {
    "source_name": "<source_slug>",
    "base_url": "[https://api.example.com/v1](https://api.example.com/v1)",
    "method": "GET",                       # "GET" | "POST"
    "request_encoding": "query",           # "query" | "form" | "json"
    "timeout_seconds": 60,
    "max_retries": 5,
    "default_rate_limit_per_minute": 60,
    "rate_limit_header": "x-limit-api",    # Dynamic rate limit header if provided by API
    "bronze_output_dir": "./data/bronze",
    "state_dir": "./data/bronze/_state",
    "compress_gzip": True,                 # True -> writes .jsonl.gz; False -> writes .jsonl
    "default_start_date": "2025-01-01",
    "incremental_lookback_minutes": 15,
    "window_lookback_days": 3,
    "detail_batch_size": 200,              # Flush N+1 details every N parent records
}

AUTH_KEY_MAP: Dict[str, Any] = {
    "mode": "header",                      # "header" | "body_or_query" | "oauth2" | "none"
    "header_name": "Authorization",
    "header_prefix": "Bearer ",
    "token_param": "token",                # Used when mode == "body_or_query"
    "token_env_var": "API_AUTH_TOKEN",
    "format_param": "formato",             # Optional RPC format key (None to disable)
    "format_value": "json",
    # OAuth2 configuration (used when mode == "oauth2")
    "oauth2": {
        "token_url": "[https://api.example.com/oauth/token](https://api.example.com/oauth/token)",
        "grant_type": "client_credentials",  # "client_credentials" | "refresh_token"
        "client_id_env": "API_CLIENT_ID",
        "client_secret_env": "API_CLIENT_SECRET",
        "refresh_token_env": "API_REFRESH_TOKEN",
        "access_token_key": "access_token",
        "expires_in_key": "expires_in",
        "expiry_buffer_seconds": 120,
    },
}

RESPONSE_KEY_MAP: Dict[str, Optional[str]] = {
    "root": None,                          # e.g. "retorno" for RPC APIs, or None if root
    "status": None,                        # e.g. "status" ("OK" | "Erro") for HTTP-200 RPC APIs
    "status_ok": "OK",
    "error_code": "codigo_erro",
    "errors": "erros",
    "error_message": "erro",
    "page": "pagina",
    "total_pages": "numero_paginas",
    "next_cursor": "next_cursor",
    "has_more": "has_more",
}

PAGINATION: Dict[str, Any] = {
    "style": "page",                       # "page" | "offset" | "cursor" | "none"
    "page_param": "pagina",
    "size_param": "limit",
    "first_page": 1,
    "page_size": 100,
    "send_size_param": False,
}

# Semantic error classification for APIs returning errors inside HTTP 200 payloads
ERROR_CODE_MAP: Dict[str, Any] = {
    "rate_limited": {6: 60, 11: 5},        # error_code -> base wait seconds
    "transient": {35, 99},                 # retryable server/maintenance codes
    "end_of_data": {20, 23},               # empty page / no records found -> stop pagination cleanly
}

DATE_FMT = "%Y-%m-%d"
DATETIME_FMT = "%Y-%m-%dT%H:%M:%SZ"

ENDPOINTS: Dict[str, Dict[str, Any]] = {
    "orders": {
        "path": "orders",
        "data_key": "orders",
        "item_key": None,                  # Set to e.g. "pedido" if items are wrapped: [{"pedido": {...}}]
        "primary_key": "id",
        "static_params": {},
        "incremental": {
            "type": "datetime_watermark",  # "datetime_watermark" | "date_window" | None
            "param": "updated_since",
            "format": DATETIME_FMT,
        },
        "full": {
            "type": "date_window",         # "date_window" | "none"
            "start_param": "start_date",
            "end_param": "end_date",
            "format": DATE_FMT,
            "window_days": 30,
        },
        "detail": {                        # Optional N+1 child extraction
            "name": "orders_detail",
            "path": "orders/detail",
            "id_param": "id",
            "data_key": "order",
        },
    },
}


# =====================================================================
# 2. TELEMETRY, TOKEN MANAGER & ADAPTIVE RATE LIMITER
# =====================================================================
class RunTelemetry:
    """Thread-safe collector for run metrics emitted to _run_manifest.json."""

    def __init__(self, run_id: str, mode: str) -> None:
        self._lock = threading.Lock()
        self.run_id = run_id
        self.mode = mode
        self.started_at_utc = dt.datetime.now(dt.timezone.utc).isoformat()
        self.http_requests = 0
        self.retries = 0
        self.token_refreshes = 0
        self.endpoints: Dict[str, Dict[str, Any]] = {}

    def incr_request(self) -> None:
        with self._lock:
            self.http_requests += 1

    def incr_retry(self) -> None:
        with self._lock:
            self.retries += 1

    def incr_token_refresh(self) -> None:
        with self._lock:
            self.token_refreshes += 1

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
            "token_refreshes_total": self.token_refreshes,
            "endpoints": self.endpoints,
        }
        manifest_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        return manifest_path


class TokenManager:
    """Thread-safe static & OAuth2 token lifecycle manager with automatic 401 refresh."""

    def __init__(self, telemetry: RunTelemetry) -> None:
        self._lock = threading.Lock()
        self._telemetry = telemetry
        self._access_token: Optional[str] = None
        self._expires_at: float = 0.0

    def get_token(self, force_refresh: bool = False) -> str:
        mode = AUTH_KEY_MAP.get("mode", "none")
        if mode == "none":
            return ""
        if mode in ("header", "body_or_query"):
            token = os.environ.get(AUTH_KEY_MAP["token_env_var"], "")
            if not token:
                raise RuntimeError(f"Environment variable {AUTH_KEY_MAP['token_env_var']} is not set.")
            return token

        # OAuth2 flow
        with self._lock:
            now = time.monotonic()
            if not force_refresh and self._access_token and now < self._expires_at:
                return self._access_token

            oauth_cfg = AUTH_KEY_MAP["oauth2"]
            client_id = os.environ.get(oauth_cfg["client_id_env"], "")
            client_secret = os.environ.get(oauth_cfg["client_secret_env"], "")
            if not client_id or not client_secret:
                raise RuntimeError("OAuth2 credentials missing in environment variables.")

            form_data: Dict[str, str] = {
                "grant_type": oauth_cfg["grant_type"],
                "client_id": client_id,
                "client_secret": client_secret,
            }
            if oauth_cfg["grant_type"] == "refresh_token":
                form_data["refresh_token"] = os.environ.get(oauth_cfg["refresh_token_env"], "")

            req = urllib.request.Request(
                oauth_cfg["token_url"],
                data=urllib.parse.urlencode(form_data).encode("utf-8"),
                headers={"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=API_CONFIG["timeout_seconds"]) as resp:
                token_payload = json.loads(resp.read().decode("utf-8"))

            self._access_token = str(token_payload[oauth_cfg["access_token_key"]])
            expires_in = int(token_payload.get(oauth_cfg.get("expires_in_key", "expires_in"), 3600))
            buffer_sec = int(oauth_cfg.get("expiry_buffer_seconds", 120))
            self._expires_at = time.monotonic() + max(30, expires_in - buffer_sec)
            self._telemetry.incr_token_refresh()
            logger.info("OAuth2 access token refreshed (valid for ~%ss)", expires_in)
            return self._access_token


class AdaptiveRateLimiter:
    """Thread-safe rate limiter that spaces requests and adapts to runtime headers."""

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
                logger.info("Rate limit adjusted to %s req/min (interval %.2fs)", per_minute, self.interval)

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


class ApiExecutionError(RuntimeError):
    def __init__(self, endpoint: str, code: Optional[int], messages: List[str]) -> None:
        super().__init__(f"[{endpoint}] error_code={code}: {'; '.join(messages) or 'unknown error'}")
        self.code = code


def _to_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _full_jitter_backoff(attempt: int, base: float = 2.0, cap: float = 120.0) -> float:
    """AWS-style Exponential Backoff with Full Jitter to prevent thundering herd."""
    ceiling = min(cap, base * (2 ** (attempt - 1)))
    return random.uniform(0, ceiling)


def call_api(
    endpoint_path: str,
    params: Dict[str, Any],
    token_mgr: TokenManager,
    telemetry: RunTelemetry,
) -> Optional[Dict[str, Any]]:
    """
    Execute an HTTP call with rate limiting, OAuth2 401 recovery, full-jitter backoff,
    and semantic error checking. Returns None when the API signals end-of-data.
    """
    base_url = f"{API_CONFIG['base_url'].rstrip('/')}/{endpoint_path.lstrip('/')}"
    rk = RESPONSE_KEY_MAP
    force_refresh = False

    for attempt in range(1, API_CONFIG["max_retries"] + 1):
        token = token_mgr.get_token(force_refresh=force_refresh)
        force_refresh = False

        headers: Dict[str, str] = {
            "Accept": "application/json",
            "User-Agent": f"{API_CONFIG['source_name']}-bronze-pipeline/2.0",
        }
        req_params = {k: v for k, v in params.items() if v is not None}
        if AUTH_KEY_MAP.get("format_param") and AUTH_KEY_MAP.get("format_value"):
            req_params.setdefault(AUTH_KEY_MAP["format_param"], AUTH_KEY_MAP["format_value"])

        auth_mode = AUTH_KEY_MAP.get("mode", "none")
        if auth_mode in ("header", "oauth2") and token:
            headers[AUTH_KEY_MAP["header_name"]] = f"{AUTH_KEY_MAP.get('header_prefix', '')}{token}"
        elif auth_mode == "body_or_query" and token:
            req_params[AUTH_KEY_MAP["token_param"]] = token

        url = base_url
        body: Optional[bytes] = None
        encoding = API_CONFIG["request_encoding"]
        if encoding == "query":
            if req_params:
                url = f"{base_url}?{urllib.parse.urlencode(req_params, doseq=True)}"
        elif encoding == "form":
            headers["Content-Type"] = "application/x-www-form-urlencoded; charset=utf-8"
            body = urllib.parse.urlencode(req_params).encode("utf-8")
        elif encoding == "json":
            headers["Content-Type"] = "application/json; charset=utf-8"
            body = json.dumps(req_params).encode("utf-8")

        RATE_LIMITER.wait()
        telemetry.incr_request()
        req = urllib.request.Request(url, data=body, headers=headers, method=API_CONFIG["method"])

        try:
            with urllib.request.urlopen(req, timeout=API_CONFIG["timeout_seconds"]) as resp:
                if API_CONFIG.get("rate_limit_header"):
                    limit = _to_int(resp.headers.get(API_CONFIG["rate_limit_header"]))
                    if limit:
                        RATE_LIMITER.set_limit(limit)
                payload = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 401 and auth_mode == "oauth2" and attempt < API_CONFIG["max_retries"]:
                logger.warning("HTTP 401 on %s. Triggering OAuth2 token refresh...", endpoint_path)
                force_refresh = True
                telemetry.incr_retry()
                continue
            if e.code == 429 or e.code >= 500:
                telemetry.incr_retry()
                retry_after = e.headers.get("Retry-After")
                wait = float(retry_after) if (retry_after and retry_after.isdigit()) else _full_jitter_backoff(attempt)
                logger.warning("HTTP %s on %s. Retry in %.2fs (%s/%s)", e.code, endpoint_path, wait, attempt, API_CONFIG["max_retries"])
                time.sleep(wait)
                continue
            raise ApiExecutionError(endpoint_path, e.code, [e.read().decode("utf-8", "replace")[:500]]) from e
        except (urllib.error.URLError, TimeoutError, ConnectionError, json.JSONDecodeError) as e:
            telemetry.incr_retry()
            wait = _full_jitter_backoff(attempt)
            logger.warning("Network/parse error on %s (%s). Retry in %.2fs", endpoint_path, e, wait)
            time.sleep(wait)
            continue

        root_obj = payload.get(rk["root"], {}) if (rk["root"] and isinstance(payload, dict)) else payload
        if not rk["status"] or not isinstance(root_obj, dict):
            return root_obj if isinstance(root_obj, dict) else {"data": root_obj}

        if str(root_obj.get(rk["status"], "")).upper() == str(rk["status_ok"]).upper():
            return root_obj

        code = _to_int(root_obj.get(rk["error_code"] or ""))
        raw_errors = root_obj.get(rk["errors"] or "") or []
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
            logger.warning("App rate limit hit (code=%s). Waiting %.2fs...", code, wait)
            time.sleep(wait)
            continue
        if code in ERROR_CODE_MAP["transient"]:
            telemetry.incr_retry()
            wait = _full_jitter_backoff(attempt, base=5.0)
            logger.warning("Transient API error (code=%s). Retry in %.2fs", code, wait)
            time.sleep(wait)
            continue
        raise ApiExecutionError(endpoint_path, code, messages)

    raise ApiExecutionError(endpoint_path, None, [f"Exhausted {API_CONFIG['max_retries']} retries"])


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
                else dt.datetime.combine(default_start, dt.time.min, tzinfo=LOCAL_TZ)
            )
            since -= dt.timedelta(minutes=API_CONFIG["incremental_lookback_minutes"])
            params = {**static, inc["param"]: since.astimezone(LOCAL_TZ).strftime(inc["format"])}
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
    endpoint_path: str,
    data_key: Optional[str],
    params: Dict[str, Any],
    token_mgr: TokenManager,
    telemetry: RunTelemetry,
) -> Iterator[Tuple[Any, Dict[str, Any]]]:
    style = PAGINATION["style"]
    page = PAGINATION["first_page"]
    offset = 0
    cursor: Optional[str] = None
    rk = RESPONSE_KEY_MAP

    while True:
        page_params = dict(params)
        if PAGINATION.get("send_size_param") and PAGINATION.get("size_param"):
            page_params[PAGINATION["size_param"]] = PAGINATION["page_size"]

        if style == "page":
            page_params[PAGINATION["page_param"]] = page
        elif style == "offset":
            page_params[PAGINATION["page_param"]] = offset
        elif style == "cursor" and cursor:
            page_params[PAGINATION["page_param"]] = cursor

        logger.info("-> %s %s", endpoint_path, page_params)
        root_obj = call_api(endpoint_path, page_params, token_mgr, telemetry)
        if root_obj is None:
            break

        items = root_obj.get(data_key) if (data_key and isinstance(root_obj, dict)) else root_obj
        if isinstance(items, dict):
            items = [items]
        elif not isinstance(items, list):
            items = []

        if not items:
            break

        for item in items:
            yield item, page_params

        if style == "none":
            break
        elif style == "page":
            total_pages = _to_int(root_obj.get(rk["total_pages"] or "")) if isinstance(root_obj, dict) else None
            if total_pages is not None:
                if page >= total_pages:
                    break
            elif len(items) < PAGINATION["page_size"]:
                break
            page += 1
        elif style == "offset":
            if len(items) < PAGINATION["page_size"]:
                break
            offset += len(items)
        elif style == "cursor":
            cursor = root_obj.get(rk["next_cursor"] or "") if isinstance(root_obj, dict) else None
            has_more = root_obj.get(rk["has_more"] or "", bool(cursor)) if isinstance(root_obj, dict) else False
            if not cursor or not has_more:
                break


# =====================================================================
# 4. ATOMIC BRONZE WRITER (.JSONL / .JSONL.GZ) & DLQ WRITER
# =====================================================================
class BronzeWriter:
    """Write partitioned JSONL or Gzipped JSONL atomically via a .tmp file."""

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
    endpoint_path: str,
    params: Dict[str, Any],
    run_id: str,
    item_key: Optional[str],
) -> Dict[str, Any]:
    inner = item.get(item_key, item) if (item_key and isinstance(item, dict)) else item
    record_id = inner.get(cfg["primary_key"]) if isinstance(inner, dict) else None
    raw = json.dumps(item, sort_keys=True, ensure_ascii=False)
    return {
        "_ingestion_timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
        "_run_id": run_id,
        "_source_name": API_CONFIG["source_name"],
        "_source_endpoint": f"{API_CONFIG['base_url'].rstrip('/')}/{endpoint_path.lstrip('/')}",
        "_request_params": params,
        "_record_id": record_id,
        "_payload_hash": hashlib.sha256(raw.encode("utf-8")).hexdigest(),
        "raw_payload": item,
    }


def wrap_dlq_record(
    record_id: Any,
    endpoint_path: str,
    params: Dict[str, Any],
    run_id: str,
    error_msg: str,
) -> Dict[str, Any]:
    return {
        "_ingestion_timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
        "_run_id": run_id,
        "_source_name": API_CONFIG["source_name"],
        "_source_endpoint": f"{API_CONFIG['base_url'].rstrip('/')}/{endpoint_path.lstrip('/')}",
        "_request_params": params,
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
    """Fetch N+1 details for a micro-batch of IDs, routing poison-pill failures to DLQ."""
    detail = cfg["detail"]
    workers = max(1, RATE_LIMITER.per_minute // 4)
    logger.info("Fetching %s details from %s (%s workers)", len(ids), detail["path"], workers)

    def _one(record_id: Any) -> Tuple[Any, Optional[Dict[str, Any]], Optional[str]]:
        try:
            res = call_api(detail["path"], {detail["id_param"]: record_id}, token_mgr, telemetry)
            payload = (res or {}).get(detail["data_key"]) if (res and detail.get("data_key")) else res
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
                logger.error("DLQ routed id=%s on %s: %s", record_id, detail["path"], err_msg)
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
    logger.info("=== %s | mode=%s | %s window/chunk(s) ===", endpoint, effective_mode, len(plans))

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
                    cfg["path"], cfg.get("data_key"), params, token_mgr, telemetry
                ):
                    row = wrap_bronze_record(item, cfg, cfg["path"], page_params, run_id, cfg.get("item_key"))
                    writer.write(row)
                    rec_id = row["_record_id"]
                    if with_details and cfg.get("detail") and rec_id is not None and rec_id not in seen_ids:
                        seen_ids.add(rec_id)
                        window_ids.append(rec_id)

            total_written += writer.count
            logger.info("[%s] chunk %s/%s: %s records written", endpoint, idx, len(plans), writer.count)

            if with_details and cfg.get("detail") and window_ids:
                for b_idx in range(0, len(window_ids), batch_size):
                    batch_ids = window_ids[b_idx : b_idx + batch_size]
                    b_suffix = f"{chunk_suffix}_b{b_idx // batch_size + 1:03d}"
                    d_ok, d_dlq = fetch_details_batch(
                        cfg, batch_ids, run_ts, run_id, b_suffix, token_mgr, telemetry
                    )
                    total_details += d_ok
                    total_dlq += d_dlq

            if chunk_wm:
                save_watermark(endpoint, chunk_wm)
                wm_after = chunk_wm
                logger.info("[%s] watermark committed: %s", endpoint, chunk_wm)

        telemetry.record_endpoint(endpoint, total_written, total_details, total_dlq, wm_before, wm_after, "success")
        return total_written
    except Exception as exc:
        telemetry.record_endpoint(
            endpoint, total_written, total_details, total_dlq, wm_before, wm_after, "failed", str(exc)
        )
        raise
```