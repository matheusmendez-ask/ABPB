#!/usr/bin/env python3
"""
Enterprise API Documentation Scraper & Schema Inspector (v2.0).
Extracts endpoints, parameters, OAuth2 flows, rate-limit headers, error code tables,
and nested response key-value structures from OpenAPI/Swagger specs, RPC/HTML docs, or local files.
Dependencies: Pure Python Standard Library only.
"""

from __future__ import annotations

import argparse
import html.parser
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional, Tuple

COMMON_SPEC_PATHS = [
    "/openapi.json",
    "/swagger.json",
    "/v3/api-docs",
    "/v2/api-docs",
    "/api-docs",
    "/api/openapi.json",
    "/api/swagger.json",
    "/docs/openapi.json",
    "/swagger/v1/swagger.json",
]
DEFAULT_MAX_PAGES = 50
CRAWL_DELAY_SECONDS = 0.2


HEADING_TAGS = ("h1", "h2", "h3", "h4")
CODE_TAGS = ("pre", "code")
CELL_TAGS = ("td", "th")
NON_VISIBLE_TAGS = ("script", "style", "noscript", "template")


def _squash(parts: List[str]) -> str:
    return re.sub(r"\s+", " ", "".join(parts)).strip()


class StructuredHTMLDocParser(html.parser.HTMLParser):
    """
    HTML parser that preserves table headers/rows, headings, code blocks,
    hyperlinks, visible text, and potential OpenAPI spec links.

    Text capture is depth-based (not top-of-stack), so nested markup such as
    <td><code>token</code></td>, <br> inside cells, or syntax-highlighted
    <pre><code><span>...</span></code></pre> is preserved. Content of
    <script>/<style> never reaches `visible_text`, which keeps analytics and
    A/B-testing snippets out of endpoint discovery.
    """

    def __init__(self) -> None:
        super().__init__()
        self.headings: List[str] = []
        self.code_blocks: List[str] = []
        self.spec_links: List[str] = []
        self.links: List[str] = []
        self.tables: List[Dict[str, Any]] = []
        self.visible_text: List[str] = []

        self._hidden_depth = 0
        self._heading_depth = 0
        self._code_depth = 0
        self._heading_buf: List[str] = []
        self._code_buf: List[str] = []
        self._cell_buf: Optional[List[str]] = None

        self._in_table = False
        self._current_table_headers: List[str] = []
        self._current_table_rows: List[List[str]] = []
        self._current_row: List[str] = []
        self._row_has_td = False
        self._last_heading: str = "Root"

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        attr_dict = {k: (v or "") for k, v in attrs}

        if tag in NON_VISIBLE_TAGS:
            self._hidden_depth += 1
        elif tag in HEADING_TAGS:
            self._heading_depth += 1
            if self._heading_depth == 1:
                self._heading_buf = []
        elif tag in CODE_TAGS:
            self._code_depth += 1
            if self._code_depth == 1:
                self._code_buf = []
        elif tag == "br":
            self._append(" ")
        elif tag == "table":
            self._in_table = True
            self._current_table_headers = []
            self._current_table_rows = []
        elif tag == "tr" and self._in_table:
            self._current_row = []
            self._row_has_td = False
        elif tag in CELL_TAGS and self._in_table:
            self._cell_buf = []
            self._row_has_td = self._row_has_td or tag == "td"

        if tag == "a" and attr_dict.get("href"):
            self.links.append(attr_dict["href"])

        if tag in ("a", "link", "script", "redoc", "elements-api"):
            for attr_name in ("href", "src", "data-url", "spec-url", "apiDescriptionUrl"):
                val = attr_dict.get(attr_name, "")
                if val and any(
                    k in val.lower() for k in ("openapi", "swagger", "api-docs", ".json", ".yaml", ".yml")
                ):
                    self.spec_links.append(val)

    def handle_endtag(self, tag: str) -> None:
        if tag in NON_VISIBLE_TAGS:
            self._hidden_depth = max(0, self._hidden_depth - 1)
        elif tag in HEADING_TAGS and self._heading_depth:
            self._heading_depth -= 1
            text = _squash(self._heading_buf)
            if self._heading_depth == 0 and text:
                self.headings.append(text)
                self._last_heading = text
        elif tag in CODE_TAGS and self._code_depth:
            self._code_depth -= 1
            raw_code = "".join(self._code_buf).strip()
            if self._code_depth == 0 and len(raw_code) > 15:
                self.code_blocks.append(raw_code)
        elif tag in CELL_TAGS and self._cell_buf is not None:
            self._current_row.append(_squash(self._cell_buf))
            self._cell_buf = None
        elif tag == "tr" and self._in_table and self._current_row:
            if not self._row_has_td and not self._current_table_headers:
                self._current_table_headers = self._current_row
            else:
                self._current_table_rows.append(self._current_row)
            self._current_row = []
        elif tag == "table" and self._in_table:
            if self._current_table_headers or self._current_table_rows:
                self.tables.append(
                    {
                        "section": self._last_heading,
                        "headers": self._current_table_headers,
                        "rows": self._current_table_rows[:50],
                    }
                )
            self._in_table = False

    def handle_data(self, data: str) -> None:
        if self._hidden_depth:
            return
        self._append(data)

    def _append(self, data: str) -> None:
        self.visible_text.append(data)
        if self._heading_depth:
            self._heading_buf.append(data)
        if self._code_depth:
            self._code_buf.append(data)
        if self._cell_buf is not None:
            self._cell_buf.append(data)


def fetch_url(url: str, timeout: int = 15, quiet: bool = False) -> Tuple[Optional[str], Dict[str, str]]:
    """
    Fetch content and response headers from a URL using standard urllib.
    Failures return (None, {}) and are reported on stderr unless `quiet` (used for
    speculative spec probes, where 404s are expected).
    """
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; APIBronzePipelineBuilder/2.0)",
            "Accept": "application/json, text/html, application/yaml, */*",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            headers = {k.lower(): v for k, v in response.headers.items()}
            return response.read().decode(charset, errors="replace"), headers
    except (urllib.error.URLError, OSError, ValueError) as exc:
        if not quiet:
            print(f"[doc_scraper] fetch failed for {url}: {exc}", file=sys.stderr)
        return None, {}


def resolve_ref(spec: Dict[str, Any], ref: str) -> Dict[str, Any]:
    """Resolve local JSON pointer references like '#/components/schemas/Order'."""
    if not ref.startswith("#/"):
        return {}
    parts = ref.lstrip("#/").split("/")
    current: Any = spec
    for part in parts:
        if isinstance(current, dict):
            current = current.get(part, {})
        else:
            return {}
    return current if isinstance(current, dict) else {}


def extract_schema_keys(
    spec: Dict[str, Any], schema: Dict[str, Any], prefix: str = "", depth: int = 0
) -> Dict[str, str]:
    """Recursively flatten OpenAPI schema properties into key: type mappings."""
    if depth > 5 or not isinstance(schema, dict):
        return {}
    if "$ref" in schema:
        schema = resolve_ref(spec, schema["$ref"])

    keys: Dict[str, str] = {}
    # Composed schemas: merge every branch (allOf = inheritance, oneOf/anyOf = union of possible keys)
    for combinator in ("allOf", "oneOf", "anyOf"):
        for sub_schema in schema.get(combinator, []) or []:
            keys.update(extract_schema_keys(spec, sub_schema, prefix, depth + 1))

    schema_type = schema.get("type", "object")

    if schema_type == "array" and "items" in schema:
        items_schema = schema["items"]
        if isinstance(items_schema, dict) and "$ref" in items_schema:
            items_schema = resolve_ref(spec, items_schema["$ref"])
        array_prefix = f"{prefix}[]" if prefix else "[]"
        if isinstance(items_schema, dict):
            keys.update(extract_schema_keys(spec, items_schema, array_prefix, depth + 1))
        return keys

    properties = schema.get("properties", {})
    for prop_name, prop_details in properties.items():
        if not isinstance(prop_details, dict):
            continue
        if "$ref" in prop_details:
            prop_details = resolve_ref(spec, prop_details["$ref"])
        prop_type = prop_details.get("type", "any")
        prop_format = prop_details.get("format")
        full_key = f"{prefix}.{prop_name}" if prefix else prop_name
        keys[full_key] = f"{prop_type} ({prop_format})" if prop_format else str(prop_type)

        if prop_type == "object" and "properties" in prop_details:
            keys.update(extract_schema_keys(spec, prop_details, full_key, depth + 1))
        elif prop_type == "array" and "items" in prop_details:
            keys.update(extract_schema_keys(spec, prop_details, full_key, depth + 1))  # array branch adds "[]"
        elif any(c in prop_details for c in ("allOf", "oneOf", "anyOf")):
            keys.update(extract_schema_keys(spec, prop_details, full_key, depth + 1))

    return keys


def infer_json_sample_structure(sample: Any, prefix: str = "", depth: int = 0) -> Dict[str, str]:
    """Infer key-value paths and nested wrappers (root, data_key, item_key) from a JSON sample."""
    if depth > 5:
        return {}
    structure: Dict[str, str] = {}
    if isinstance(sample, dict):
        for k, v in sample.items():
            path = f"{prefix}.{k}" if prefix else k
            if isinstance(v, dict):
                structure[path] = "object"
                structure.update(infer_json_sample_structure(v, path, depth + 1))
            elif isinstance(v, list):
                structure[f"{path}[]"] = f"array (len={len(v)})"
                if v:
                    structure.update(infer_json_sample_structure(v[0], f"{path}[]", depth + 1))
            else:
                structure[path] = type(v).__name__
    elif isinstance(sample, list) and sample:
        structure["[]"] = f"root_array (len={len(sample)})"
        structure.update(infer_json_sample_structure(sample[0], "[]", depth + 1))
    return structure


def parse_openapi_spec(spec: Dict[str, Any]) -> Dict[str, Any]:
    """Parse an OpenAPI 3.x or Swagger 2.0 dictionary into an enterprise metadata summary."""
    info = spec.get("info", {})
    servers = [s.get("url") for s in spec.get("servers", []) if isinstance(s, dict) and "url" in s]
    if not servers and "host" in spec:
        scheme = (spec.get("schemes") or ["https"])[0]
        base_path = spec.get("basePath", "")
        servers = [f"{scheme}://{spec['host']}{base_path}"]

    # Security / Auth schemes including OAuth2 flows (tokenUrl, refreshUrl, scopes)
    security_schemes = (
        spec.get("components", {}).get("securitySchemes", {})
        or spec.get("securityDefinitions", {})
    )
    auth_methods: Dict[str, Any] = {}
    for name, details in security_schemes.items():
        if not isinstance(details, dict):
            continue
        auth_entry: Dict[str, Any] = {
            "type": details.get("type"),
            "in": details.get("in"),
            "key_name": details.get("name"),
            "scheme": details.get("scheme"),
        }
        # OpenAPI 3 OAuth2 flows
        if "flows" in details and isinstance(details["flows"], dict):
            auth_entry["oauth2_flows"] = details["flows"]
        # Swagger 2 OAuth2 fields
        if details.get("type") == "oauth2" and "tokenUrl" in details:
            auth_entry["oauth2_flows"] = {
                details.get("flow", "oauth2"): {
                    "tokenUrl": details.get("tokenUrl"),
                    "authorizationUrl": details.get("authorizationUrl"),
                }
            }
        auth_methods[name] = auth_entry

    endpoints = []
    paths = spec.get("paths", {})
    for path, path_item in paths.items():
        if not isinstance(path_item, dict):
            continue
        common_params = path_item.get("parameters", [])
        for method in ("get", "post", "put", "delete", "patch"):
            if method not in path_item:
                continue
            op = path_item[method]
            if not isinstance(op, dict):
                continue

            all_params = common_params + op.get("parameters", [])
            parsed_params = []
            for p in all_params:
                if not isinstance(p, dict):
                    continue
                if "$ref" in p:
                    p = resolve_ref(spec, p["$ref"])
                p_schema = p.get("schema", {}) if isinstance(p.get("schema"), dict) else {}
                parsed_params.append(
                    {
                        "key": p.get("name"),
                        "in": p.get("in"),
                        "required": p.get("required", False),
                        "type": p.get("type") or p_schema.get("type", "string"),
                        "format": p.get("format") or p_schema.get("format"),
                        "default": p.get("default", p_schema.get("default")),
                        "description": (p.get("description") or "")[:140],
                    }
                )

            # Request body & encoding (JSON vs form-urlencoded)
            body_keys: Dict[str, str] = {}
            request_encodings: List[str] = []
            req_body = op.get("requestBody", {})
            if "$ref" in req_body:
                req_body = resolve_ref(spec, req_body["$ref"])
            content = req_body.get("content", {})
            for mime, media_obj in content.items():
                request_encodings.append(mime)
                if isinstance(media_obj, dict) and "schema" in media_obj:
                    body_keys.update(extract_schema_keys(spec, media_obj["schema"]))

            # Response keys & rate-limit headers
            response_keys: Dict[str, str] = {}
            response_headers: List[str] = []
            responses = op.get("responses", {})
            status_codes = list(responses.keys())
            for code in ("200", "201", "202", "default"):
                if code in responses and isinstance(responses[code], dict):
                    resp_obj = responses[code]
                    if "$ref" in resp_obj:
                        resp_obj = resolve_ref(spec, resp_obj["$ref"])
                    if isinstance(resp_obj.get("headers"), dict):
                        response_headers.extend(resp_obj["headers"].keys())
                    resp_content = resp_obj.get("content", {})
                    for mime, media_obj in resp_content.items():
                        if "json" in mime and isinstance(media_obj, dict):
                            response_keys = extract_schema_keys(spec, media_obj.get("schema", {}))
                            break
                    if not response_keys and "schema" in resp_obj:
                        response_keys = extract_schema_keys(spec, resp_obj["schema"])
                    if response_keys:
                        break

            # Recommend pipeline strategy per endpoint
            param_names_lower = {str(p.get("key", "")).lower() for p in parsed_params} | {
                k.lower() for k in body_keys.keys()
            }
            resp_keys_lower = {k.lower() for k in response_keys.keys()}

            if "202" in status_codes or any("job" in k or "task_id" in k for k in resp_keys_lower):
                suggested_pattern = "Async Job Polling"
            elif any(
                k in param_names_lower
                for k in (
                    "updated_since",
                    "updated_at",
                    "dataatualizacao",
                    "dataminimaatualizacao",
                    "modified_after",
                    "since",
                )
            ):
                suggested_pattern = "Incremental (datetime_watermark)"
            elif any(
                k in param_names_lower
                for k in ("start_date", "datainicial", "data_ini", "from_date", "date_from")
            ):
                suggested_pattern = "Chunked Date Window (date_window)"
            elif ("{" in path and method == "get") or any(
                x in path.lower() for x in (".obter", "/detail", "/get")
            ):
                suggested_pattern = "N+1 Detail Enrichment (micro-batched with DLQ)"
            else:
                suggested_pattern = "Batch Full Snapshot (with pagination check)"

            endpoints.append(
                {
                    "path": path,
                    "method": method.upper(),
                    "request_encodings": request_encodings or (["query"] if method == "get" else ["form/json"]),
                    "summary": op.get("summary") or op.get("operationId") or "",
                    "status_codes": status_codes,
                    "response_headers": response_headers,
                    "parameters": parsed_params,
                    "request_body_keys": body_keys,
                    "response_keys": response_keys,
                    "suggested_pattern": suggested_pattern,
                }
            )

    return {
        "doc_type": "OpenAPI/Swagger",
        "title": info.get("title", "Unknown API"),
        "version": info.get("version", ""),
        "base_urls": servers,
        "authentication_schemes": auth_methods,
        "endpoint_count": len(endpoints),
        "endpoints": endpoints,
    }


def analyze_html_and_rpc_docs(html_text: str, base_url: str = "") -> Dict[str, Any]:
    """
    Extract REST & RPC endpoints (.php, .asmx, /api2/...), classify HTML tables
    (parameters vs error codes vs rate limits), and infer schemas from JSON code blocks.
    """
    parser = StructuredHTMLDocParser()
    parser.feed(html_text)
    parser.close()

    # Endpoint and rate-limit discovery only looks at what a reader sees (visible text,
    # code blocks, link targets). Inline/third-party <script> bodies (analytics, A/B
    # testing such as VWO's `j.php`) are excluded to avoid false-positive endpoints.
    searchable_text = "".join(parser.visible_text) + "\n" + "\n".join(parser.links)

    # 1. Discover standard REST endpoints (GET /v1/orders)
    rest_matches = re.findall(
        r"\b(GET|POST|PUT|DELETE|PATCH)\s+((?:https?://[^\s\"'<>]+)|(?:/[\w\-\./\{\}:?&=]+))",
        searchable_text,
    )
    discovered_endpoints = [{"method": m, "path": p} for m, p in dict.fromkeys(rest_matches)]

    # 2. Discover RPC-style endpoints (e.g. pedidos.pesquisa.php, /api2/contatos.obter.php)
    rpc_matches = re.findall(
        r"\b((?:https?://[\w\.\-]+/[\w\./\-]+/)?[\w\.]+\.(?:php|asmx|do|action))\b",
        searchable_text,
        flags=re.IGNORECASE,
    )
    for rpc_ep in dict.fromkeys(rpc_matches):
        discovered_endpoints.append({"method": "POST/GET (RPC)", "path": rpc_ep})

    # 3. Extract rate limit mentions & custom headers (e.g. x-limit-api, X-RateLimit-Remaining)
    rate_limit_headers = list(
        dict.fromkeys(
            re.findall(
                r"\b(x-[\w\-]*limit[\w\-]*|retry-after|ratelimit-[\w\-]+)\b",
                searchable_text,
                flags=re.IGNORECASE,
            )
        )
    )
    rate_limit_notes = re.findall(
        r"(\d+\s*(?:requisi[çc][õo]es|requests|chamadas|req)\s*(?:por|/)\s*(?:minuto|min|segundo|seg|hora|dia))",
        searchable_text,
        flags=re.IGNORECASE,
    )

    # 4. Classify HTML tables into Parameter Tables vs Error Code Tables
    parameter_tables = []
    error_code_tables = []
    other_tables = []

    for tbl in parser.tables:
        header_str = " ".join(tbl["headers"]).lower() + " " + tbl["section"].lower()
        if any(k in header_str for k in ("erro", "error", "código", "codigo", "status code")):
            error_code_tables.append(tbl)
        elif any(k in header_str for k in ("parâmetro", "parametro", "campo", "field", "parameter", "tipo", "formato")):
            parameter_tables.append(tbl)
        else:
            other_tables.append(tbl)

    # 5. Parse JSON samples inside <pre>/<code> blocks to infer response envelopes
    inferred_json_schemas = []
    for block in parser.code_blocks:
        first_brace = min(
            [i for i in (block.find("{"), block.find("[")) if i != -1],
            default=-1,
        )
        if first_brace != -1:
            candidate = block[first_brace:]
            try:
                parsed_json = json.loads(candidate)
                inferred_json_schemas.append(
                    {
                        "inferred_keys": infer_json_sample_structure(parsed_json),
                        "raw_preview": candidate[:300],
                    }
                )
            except Exception:
                pass

    # 6. Embedded OpenAPI spec links
    embedded_urls = re.findall(
        r'["\']((?:https?:)?//[^"\']+(?:openapi|swagger|api-docs)[^"\']*\.(?:json|yaml|yml))["\']',
        html_text,
        flags=re.IGNORECASE,
    )
    all_spec_links = list(dict.fromkeys(parser.spec_links + embedded_urls))
    if base_url:
        all_spec_links = [urllib.parse.urljoin(base_url, link) for link in all_spec_links]

    return {
        "doc_type": "HTML / RPC Documentation",
        "base_url": base_url,
        "potential_spec_links": all_spec_links[:10],
        "rate_limit_signals": {
            "discovered_headers": rate_limit_headers,
            "quota_mentions": list(dict.fromkeys(rate_limit_notes))[:10],
        },
        "headings": parser.headings[:40],
        "discovered_endpoints": discovered_endpoints[:40],
        "parameter_tables": parameter_tables[:20],
        "error_code_tables": error_code_tables[:10],
        "other_tables": other_tables[:10],
        "inferred_json_payloads": inferred_json_schemas[:8],
        "raw_code_samples": [block[:2000] for block in parser.code_blocks[:8]],
        "links": list(dict.fromkeys(parser.links)),
    }


def load_openapi(raw: Optional[str]) -> Optional[Dict[str, Any]]:
    """Return the parsed document if `raw` is a JSON OpenAPI/Swagger spec, else None."""
    if not raw:
        return None
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if isinstance(data, dict) and ("swagger" in data or "openapi" in data or "paths" in data):
        return data
    return None


def crawl_scope_prefix(url: str) -> str:
    """Directory of the start URL: '/api-docs/api' -> '/api-docs/', '/docs/' -> '/docs/'."""
    path = urllib.parse.urlparse(url).path or "/"
    return path if path.endswith("/") else path.rsplit("/", 1)[0] + "/"


def in_crawl_scope(link: str, origin_netloc: str, prefix: str) -> bool:
    """Same host, same path prefix, and looks like an HTML page (not an asset)."""
    parsed = urllib.parse.urlparse(link)
    if parsed.scheme not in ("http", "https") or parsed.netloc != origin_netloc:
        return False
    if not parsed.path.startswith(prefix):
        return False
    return not re.search(r"\.(?:js|css|png|jpe?g|gif|svg|ico|woff2?|ttf|pdf|zip)$", parsed.path, re.IGNORECASE)


def crawl_html_docs(
    start_url: str, start_html: str, depth: int, max_pages: int, delay: float = CRAWL_DELAY_SECONDS
) -> Dict[str, Any]:
    """
    Breadth-first crawl of child documentation pages under the start URL's path prefix.
    Needed for portals (e.g. Tiny ERP) whose index only links to one page per endpoint.
    """
    origin = urllib.parse.urlparse(start_url).netloc
    prefix = crawl_scope_prefix(start_url)
    seen = {urllib.parse.urldefrag(start_url)[0]}
    queue: List[Tuple[str, str, int]] = [(start_url, start_html, 0)]
    pages: List[Dict[str, Any]] = []
    failed: List[str] = []

    while queue and len(pages) < max_pages:
        url, html_text, level = queue.pop(0)
        summary = analyze_html_and_rpc_docs(html_text, base_url=url)
        links = summary.pop("links")
        summary.pop("doc_type", None)
        pages.append({"url": url, "depth": level, **summary})

        if level >= depth:
            continue
        for link in links:
            child = urllib.parse.urldefrag(urllib.parse.urljoin(url, link))[0]
            if child in seen or not in_crawl_scope(child, origin, prefix):
                continue
            seen.add(child)
            if len(pages) + len(queue) >= max_pages:
                break
            time.sleep(delay)  # ponytail: fixed politeness delay, make it a flag if a portal throttles harder
            child_html, _ = fetch_url(child)
            if child_html is None:
                failed.append(child)
                continue
            queue.append((child, child_html, level + 1))

    endpoints: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for page in pages:
        for ep in page["discovered_endpoints"]:
            entry = endpoints.setdefault((ep["method"], ep["path"]), {**ep, "found_on": []})
            entry["found_on"].append(page["url"])

    return {
        "doc_type": "HTML / RPC Documentation (crawled)",
        "start_url": start_url,
        "crawl_scope": f"{origin}{prefix}",
        "crawl_depth": depth,
        "pages_crawled": len(pages),
        "pages_failed": failed,
        "max_pages_reached": len(pages) >= max_pages,
        "discovered_endpoints": list(endpoints.values()),
        "rate_limit_signals": {
            "discovered_headers": list(
                dict.fromkeys(h for p in pages for h in p["rate_limit_signals"]["discovered_headers"])
            ),
            "quota_mentions": list(
                dict.fromkeys(q for p in pages for q in p["rate_limit_signals"]["quota_mentions"])
            )[:20],
        },
        "potential_spec_links": list(dict.fromkeys(s for p in pages for s in p["potential_spec_links"]))[:10],
        "pages": pages,
    }


def inspect_target(target: str, crawl_depth: int = 0, max_pages: int = DEFAULT_MAX_PAGES) -> Dict[str, Any]:
    """Inspect a URL or local file path and return structured API documentation metadata."""
    if not target.startswith(("http://", "https://")):
        try:
            with open(target, "r", encoding="utf-8") as fh:
                raw = fh.read()
        except OSError as exc:
            return {"error": f"Failed to read local file '{target}': {exc}"}
        spec = load_openapi(raw)
        if spec is not None:
            return parse_openapi_spec(spec)
        result = analyze_html_and_rpc_docs(raw, base_url=target)
        result.pop("links")
        return result

    raw_content, resp_headers = fetch_url(target)
    spec = load_openapi(raw_content)
    if spec is not None:
        result = parse_openapi_spec(spec)
        result["live_response_headers"] = resp_headers
        return result

    # Probe common OpenAPI spec paths relative to the URL origin
    parsed_url = urllib.parse.urlparse(target)
    base_origin = f"{parsed_url.scheme}://{parsed_url.netloc}"
    for spec_path in COMMON_SPEC_PATHS:
        probe_url = urllib.parse.urljoin(base_origin, spec_path)
        spec = load_openapi(fetch_url(probe_url, timeout=6, quiet=True)[0])
        if spec is not None:
            result = parse_openapi_spec(spec)
            result["discovered_spec_url"] = probe_url
            return result

    if not raw_content:
        return {"error": f"Unable to retrieve or parse documentation from {target}"}

    html_summary = analyze_html_and_rpc_docs(raw_content, base_url=target)
    html_summary.pop("links")
    unparsed_yaml_specs = []
    for spec_link in html_summary.get("potential_spec_links", []):
        if re.search(r"\.ya?ml(?:$|\?)", spec_link, re.IGNORECASE):
            unparsed_yaml_specs.append(spec_link)  # stdlib has no YAML parser
            continue
        spec = load_openapi(fetch_url(spec_link, timeout=6, quiet=True)[0])
        if spec is not None:
            result = parse_openapi_spec(spec)
            result["discovered_spec_url"] = spec_link
            return result

    if crawl_depth > 0:
        html_summary = crawl_html_docs(target, raw_content, crawl_depth, max_pages)
    if unparsed_yaml_specs:
        html_summary["unparsed_yaml_spec_links"] = unparsed_yaml_specs
    return html_summary


def main() -> None:
    arg_parser = argparse.ArgumentParser(
        description="Enterprise API Documentation Scraper & Schema Inspector (v2.0)."
    )
    arg_parser.add_argument("target", help="URL or local file path to the API documentation or OpenAPI spec.")
    arg_parser.add_argument(
        "--crawl-depth",
        type=int,
        default=0,
        help="HTML docs only: follow links under the start URL's path prefix up to N levels (default: 0).",
    )
    arg_parser.add_argument(
        "--max-pages",
        type=int,
        default=DEFAULT_MAX_PAGES,
        help=f"Safety cap on pages fetched while crawling (default: {DEFAULT_MAX_PAGES}).",
    )
    args = arg_parser.parse_args()
    if args.crawl_depth < 0 or args.max_pages < 1:
        arg_parser.error("--crawl-depth must be >= 0 and --max-pages >= 1")

    result = inspect_target(args.target, crawl_depth=args.crawl_depth, max_pages=args.max_pages)
    sys.stdout.reconfigure(encoding="utf-8")  # Windows redirects default to cp1252
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()