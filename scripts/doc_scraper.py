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


class StructuredHTMLDocParser(html.parser.HTMLParser):
    """
    HTML parser that preserves table headers/rows, headings, code blocks,
    and potential OpenAPI spec links.
    """

    def __init__(self) -> None:
        super().__init__()
        self._tag_stack: List[str] = []
        self._current_text: List[str] = []
        self.headings: List[str] = []
        self.code_blocks: List[str] = []
        self.spec_links: List[str] = []
        self.tables: List[Dict[str, Any]] = []

        self._in_table = False
        self._current_table_headers: List[str] = []
        self._current_table_rows: List[List[str]] = []
        self._current_row: List[str] = []
        self._current_cell_is_header = False
        self._last_heading: str = "Root"

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        attr_dict = {k: (v or "") for k, v in attrs}
        self._tag_stack.append(tag)

        if tag in ("h1", "h2", "h3", "h4", "code", "pre", "td", "th"):
            self._current_text = []

        if tag == "table":
            self._in_table = True
            self._current_table_headers = []
            self._current_table_rows = []
        elif tag == "tr" and self._in_table:
            self._current_row = []
        elif tag == "th" and self._in_table:
            self._current_cell_is_header = True
        elif tag == "td" and self._in_table:
            self._current_cell_is_header = False

        if tag in ("a", "link", "script", "redoc", "elements-api"):
            for attr_name in ("href", "src", "data-url", "spec-url", "apiDescriptionUrl"):
                val = attr_dict.get(attr_name, "")
                if val and any(
                    k in val.lower() for k in ("openapi", "swagger", "api-docs", ".json", ".yaml", ".yml")
                ):
                    self.spec_links.append(val)

    def handle_endtag(self, tag: str) -> None:
        text = "".join(self._current_text).strip()
        text = re.sub(r"\s+", " ", text)

        if tag in ("h1", "h2", "h3", "h4") and text:
            self.headings.append(text)
            self._last_heading = text
            self._current_text = []
        elif tag in ("code", "pre"):
            raw_code = "".join(self._current_text).strip()
            if len(raw_code) > 15:
                self.code_blocks.append(raw_code[:2000])
            self._current_text = []
        elif tag in ("td", "th") and self._in_table:
            self._current_row.append(text)
            self._current_text = []
        elif tag == "tr" and self._in_table and self._current_row:
            if self._current_cell_is_header and not self._current_table_headers:
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

        if self._tag_stack and self._tag_stack[-1] == tag:
            self._tag_stack.pop()

    def handle_data(self, data: str) -> None:
        if self._tag_stack and self._tag_stack[-1] in ("h1", "h2", "h3", "h4", "code", "pre", "td", "th"):
            self._current_text.append(data)


def fetch_url(url: str, timeout: int = 15) -> Tuple[Optional[str], Dict[str, str]]:
    """Fetch content and response headers from a URL using standard urllib."""
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
    except Exception:
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
            keys.update(extract_schema_keys(spec, prop_details, f"{full_key}[]", depth + 1))

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
                        "default": p.get("default") or p_schema.get("default"),
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

    # 1. Discover standard REST endpoints (GET /v1/orders)
    rest_matches = re.findall(
        r"\b(GET|POST|PUT|DELETE|PATCH)\s+((?:https?://[^\s\"'<>]+)|(?:/[\w\-\./\{\}:?&=]+))",
        html_text,
    )
    discovered_endpoints = [{"method": m, "path": p} for m, p in dict.fromkeys(rest_matches)]

    # 2. Discover RPC-style endpoints (e.g. pedidos.pesquisa.php, /api2/contatos.obter.php)
    rpc_matches = re.findall(
        r"\b((?:https?://[\w\.\-]+/[\w\./\-]+/)?[\w\.]+\.(?:php|asmx|do|action))\b",
        html_text,
        flags=re.IGNORECASE,
    )
    for rpc_ep in dict.fromkeys(rpc_matches):
        discovered_endpoints.append({"method": "POST/GET (RPC)", "path": rpc_ep})

    # 3. Extract rate limit mentions & custom headers (e.g. x-limit-api, X-RateLimit-Remaining)
    rate_limit_headers = list(
        dict.fromkeys(
            re.findall(
                r"\b(x-[\w\-]*limit[\w\-]*|retry-after|ratelimit-[\w\-]+)\b",
                html_text,
                flags=re.IGNORECASE,
            )
        )
    )
    rate_limit_notes = re.findall(
        r"(\d+\s*(?:requisi[çc][õo]es|requests|chamadas|req)\s*(?:por|/)\s*(?:minuto|min|segundo|seg|hora|dia))",
        html_text,
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
        "rate_limit_Signals": {
            "discovered_headers": rate_limit_headers,
            "quota_mentions": list(dict.fromkeys(rate_limit_notes))[:10],
        },
        "headings": parser.headings[:40],
        "discovered_endpoints": discovered_endpoints[:40],
        "parameter_tables": parameter_tables[:20],
        "error_code_tables": error_code_tables[:10],
        "other_tables": other_tables[:10],
        "inferred_json_payloads": inferred_json_schemas[:8],
        "raw_code_samples": parser.code_blocks[:8],
    }


def inspect_target(target: str) -> Dict[str, Any]:
    """Inspect a URL or local file path and return structured API documentation metadata."""
    if not target.startswith(("http://", "https://")):
        try:
            raw = open(target, "r", encoding="utf-8").read()
            data = json.loads(raw)
            if "swagger" in data or "openapi" in data or "paths" in data:
                return parse_openapi_spec(data)
            return analyze_html_and_rpc_docs(raw, base_url=target)
        except json.JSONDecodeError:
            raw = open(target, "r", encoding="utf-8").read()
            return analyze_html_and_rpc_docs(raw, base_url=target)
        except Exception as exc:
            return {"error": f"Failed to parse local file '{target}': {exc}"}

    raw_content, resp_headers = fetch_url(target)
    if raw_content:
        try:
            data = json.loads(raw_content)
            if "swagger" in data or "openapi" in data or "paths" in data:
                result = parse_openapi_spec(data)
                result["live_response_headers"] = resp_headers
                return result
        except json.JSONDecodeError:
            pass

    # Probe common OpenAPI spec paths relative to the URL origin
    parsed_url = urllib.parse.urlparse(target)
    base_origin = f"{parsed_url.scheme}://{parsed_url.netloc}"
    for spec_path in COMMON_SPEC_PATHS:
        probe_url = urllib.parse.urljoin(base_origin, spec_path)
        probe_content, _ = fetch_url(probe_url, timeout=6)
        if probe_content:
            try:
                data = json.loads(probe_content)
                if "swagger" in data or "openapi" in data or "paths" in data:
                    result = parse_openapi_spec(data)
                    result["discovered_spec_url"] = probe_url
                    return result
            except json.JSONDecodeError:
                continue

    if raw_content:
        html_summary = analyze_html_and_rpc_docs(raw_content, base_url=target)
        for spec_link in html_summary.get("potential_spec_links", []):
            spec_content, _ = fetch_url(spec_link, timeout=6)
            if spec_content:
                try:
                    data = json.loads(spec_content)
                    if "swagger" in data or "openapi" in data or "paths" in data:
                        result = parse_openapi_spec(data)
                        result["discovered_spec_url"] = spec_link
                        return result
                except json.JSONDecodeError:
                    continue
        return html_summary

    return {"error": f"Unable to retrieve or parse documentation from {target}"}


def main() -> None:
    arg_parser = argparse.ArgumentParser(
        description="Enterprise API Documentation Scraper & Schema Inspector (v2.0)."
    )
    arg_parser.add_argument("target", help="URL or local file path to the API documentation or OpenAPI spec.")
    args = arg_parser.parse_args()

    result = inspect_target(args.target)
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()