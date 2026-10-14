"""Stdlib-only regression tests for scripts/doc_scraper.py. Run: python -m unittest discover tests"""

import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import doc_scraper as ds  # noqa: E402


class HtmlParserTests(unittest.TestCase):
    def parse(self, html: str) -> ds.StructuredHTMLDocParser:
        parser = ds.StructuredHTMLDocParser()
        parser.feed(html)
        return parser

    def test_cell_text_survives_nested_code_and_br(self):
        p = self.parse(
            "<table><tr><th>Parametro</th><th>Tipo</th></tr>"
            "<tr><td><code>token</code></td><td>string<br>obrigatorio</td></tr></table>"
        )
        self.assertEqual(p.tables[0]["headers"], ["Parametro", "Tipo"])
        self.assertEqual(p.tables[0]["rows"], [["token", "string obrigatorio"]])

    def test_syntax_highlighted_code_block_is_captured(self):
        p = self.parse('<pre><code><span>{</span><span>"retorno": {"status": "OK"}}</span></code></pre>')
        self.assertEqual(p.code_blocks, ['{"retorno": {"status": "OK"}}'])

    def test_large_json_sample_is_not_truncated_before_parsing(self):
        items = ",".join(f'{{"id": {i}, "nome": "item {i}"}}' for i in range(200))
        result = ds.analyze_html_and_rpc_docs(f'<pre>{{"itens": [{items}]}}</pre>')
        self.assertIn("itens[].nome", result["inferred_json_payloads"][0]["inferred_keys"])


class EndpointDiscoveryTests(unittest.TestCase):
    def test_script_bodies_do_not_produce_endpoints(self):
        html = (
            '<script>window._vwo="https://dev.visualwebsiteoptimizer.com/j.php?a=1"</script>'
            "<p>URL: https://api.tiny.com.br/api2/pedidos.pesquisa.php</p>"
        )
        paths = [e["path"] for e in ds.analyze_html_and_rpc_docs(html)["discovered_endpoints"]]
        self.assertEqual(paths, ["https://api.tiny.com.br/api2/pedidos.pesquisa.php"])


class CrawlTests(unittest.TestCase):
    PAGES = {
        "https://docs.example.com/api-docs/pedidos": (
            "<p>POST https://api.example.com/api2/pedidos.pesquisa.php</p>"
            '<a href="api2-pedidos-obter">obter</a>'
        ),
        "https://docs.example.com/api-docs/api2-pedidos-obter": (
            "<p>https://api.example.com/api2/pedido.obter.php</p>"
        ),
    }
    INDEX = (
        '<a href="pedidos">Pedidos</a>'
        '<a href="/blog/post">fora do prefixo</a>'
        '<a href="https://other.com/api-docs/x">outro host</a>'
        '<a href="app.js">asset</a>'
    )

    def fake_fetch(self, url, timeout=15, quiet=False):
        return self.PAGES.get(url), {}

    def crawl(self, depth, max_pages=50):
        with mock.patch.object(ds, "fetch_url", side_effect=self.fake_fetch):
            return ds.crawl_html_docs("https://docs.example.com/api-docs/api", self.INDEX, depth, max_pages, delay=0)

    def test_scope_prefix_is_start_url_directory(self):
        self.assertEqual(ds.crawl_scope_prefix("https://h/api-docs/api"), "/api-docs/")
        self.assertEqual(ds.crawl_scope_prefix("https://h/docs/"), "/docs/")

    def test_follows_only_in_scope_links_up_to_depth(self):
        result = self.crawl(depth=1)
        self.assertEqual(
            [p["url"] for p in result["pages"]],
            ["https://docs.example.com/api-docs/api", "https://docs.example.com/api-docs/pedidos"],
        )
        result = self.crawl(depth=2)
        paths = {e["path"] for e in result["discovered_endpoints"]}
        self.assertIn("https://api.example.com/api2/pedido.obter.php", paths)

    def test_max_pages_caps_the_crawl(self):
        result = self.crawl(depth=5, max_pages=1)
        self.assertEqual(result["pages_crawled"], 1)
        self.assertTrue(result["max_pages_reached"])


class OpenApiTests(unittest.TestCase):
    def test_allof_and_arrays_are_flattened(self):
        spec = {
            "components": {"schemas": {"Base": {"properties": {"id": {"type": "integer"}}}}},
        }
        schema = {
            "allOf": [{"$ref": "#/components/schemas/Base"}],
            "properties": {"tags": {"type": "array", "items": {"properties": {"nome": {"type": "string"}}}}},
        }
        keys = ds.extract_schema_keys(spec, schema)
        self.assertEqual(keys, {"id": "integer", "tags": "array", "tags[].nome": "string"})

    def test_non_dict_json_is_not_treated_as_spec(self):
        self.assertIsNone(ds.load_openapi("123"))
        self.assertIsNone(ds.load_openapi("[1, 2]"))
        self.assertIsNotNone(ds.load_openapi('{"openapi": "3.0.0", "paths": {}}'))


if __name__ == "__main__":
    unittest.main()
