from __future__ import annotations

"""Static tests for the DeliveryFlow HTML Documentation Portal."""

import re
from pathlib import Path


DOCS_DIR = Path("docs")
CSS_FILE = DOCS_DIR / "assets" / "css" / "documentation.css"
JS_FILE = DOCS_DIR / "assets" / "js" / "documentation.js"

HTML_FILES = [
    "index.html",
    "platform-overview.html",
    "developer-guide.html",
    "application-workflow.html",
    "streaming-architecture.html",
    "batch-lakehouse.html",
    "transportation-cost-flow.html",
    "scalability-multi-application.html",
    "architecture-decisions.html",
    "postgres-source-tables.html",
    "data-model.html",
    "superset-serving.html",
    "troubleshooting.html",
]


def test_assets_exist_and_non_empty() -> None:
    """Verify that CSS and JS assets are present and properly formed."""
    assert CSS_FILE.is_file()
    assert JS_FILE.is_file()
    css_content = CSS_FILE.read_text(encoding="utf-8")
    js_content = JS_FILE.read_text(encoding="utf-8")

    assert "--primary:" in css_content
    assert "[data-theme=\"dark\"]" in css_content or "[data-theme='dark']" in css_content
    assert "data-theme-toggle" in js_content
    assert "data-copy" in js_content


def test_all_expected_html_pages_exist() -> None:
    """Verify that all 12 core documentation portal pages exist."""
    for filename in HTML_FILES:
        file_path = DOCS_DIR / filename
        assert file_path.is_file(), f"Missing HTML page: {filename}"


def test_no_duplicate_head_or_body_tags() -> None:
    """Verify that merge collisions did not leave duplicated head/body tags."""
    for filename in HTML_FILES:
        content = (DOCS_DIR / filename).read_text(encoding="utf-8")
        head_count = len(re.findall(r"<head\b", content, re.IGNORECASE))
        body_count = len(re.findall(r"<body\b", content, re.IGNORECASE))
        assert head_count == 1, f"{filename} has {head_count} <head> tags"
        assert body_count == 1, f"{filename} has {body_count} <body> tags"


def test_internal_links_resolve() -> None:
    """Verify that all internal href links between docs resolve to existing files."""
    link_pattern = re.compile(r'href=["\']([^"\']+)["\']')
    for filename in HTML_FILES:
        content = (DOCS_DIR / filename).read_text(encoding="utf-8")
        for match in link_pattern.finditer(content):
            href = match.group(1)
            # Skip external links, anchors only, or mailto
            if href.startswith(("http://", "https://", "#", "mailto:")):
                continue
            # Strip fragment
            base_href = href.split("#")[0]
            if not base_href:
                continue
            resolved = (DOCS_DIR / base_href).resolve()
            assert resolved.exists(), f"Broken link '{href}' in {filename} -> {resolved} not found"


def test_transportation_cost_flow_html_content() -> None:
    """Verify transportation-cost-flow.html covers Dashboard 6 and all layers."""
    content = (DOCS_DIR / "transportation-cost-flow.html").read_text(encoding="utf-8")
    assert "Transportation Cost Batch Flow" in content
    assert "public.transportation_costs" in content
    assert "delivery_transportation_cost_daily_batch" in content
    assert "nessie.bronze.transportation_costs" in content
    assert "nessie.silver.transportation_costs_enriched" in content
    assert "nessie.gold.daily_transportation_cost_kpi" in content
    assert "delivery.v_transportation_cost_performance" in content
    assert "Transportation Cost &amp; Route Performance" in content or "Transportation Cost & Route Performance" in content

