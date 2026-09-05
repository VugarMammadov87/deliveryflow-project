from __future__ import annotations

"""Static tests for Transportation Cost operator commands and documentation."""

from pathlib import Path


MAKEFILE = Path("Makefile")
README = Path("README.md")
RUNBOOK = Path("docs/transportation-cost-flow.md")


def test_makefile_exposes_parameterized_transportation_cost_batch_command() -> None:
    """Verify developers can run Dashboard 6 batch processing from Make."""
    makefile = MAKEFILE.read_text(encoding="utf-8")

    assert "DATASET ?= daily-kpi" in makefile
    assert "BATCH_DATE ?=" in makefile
    assert "run-batch:" in makefile
    assert "ifeq ($(DATASET),transportation-costs)" in makefile
    assert "/opt/deliveryflow/src/etl/apps/transportation_cost_kpi_job.py" in makefile
    assert "make run-batch DATASET=transportation-costs" in makefile


def test_readme_documents_transportation_cost_batch_flow() -> None:
    """Verify the primary README includes the operator path for Dashboard 6."""
    readme = README.read_text(encoding="utf-8")

    assert "make run-batch DATASET=transportation-costs" in readme
    assert "Transportation Cost & Route Performance" in readme
    assert "docs/transportation-cost-flow.md" in readme
    assert "Iceberg Bronze" in readme
    assert "Iceberg Silver" in readme
    assert "Iceberg Gold" in readme


def test_transportation_cost_runbook_documents_full_end_to_end_flow() -> None:
    """Verify the dedicated runbook explains source-to-dashboard processing."""
    runbook = RUNBOOK.read_text(encoding="utf-8")

    expected_sections = [
        "PostgreSQL Source",
        "Airflow",
        "Spark",
        "Iceberg Bronze",
        "Iceberg Silver",
        "Iceberg Gold",
        "ClickHouse",
        "Superset Dataset",
        "Transportation Cost & Route Performance Dashboard",
    ]
    for section in expected_sections:
        assert section in runbook

    assert "public.transportation_costs" in runbook
    assert "delivery.v_transportation_cost_performance" in runbook
    assert "make import-superset-assets" in runbook
