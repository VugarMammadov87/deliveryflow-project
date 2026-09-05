from __future__ import annotations

"""Reusable data-quality checks for Spark batch datasets."""

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from etl.framework.metadata import DatasetMetadata


@dataclass(frozen=True)
class QualityResult:
    """Result from a data-quality rule group."""

    rule: str
    failed_count: int

    @property
    def ok(self) -> bool:
        """Return whether the rule group passed."""
        return self.failed_count == 0


class QualityValidator:
    """Validate metadata-backed quality rules for Spark DataFrames.

    The class imports PySpark functions lazily so framework metadata tests can
    run in lightweight Python environments that do not have Spark installed.
    """

    def __init__(self, metadata: DatasetMetadata) -> None:
        self.metadata = metadata

    def validate_columns(self, columns: list[str] | tuple[str, ...]) -> None:
        """Fail fast when metadata references columns missing from a DataFrame."""
        available = set(columns)
        expected = set(self.metadata.quality.get("not_null", []))
        expected.update(self.metadata.quality.get("non_negative", []))
        equality = self.metadata.quality.get("equality", {})
        if isinstance(equality, dict):
            for target_column, rule in equality.items():
                expected.add(str(target_column))
                formula = rule.get("formula") if isinstance(rule, dict) else None
                if formula:
                    expected.update(self._formula_columns(formula))

        missing = sorted(expected.difference(available))
        if missing:
            raise ValueError(f"Dataset {self.metadata.dataset_id} missing quality columns: {', '.join(missing)}")

    def validate_or_raise(self, dataframe: object) -> list[QualityResult]:
        """Run supported quality checks and raise on the first failure."""
        self.validate_columns(list(dataframe.columns))
        results = self.validate(dataframe)
        failures = [result for result in results if not result.ok]
        if failures:
            details = ", ".join(f"{result.rule} failed_count={result.failed_count}" for result in failures)
            raise ValueError(f"Dataset {self.metadata.dataset_id} quality validation failed: {details}")
        return results

    def validate(self, dataframe: object) -> list[QualityResult]:
        """Return quality results for not-null, non-negative, and equality rules."""
        from pyspark.sql import functions as F

        rules: list[tuple[str, Any]] = []
        not_null_columns = self.metadata.quality.get("not_null", [])
        if not_null_columns:
            condition = None
            for column in not_null_columns:
                column_condition = F.col(str(column)).isNull()
                condition = column_condition if condition is None else condition | column_condition
            rules.append(("not_null", condition))

        non_negative_columns = self.metadata.quality.get("non_negative", [])
        if non_negative_columns:
            condition = None
            for column in non_negative_columns:
                column_condition = F.col(str(column)) < 0
                condition = column_condition if condition is None else condition | column_condition
            rules.append(("non_negative", condition))

        equality = self.metadata.quality.get("equality", {})
        if isinstance(equality, dict):
            for target_column, rule in equality.items():
                formula = rule.get("formula") if isinstance(rule, dict) else None
                if formula:
                    expected = self._sum_expression(F, formula)
                    condition = F.abs(F.col(str(target_column)).cast("double") - expected) > F.lit(0.01)
                    rules.append((f"equality:{target_column}", condition))

        return [QualityResult(rule=name, failed_count=dataframe.filter(condition).count()) for name, condition in rules]

    @staticmethod
    def _sum_expression(functions: Any, formula: str) -> Any:
        """Build a Spark expression for simple plus-only equality formulas."""
        columns = QualityValidator._formula_columns(formula)
        if not columns:
            raise ValueError("Equality formula must contain at least one column")

        expression = None
        for column in columns:
            part = functions.coalesce(functions.col(column).cast("double"), functions.lit(float(Decimal("0"))))
            expression = part if expression is None else expression + part
        return expression

    @staticmethod
    def _formula_columns(formula: str) -> list[str]:
        """Return column names from a simple plus-only formula."""
        return [column.strip() for column in formula.split("+") if column.strip()]
