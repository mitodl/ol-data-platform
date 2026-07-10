"""Tests for the dimensional-layering lint (enforces #2072 DoD)."""

from __future__ import annotations

from pathlib import Path

from ol_dbt_cli.commands.validate import (
    Severity,
    ValidationReport,
    _check_dimensional_layering,
    _compute_layering_violations,
)
from ol_dbt_cli.lib.dimensional_layering import (
    LayeringViolation,
    classify_layer,
    find_violations_from_manifest,
    find_violations_from_sql,
    load_baseline,
    render_baseline,
    write_baseline,
)
from ol_dbt_cli.lib.manifest import ManifestModel, ManifestRegistry
from ol_dbt_cli.lib.sql_parser import ParsedModel


def _model(name: str, path: str, depends_on: list[str] | None = None) -> ManifestModel:
    return ManifestModel(
        unique_id=f"model.pkg.{name}",
        name=name,
        resource_type="model",
        original_file_path=path,
        schema="",
        database="",
        depends_on=depends_on or [],
    )


def _registry(*models: ManifestModel) -> ManifestRegistry:
    reg = ManifestRegistry()
    reg.nodes = {m.unique_id: m for m in models}
    reg.by_name = {m.name: m for m in models}
    return reg


# ---------------------------------------------------------------------------
# classify_layer
# ---------------------------------------------------------------------------


class TestClassifyLayer:
    def test_marts(self) -> None:
        assert classify_layer("models/marts/marts__combined__orders.sql") == "marts"

    def test_reporting(self) -> None:
        assert classify_layer("models/reporting/foo/bar.sql") == "reporting"

    def test_staging_nested(self) -> None:
        assert classify_layer("models/staging/mitxonline/stg__x.sql") == "staging"

    def test_intermediate(self) -> None:
        assert classify_layer("models/intermediate/int__x.sql") == "intermediate"

    def test_dimensional(self) -> None:
        assert classify_layer("models/dimensional/dim_user.sql") == "dimensional"

    def test_absolute_path(self) -> None:
        assert classify_layer(Path("/repo/src/ol_dbt/models/marts/x.sql")) == "marts"

    def test_not_under_models(self) -> None:
        assert classify_layer("macros/foo.sql") is None

    def test_models_is_last_segment(self) -> None:
        assert classify_layer("a/b/models") is None


# ---------------------------------------------------------------------------
# find_violations_from_manifest
# ---------------------------------------------------------------------------


class TestFindViolationsFromManifest:
    def test_flags_mart_referencing_intermediate(self) -> None:
        intermediate = _model("int__orders", "models/intermediate/int__orders.sql")
        mart = _model(
            "marts__orders",
            "models/marts/marts__orders.sql",
            depends_on=[intermediate.unique_id],
        )
        violations = find_violations_from_manifest(_registry(intermediate, mart))
        assert len(violations) == 1
        assert violations[0].key == "marts__orders -> int__orders"
        assert violations[0].child_layer == "marts"
        assert violations[0].parent_layer == "intermediate"

    def test_flags_reporting_referencing_staging(self) -> None:
        staging = _model("stg__users", "models/staging/x/stg__users.sql")
        report = _model(
            "usage_report",
            "models/reporting/usage_report.sql",
            depends_on=[staging.unique_id],
        )
        violations = find_violations_from_manifest(_registry(staging, report))
        assert [v.key for v in violations] == ["usage_report -> stg__users"]

    def test_dimensional_reference_is_allowed(self) -> None:
        dim = _model("dim_user", "models/dimensional/dim_user.sql")
        mart = _model("marts__x", "models/marts/marts__x.sql", depends_on=[dim.unique_id])
        assert find_violations_from_manifest(_registry(dim, mart)) == []

    def test_mart_to_mart_is_allowed(self) -> None:
        mart_a = _model("marts__a", "models/marts/marts__a.sql")
        mart_b = _model("marts__b", "models/marts/marts__b.sql", depends_on=[mart_a.unique_id])
        assert find_violations_from_manifest(_registry(mart_a, mart_b)) == []

    def test_staging_to_intermediate_is_not_a_violation(self) -> None:
        # Only marts/reporting are consumers; staging referencing intermediate is fine.
        intermediate = _model("int__x", "models/intermediate/int__x.sql")
        staging = _model("stg__x", "models/staging/stg__x.sql", depends_on=[intermediate.unique_id])
        assert find_violations_from_manifest(_registry(staging, intermediate)) == []

    def test_non_model_parent_ignored(self) -> None:
        source = ManifestModel(
            unique_id="source.pkg.raw.users",
            name="users",
            resource_type="source",
            original_file_path="models/staging/_sources.yml",
            schema="",
            database="",
        )
        reg = ManifestRegistry()
        mart = _model("marts__x", "models/marts/marts__x.sql", depends_on=[source.unique_id])
        reg.nodes = {source.unique_id: source, mart.unique_id: mart}
        reg.by_name = {"marts__x": mart}
        assert find_violations_from_manifest(reg) == []


# ---------------------------------------------------------------------------
# find_violations_from_sql (fallback)
# ---------------------------------------------------------------------------


class TestFindViolationsFromSql:
    def test_flags_via_refs(self) -> None:
        parsed = {
            "marts__orders": ParsedModel(name="marts__orders", refs=["int__orders", "dim_user"]),
            "int__orders": ParsedModel(name="int__orders"),
            "dim_user": ParsedModel(name="dim_user"),
        }
        layers = {
            "marts__orders": "marts",
            "int__orders": "intermediate",
            "dim_user": "dimensional",
        }
        violations = find_violations_from_sql(parsed, layers)
        assert [v.key for v in violations] == ["marts__orders -> int__orders"]

    def test_dedupes_repeated_refs(self) -> None:
        parsed = {
            "rep": ParsedModel(name="rep", refs=["stg__x", "stg__x"]),
            "stg__x": ParsedModel(name="stg__x"),
        }
        layers = {"rep": "reporting", "stg__x": "staging"}
        assert len(find_violations_from_sql(parsed, layers)) == 1

    def test_unknown_layer_ignored(self) -> None:
        parsed = {"marts__x": ParsedModel(name="marts__x", refs=["mystery"])}
        layers: dict[str, str | None] = {"marts__x": "marts", "mystery": None}
        assert find_violations_from_sql(parsed, layers) == []


# ---------------------------------------------------------------------------
# baseline I/O
# ---------------------------------------------------------------------------


class TestBaseline:
    def test_roundtrip(self, tmp_path: Path) -> None:
        violations = [
            LayeringViolation("marts__a", "int__x", "marts", "intermediate"),
            LayeringViolation("rep_b", "stg__y", "reporting", "staging"),
        ]
        path = tmp_path / "baseline.txt"
        write_baseline(path, violations)
        assert load_baseline(path) == {"marts__a -> int__x", "rep_b -> stg__y"}

    def test_load_missing_returns_empty(self, tmp_path: Path) -> None:
        assert load_baseline(tmp_path / "nope.txt") == set()

    def test_write_creates_missing_parent_dirs(self, tmp_path: Path) -> None:
        # A custom --baseline-file path whose parent doesn't exist yet must not crash.
        path = tmp_path / "config" / "nested" / "baseline.txt"
        write_baseline(path, [LayeringViolation("marts__a", "int__x", "marts", "intermediate")])
        assert load_baseline(path) == {"marts__a -> int__x"}

    def test_comments_and_blanks_ignored(self, tmp_path: Path) -> None:
        path = tmp_path / "baseline.txt"
        path.write_text("# header\n\nmarts__a -> int__x\n  # indented comment\nrep_b -> stg__y  # trailing\n")
        assert load_baseline(path) == {"marts__a -> int__x", "rep_b -> stg__y"}

    def test_render_is_sorted_and_deduped(self) -> None:
        violations = [
            LayeringViolation("z", "int__a", "marts", "intermediate"),
            LayeringViolation("a", "int__b", "marts", "intermediate"),
            LayeringViolation("a", "int__b", "marts", "intermediate"),
        ]
        body = [ln for ln in render_baseline(violations).splitlines() if ln and not ln.startswith("#")]
        assert body == ["a -> int__b", "z -> int__a"]


# ---------------------------------------------------------------------------
# _check_dimensional_layering (validate.py integration)
# ---------------------------------------------------------------------------


class TestCheckDimensionalLayering:
    def _parsed(self, refs_by_name: dict[str, list[str]]) -> dict[str, ParsedModel]:
        return {name: ParsedModel(name=name, refs=refs) for name, refs in refs_by_name.items()}

    def _paths(self, layer_by_name: dict[str, str]) -> dict[str, Path]:
        return {name: Path(f"models/{layer}/{name}.sql") for name, layer in layer_by_name.items()}

    def test_new_violation_errors(self) -> None:
        parsed = self._parsed({"marts__x": ["int__y"], "int__y": []})
        paths = self._paths({"marts__x": "marts", "int__y": "intermediate"})
        report = ValidationReport()
        _check_dimensional_layering(None, parsed, paths, baseline=set(), report=report)
        assert len(report.errors) == 1
        assert report.errors[0].check == "dimensional_layering"
        assert report.errors[0].model == "marts__x"

    def test_baselined_violation_does_not_error(self) -> None:
        parsed = self._parsed({"marts__x": ["int__y"], "int__y": []})
        paths = self._paths({"marts__x": "marts", "int__y": "intermediate"})
        report = ValidationReport()
        _check_dimensional_layering(None, parsed, paths, baseline={"marts__x -> int__y"}, report=report)
        assert report.errors == []
        infos = [i for i in report.issues if i.severity == Severity.INFO]
        assert any("tolerated by baseline" in i.message for i in infos)

    def test_stale_baseline_entry_reported(self) -> None:
        # No current violations, but the baseline lists one → INFO to prompt cleanup.
        parsed = self._parsed({"marts__x": ["dim_user"], "dim_user": []})
        paths = self._paths({"marts__x": "marts", "dim_user": "dimensional"})
        report = ValidationReport()
        _check_dimensional_layering(None, parsed, paths, baseline={"marts__x -> int__gone"}, report=report)
        assert report.errors == []
        assert any("Resolved baseline entry" in i.message for i in report.issues)

    def test_manifest_takes_precedence_over_sql(self) -> None:
        # SQL fallback would find nothing (no refs), but the manifest has the edge.
        intermediate = _model("int__y", "models/intermediate/int__y.sql")
        mart = _model("marts__x", "models/marts/marts__x.sql", depends_on=[intermediate.unique_id])
        manifest = _registry(intermediate, mart)
        parsed = self._parsed({"marts__x": [], "int__y": []})
        paths = self._paths({"marts__x": "marts", "int__y": "intermediate"})
        violations = _compute_layering_violations(manifest, parsed, paths)
        assert [v.key for v in violations] == ["marts__x -> int__y"]
