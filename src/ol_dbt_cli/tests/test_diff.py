"""Tests for commands/diff.py — column reconciliation, audit_helper wrapping, CLI."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from ol_dbt_cli.commands import diff as diff_mod
from ol_dbt_cli.commands.diff import (
    Verdict,
    _compare_relations_sql,
    _extract_show_rows,
    _jinja_list,
    _summarize_relations,
    diff,
    reconcile_columns,
)


class TestReconcileColumns:
    def test_identical(self) -> None:
        r = reconcile_columns({"a", "b"}, {"a", "b"}, set())
        assert r.only_in_old == [] and r.only_in_new == []
        assert r.compared == ["a", "b"]
        assert not r.diverged

    def test_only_in_new(self) -> None:
        r = reconcile_columns({"a"}, {"a", "b"}, set())
        assert r.only_in_new == ["b"]
        assert r.only_in_old == []
        assert r.diverged

    def test_only_in_old(self) -> None:
        r = reconcile_columns({"a", "b"}, {"a"}, set())
        assert r.only_in_old == ["b"]
        assert r.diverged

    def test_excluded_column_is_not_a_divergence(self) -> None:
        # 'b' only in old but excluded -> not a divergence, and not compared
        r = reconcile_columns({"a", "b"}, {"a"}, {"b"})
        assert not r.diverged
        assert r.compared == ["a"]

    def test_exclude_is_case_insensitive(self) -> None:
        r = reconcile_columns({"a", "LoadedAt"}, {"a"}, {"loadedat"})
        assert not r.diverged


class TestExtractShowRows:
    def test_plain_show_object(self) -> None:
        payload = '{"node": "inline", "show": [{"x": 1}, {"x": 2}]}'
        assert _extract_show_rows(payload) == [{"x": 1}, {"x": 2}]

    def test_bare_list(self) -> None:
        assert _extract_show_rows('[{"x": 1}]') == [{"x": 1}]

    def test_log_prefixed_json(self) -> None:
        payload = (
            "12:00:00  Running with dbt=1.9\n"
            "12:00:01  Previewing inline node:\n"
            '{"node": "inline", "show": [{"in_a": true, "in_b": false, "count": 3}]}\n'
        )
        rows = _extract_show_rows(payload)
        assert rows == [{"in_a": True, "in_b": False, "count": 3}]

    def test_empty(self) -> None:
        assert _extract_show_rows("no json here") == []

    def test_log_prefixed_bare_list(self) -> None:
        # A bare JSON array preceded by log lines must still be extracted.
        payload = '12:00:00  Running with dbt=1.9\n[{"in_a": true, "in_b": true, "count": 5}]\n'
        assert _extract_show_rows(payload) == [{"in_a": True, "in_b": True, "count": 5}]

    def test_json_with_trailing_log_lines(self) -> None:
        # raw_decode must tolerate trailing text after the JSON document.
        payload = '{"show": [{"x": 1}]}\n12:00:02  Done.\n'
        assert _extract_show_rows(payload) == [{"x": 1}]


class TestSummarizeRelations:
    def test_perfect_match(self) -> None:
        rows = [{"in_a": True, "in_b": True, "count": 100}]
        counts, mismatched = _summarize_relations(rows)
        assert counts == {"old": 100, "new": 100, "delta": 0}
        assert mismatched == 0

    def test_rows_missing_from_new(self) -> None:
        rows = [
            {"in_a": True, "in_b": True, "count": 90},
            {"in_a": True, "in_b": False, "count": 10},
        ]
        counts, mismatched = _summarize_relations(rows)
        assert counts == {"old": 100, "new": 90, "delta": -10}
        assert mismatched == 10

    def test_rows_on_both_sides(self) -> None:
        rows = [
            {"in_a": True, "in_b": True, "count": 80},
            {"in_a": True, "in_b": False, "count": 5},
            {"in_a": False, "in_b": True, "count": 7},
        ]
        counts, mismatched = _summarize_relations(rows)
        assert counts == {"old": 85, "new": 87, "delta": 2}
        assert mismatched == 12


class TestSqlBuilders:
    def test_jinja_list(self) -> None:
        assert _jinja_list(["a", "b"]) == "['a', 'b']"

    def test_compare_relations_single_pk_and_exclude(self) -> None:
        sql = _compare_relations_sql("old_m", "new_m", ["id"], ["_loaded_at"], summarize=True)
        assert "ref('old_m')" in sql
        assert "ref('new_m')" in sql
        assert "primary_key='id'" in sql
        assert "exclude_columns=['_loaded_at']" in sql
        assert "summarize=true" in sql

    def test_compare_relations_composite_pk(self) -> None:
        sql = _compare_relations_sql("a", "b", ["k1", "k2"], [], summarize=False)
        assert "primary_key=['k1', 'k2']" in sql
        assert "summarize=false" in sql

    def test_compare_relations_no_pk(self) -> None:
        sql = _compare_relations_sql("a", "b", [], [], summarize=True)
        assert "primary_key" not in sql


def _make_project(tmp_path: Path, old_sql: str, new_sql: str) -> Path:
    """Create a minimal dbt project dir with two leaf models."""
    dbt_dir = tmp_path / "ol_dbt"
    (dbt_dir / "models").mkdir(parents=True)
    (dbt_dir / "dbt_project.yml").write_text("name: test\nprofile: test\n")
    (dbt_dir / "models" / "m_old.sql").write_text(old_sql)
    (dbt_dir / "models" / "m_new.sql").write_text(new_sql)
    return dbt_dir


class TestValidateIdentifiers:
    def test_accepts_plain_identifiers(self) -> None:
        # Should not raise.
        diff_mod._validate_identifiers("model name", ["dim_user", "_stg__x", "a1"])

    @pytest.mark.parametrize(
        "bad",
        ["x{{ config }}", "a b", "a'b", "1abc", "a);drop", "ref('x')", ""],
    )
    def test_rejects_injection_and_non_identifiers(self, bad: str) -> None:
        with pytest.raises(diff_mod.InvalidIdentifierError):
            diff_mod._validate_identifiers("model name", [bad])


class TestDiffCommand:
    def test_missing_project_exits_1(self, tmp_path: Path) -> None:
        with pytest.raises(SystemExit) as exc:
            diff(old="a", new="b", dbt_dir_path=str(tmp_path / "does_not_exist"))
        assert exc.value.code == 1

    def test_injection_identifier_exits_1_before_comparison(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        dbt_dir = _make_project(tmp_path, "select 1 as id", "select 1 as id")

        def boom(*a: Any, **k: Any) -> list[dict[str, Any]]:
            msg = "must reject the bad identifier before ever invoking dbt"
            raise AssertionError(msg)

        monkeypatch.setattr(diff_mod, "_run_dbt_show", boom)
        with pytest.raises(SystemExit) as exc:
            diff(old="m_old", new="m_new", primary_key=("id; drop table x",), dbt_dir_path=str(dbt_dir))
        assert exc.value.code == 1

    def test_match_exits_0(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        dbt_dir = _make_project(
            tmp_path,
            "select 1 as id, 'x' as name",
            "select 1 as id, 'x' as name",
        )
        monkeypatch.setattr(
            diff_mod,
            "_run_dbt_show",
            lambda *a, **k: [{"in_a": True, "in_b": True, "count": 10}],
        )
        # A clean match returns normally (no SystemExit).
        diff(old="m_old", new="m_new", dbt_dir_path=str(dbt_dir))

    def test_row_mismatch_exits_1(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        dbt_dir = _make_project(
            tmp_path,
            "select 1 as id, 'x' as name",
            "select 1 as id, 'x' as name",
        )

        def fake_show(inline_sql: str, *a: Any, **k: Any) -> list[dict[str, Any]]:
            if "summarize=true" in inline_sql:
                return [
                    {"in_a": True, "in_b": True, "count": 9},
                    {"in_a": True, "in_b": False, "count": 1},
                ]
            return [{"id": 42}]  # sample mismatched rows

        monkeypatch.setattr(diff_mod, "_run_dbt_show", fake_show)
        with pytest.raises(SystemExit) as exc:
            diff(old="m_old", new="m_new", dbt_dir_path=str(dbt_dir))
        assert exc.value.code == 1

    def test_schema_divergence_exits_1_without_comparing(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        dbt_dir = _make_project(
            tmp_path,
            "select 1 as id, 'x' as name",
            "select 1 as id, 'x' as name, 2 as extra",
        )

        def boom(*a: Any, **k: Any) -> list[dict[str, Any]]:
            msg = "comparison should be skipped on schema divergence"
            raise AssertionError(msg)

        monkeypatch.setattr(diff_mod, "_run_dbt_show", boom)
        with pytest.raises(SystemExit) as exc:
            diff(old="m_old", new="m_new", dbt_dir_path=str(dbt_dir))
        assert exc.value.code == 1

    def test_json_output_schema(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        import json

        dbt_dir = _make_project(
            tmp_path,
            "select 1 as id, 'x' as name",
            "select 1 as id, 'x' as name",
        )
        monkeypatch.setattr(
            diff_mod,
            "_run_dbt_show",
            lambda *a, **k: [{"in_a": True, "in_b": True, "count": 10}],
        )
        diff(old="m_old", new="m_new", dbt_dir_path=str(dbt_dir), output_format="json")
        payload = json.loads(capsys.readouterr().out)
        assert payload["verdict"] == Verdict.MATCH.value
        for key in (
            "old",
            "new",
            "target",
            "primary_key",
            "excluded_columns",
            "column_reconciliation",
            "row_counts",
            "column_mismatches",
            "sample_mismatches",
        ):
            assert key in payload
        assert set(payload["column_reconciliation"]) == {"only_in_old", "only_in_new", "compared"}
        assert payload["row_counts"] == {"old": 10, "new": 10, "delta": 0}

    def test_sample_mismatches_respect_limit(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        import json

        dbt_dir = _make_project(
            tmp_path,
            "select 1 as id, 'x' as name",
            "select 1 as id, 'x' as name",
        )
        captured_limits: list[int] = []

        def fake_show(inline_sql: str, dbt_dir: Path, target: str, limit: int) -> list[dict[str, Any]]:
            captured_limits.append(limit)
            if "summarize=true" in inline_sql:
                return [
                    {"in_a": True, "in_b": True, "count": 5},
                    {"in_a": True, "in_b": False, "count": 5},
                ]
            # honor the limit the command passed for the sample query
            return [{"id": i} for i in range(limit)]

        monkeypatch.setattr(diff_mod, "_run_dbt_show", fake_show)
        with pytest.raises(SystemExit):
            diff(old="m_old", new="m_new", dbt_dir_path=str(dbt_dir), limit=3, output_format="json")
        payload = json.loads(capsys.readouterr().out)
        assert len(payload["sample_mismatches"]) == 3
        # the sample query was invoked with the user's --limit
        assert 3 in captured_limits

    def test_dbt_failure_exits_1(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        dbt_dir = _make_project(
            tmp_path,
            "select 1 as id, 'x' as name",
            "select 1 as id, 'x' as name",
        )

        def raise_runtime(*a: Any, **k: Any) -> list[dict[str, Any]]:
            msg = "dbt show failed: boom"
            raise RuntimeError(msg)

        monkeypatch.setattr(diff_mod, "_run_dbt_show", raise_runtime)
        with pytest.raises(SystemExit) as exc:
            diff(old="m_old", new="m_new", dbt_dir_path=str(dbt_dir))
        assert exc.value.code == 1
