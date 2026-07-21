"""Tests for commands/diff.py — column reconciliation, audit_helper wrapping, CLI."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from ol_dbt_cli.commands import diff as diff_mod
from ol_dbt_cli.commands.diff import (
    Verdict,
    _compare_column_sql,
    _compare_relations_sql,
    _extract_show_rows,
    _format_sample_mismatches,
    _jinja_list,
    _relation_jinja,
    _resolve_raw_columns,
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

    def test_no_json_document_returns_none(self) -> None:
        # No JSON found at all is an anomaly (None), NOT an empty result ([]).
        assert _extract_show_rows("no json here") is None
        assert _extract_show_rows("") is None
        assert _extract_show_rows("12:00 [info] running") is None

    def test_explicit_empty_result_is_empty_list(self) -> None:
        # A genuinely empty result set is [] (distinct from the None anomaly).
        assert _extract_show_rows('{"show": []}') == []
        assert _extract_show_rows("[]") == []
        assert _extract_show_rows('12:00:00  Preview\n{"show": []}\n') == []

    def test_log_prefixed_bare_list(self) -> None:
        # A bare JSON array preceded by log lines must still be extracted.
        payload = '12:00:00  Running with dbt=1.9\n[{"in_a": true, "in_b": true, "count": 5}]\n'
        assert _extract_show_rows(payload) == [{"in_a": True, "in_b": True, "count": 5}]

    def test_json_with_trailing_log_lines(self) -> None:
        # raw_decode must tolerate trailing text after the JSON document.
        payload = '{"show": [{"x": 1}]}\n12:00:02  Done.\n'
        assert _extract_show_rows(payload) == [{"x": 1}]


class TestRunDbtShowParseAnomaly:
    def test_raises_when_dbt_exits_0_but_output_unparseable(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import subprocess

        # dbt returns 0 but emits no JSON document (e.g. an unexpected future format).
        # This must raise, not silently return [] and look like a difference-free run.
        fake = subprocess.CompletedProcess(args=["dbt"], returncode=0, stdout="12:00 [info] weird output", stderr="")
        monkeypatch.setattr(diff_mod.subprocess, "run", lambda *a, **k: fake)
        with pytest.raises(RuntimeError, match="could not parse rows"):
            diff_mod._run_dbt_show("{{ x }}", tmp_path, "dev_local", 10)

    def test_returns_empty_for_explicit_empty_result(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        import subprocess

        fake = subprocess.CompletedProcess(args=["dbt"], returncode=0, stdout='{"show": []}', stderr="")
        monkeypatch.setattr(diff_mod.subprocess, "run", lambda *a, **k: fake)
        assert diff_mod._run_dbt_show("{{ x }}", tmp_path, "dev_local", 10) == []


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


class TestFormatSampleMismatches:
    def test_value_diff_shows_only_differing_fields(self) -> None:
        # a_row/b_row share id=1 but differ only in "name" -- "other" must NOT
        # appear in the output even though it's present in both raw rows.
        rows = [
            {"id": 1, "name": "old", "other": "same", "in_a": True, "in_b": False},
            {"id": 1, "name": "new", "other": "same", "in_a": False, "in_b": True},
        ]
        lines = _format_sample_mismatches(rows, ["id"], "old_side", "new_side")
        assert lines == ["id=1: name: 'old' → 'new'"]

    def test_row_present_only_on_one_side(self) -> None:
        rows = [{"id": 2, "name": "x", "in_a": True, "in_b": False}]
        lines = _format_sample_mismatches(rows, ["id"], "old_side", "new_side")
        assert lines == ["id=2: only in old_side"]

    def test_flags_when_key_group_has_more_than_two_rows(self) -> None:
        # A non-unique primary key can put >2 rows in one group -- must flag
        # the extras rather than silently comparing only the first pair.
        rows = [
            {"id": 3, "name": "a", "in_a": True, "in_b": False},
            {"id": 3, "name": "b", "in_a": True, "in_b": False},
            {"id": 3, "name": "c", "in_a": True, "in_b": False},
        ]
        lines = _format_sample_mismatches(rows, ["id"], "old_side", "new_side")
        assert lines == ["id=3: only in old_side (+1 more rows in this key group, not shown)"]


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


class TestRelationJinja:
    def test_non_raw_uses_ref(self) -> None:
        assert _relation_jinja("dim_user") == "ref('dim_user')"

    def test_raw_defaults_to_target_database_and_schema(self) -> None:
        expr = _relation_jinja("glue__ol_warehouse_production_reporting__enrollment_detail_report", raw=True)
        assert expr == (
            "api.Relation.create(database=target.database, schema=target.schema, "
            "identifier='glue__ol_warehouse_production_reporting__enrollment_detail_report')"
        )

    def test_raw_with_explicit_database_and_schema(self) -> None:
        expr = _relation_jinja(
            "enrollment_detail_report",
            raw=True,
            database="ol_data_lake_production",
            schema="ol_warehouse_production_reporting",
        )
        assert expr == (
            "api.Relation.create(database='ol_data_lake_production', "
            "schema='ol_warehouse_production_reporting', identifier='enrollment_detail_report')"
        )

    def test_compare_relations_sql_with_old_raw(self) -> None:
        sql = _compare_relations_sql(
            "glue__x__y",
            "new_m",
            ["id"],
            [],
            summarize=True,
            old_raw=True,
            old_schema="main",
        )
        assert "ref('glue__x__y')" not in sql
        assert "api.Relation.create(database=target.database, schema='main', identifier='glue__x__y')" in sql
        assert "ref('new_m')" in sql

    def test_compare_column_sql_with_both_raw(self) -> None:
        sql = _compare_column_sql(
            "old_tbl",
            "new_tbl",
            ["id"],
            "some_col",
            old_raw=True,
            old_database="db_a",
            old_schema="schema_a",
            new_raw=True,
            new_database="db_b",
            new_schema="schema_b",
        )
        assert "api.Relation.create(database='db_a', schema='schema_a', identifier='old_tbl')" in sql
        assert "api.Relation.create(database='db_b', schema='schema_b', identifier='new_tbl')" in sql
        assert "ref(" not in sql

    def test_compare_column_sql_narrows_to_primary_key_and_column(self) -> None:
        # Narrows to primary key + compared column instead of `select *`, since
        # compare_column_values' full outer join doesn't need the rest of the row.
        sql = _compare_column_sql("old_m", "new_m", ["id"], "some_col")
        assert "select id, some_col from" in sql
        assert "select *" not in sql

    def test_glue_database_rewrites_identifier_and_forces_raw(self) -> None:
        expr = _relation_jinja("enrollment_detail_report", glue_database="ol_warehouse_production_reporting")
        assert expr == (
            "api.Relation.create(database=target.database, schema=target.schema, "
            "identifier='glue__ol_warehouse_production_reporting__enrollment_detail_report')"
        )

    def test_compare_relations_sql_with_old_glue(self) -> None:
        sql = _compare_relations_sql(
            "enrollment_detail_report",
            "enrollment_detail_report",
            ["id"],
            [],
            summarize=True,
            old_glue="ol_warehouse_production_reporting",
        )
        assert "ref('enrollment_detail_report')" in sql  # --new side unaffected
        assert "identifier='glue__ol_warehouse_production_reporting__enrollment_detail_report'" in sql
        assert "ref('glue__" not in sql


class TestResolveRawColumns:
    def test_returns_lowercased_keys_from_sampled_row(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        monkeypatch.setattr(diff_mod, "_run_dbt_show", lambda *a, **k: [{"Id": 1, "NAME": "x"}])
        cols, error = _resolve_raw_columns("some_view", tmp_path, "dev_local")
        assert cols == {"id", "name"}
        assert error is None

    def test_empty_result_returns_empty_set_no_error(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        monkeypatch.setattr(diff_mod, "_run_dbt_show", lambda *a, **k: [])
        cols, error = _resolve_raw_columns("some_view", tmp_path, "dev_local")
        assert cols == set()
        assert error is None

    def test_dbt_failure_returns_empty_set_and_surfaces_error(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        def boom(*a: Any, **k: Any) -> list[dict[str, Any]]:
            msg = "dbt show failed: relation glue__x__y not found"
            raise RuntimeError(msg)

        monkeypatch.setattr(diff_mod, "_run_dbt_show", boom)
        cols, error = _resolve_raw_columns("missing_view", tmp_path, "dev_local")
        assert cols == set()
        assert error == "dbt show failed: relation glue__x__y not found"


def _make_project(tmp_path: Path, old_sql: str, new_sql: str, *, new_name: str = "m_new") -> Path:
    """Create a minimal dbt project dir with two leaf models and audit_helper "installed"."""
    dbt_dir = tmp_path / "ol_dbt"
    (dbt_dir / "models").mkdir(parents=True)
    (dbt_dir / "dbt_packages" / "audit_helper").mkdir(parents=True)
    (dbt_dir / "dbt_project.yml").write_text("name: test\nprofile: test\n")
    (dbt_dir / "models" / "m_old.sql").write_text(old_sql)
    (dbt_dir / "models" / f"{new_name}.sql").write_text(new_sql)
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

    def test_unresolved_columns_warns_that_schema_gate_skipped(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        # Models whose columns cannot be resolved (no such files, no manifest/YAML)
        # must not silently pass the schema gate — a stderr warning is emitted.
        dbt_dir = _make_project(tmp_path, "select 1 as id", "select 1 as id")
        monkeypatch.setattr(
            diff_mod,
            "_run_dbt_show",
            lambda *a, **k: [{"in_a": True, "in_b": True, "count": 1}],
        )
        diff(old="ghost_old", new="ghost_new", dbt_dir_path=str(dbt_dir))
        err = capsys.readouterr().err
        assert "schema-divergence" in err
        assert "Warning" in err

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
            "old_label",
            "new_label",
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


class TestDiffCommandRaw:
    def test_old_schema_without_old_raw_exits_1(self, tmp_path: Path) -> None:
        dbt_dir = _make_project(tmp_path, "select 1 as id", "select 1 as id")
        with pytest.raises(SystemExit) as exc:
            diff(old="m_old", new="m_new", old_schema="main", dbt_dir_path=str(dbt_dir))
        assert exc.value.code == 1

    def test_new_database_without_new_raw_exits_1(self, tmp_path: Path) -> None:
        dbt_dir = _make_project(tmp_path, "select 1 as id", "select 1 as id")
        with pytest.raises(SystemExit) as exc:
            diff(old="m_old", new="m_new", new_database="db_b", dbt_dir_path=str(dbt_dir))
        assert exc.value.code == 1

    def test_injected_schema_override_rejected(self, tmp_path: Path) -> None:
        dbt_dir = _make_project(tmp_path, "select 1 as id", "select 1 as id")
        with pytest.raises(SystemExit) as exc:
            diff(old="m_old", new="m_new", old_raw=True, old_schema="a; drop table x", dbt_dir_path=str(dbt_dir))
        assert exc.value.code == 1

    def test_old_raw_match_exits_0_and_notes_resolution(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        # "old" is a raw relation (e.g. a Glue view) that isn't a dbt model at all —
        # it must not go through static column resolution or ref().
        dbt_dir = _make_project(tmp_path, "select 1 as id, 'x' as name", "select 1 as id, 'x' as name")
        seen_inline: list[str] = []

        def fake_show(inline_sql: str, *a: Any, **k: Any) -> list[dict[str, Any]]:
            seen_inline.append(inline_sql)
            if "audit_helper" not in inline_sql:
                return [{"id": 1, "name": "x"}]  # raw column sample
            return [{"in_a": True, "in_b": True, "count": 10}]

        monkeypatch.setattr(diff_mod, "_run_dbt_show", fake_show)
        diff(
            old="glue__ol_warehouse_production_reporting__enrollment_detail_report",
            new="m_new",
            old_raw=True,
            old_schema="main",
            dbt_dir_path=str(dbt_dir),
        )
        err = capsys.readouterr().err
        assert "schema-divergence" not in err
        # ref() must never be called for the raw side.
        assert not any("ref('glue__" in sql for sql in seen_inline)
        assert any("api.Relation.create(database=target.database, schema='main'" in sql for sql in seen_inline)

    def test_auto_build_skips_raw_sides(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        dbt_dir = _make_project(tmp_path, "select 1 as id, 'x' as name", "select 1 as id, 'x' as name")

        def fake_show(inline_sql: str, *a: Any, **k: Any) -> list[dict[str, Any]]:
            if "compare_relations" in inline_sql:
                return [{"in_a": True, "in_b": True, "count": 1}]
            return [{"id": 1, "name": "x"}]  # raw column sample, matching m_new's parsed columns

        monkeypatch.setattr(diff_mod, "_run_dbt_show", fake_show)
        build_calls: list[list[str]] = []

        def fake_run(cmd: list[str], **kwargs: Any) -> Any:
            build_calls.append(cmd)
            import subprocess

            return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="", stderr="")

        monkeypatch.setattr(diff_mod.subprocess, "run", fake_run)
        diff(
            old="glue__x__y",
            new="m_new",
            old_raw=True,
            auto_build=True,
            dbt_dir_path=str(dbt_dir),
        )
        assert len(build_calls) == 1
        select_idx = build_calls[0].index("--select") + 1
        assert build_calls[0][select_idx] == "m_new"

    def test_auto_build_skipped_entirely_when_both_raw(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        dbt_dir = _make_project(tmp_path, "select 1 as id, 'x' as name", "select 1 as id, 'x' as name")
        monkeypatch.setattr(
            diff_mod,
            "_run_dbt_show",
            lambda *a, **k: [{"in_a": True, "in_b": True, "count": 1}],
        )

        def boom(*a: Any, **k: Any) -> Any:
            msg = "dbt build must not run when both sides are raw"
            raise AssertionError(msg)

        monkeypatch.setattr(diff_mod.subprocess, "run", boom)
        diff(
            old="glue__x__y",
            new="glue__a__b",
            old_raw=True,
            new_raw=True,
            auto_build=True,
            dbt_dir_path=str(dbt_dir),
        )

    def test_refresh_glue_registers_all_layers_before_comparing(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # --refresh-glue must run before any Glue-registered view is read --
        # both the --old-glue/--new-glue materialization and (for --auto-build)
        # dbt build's own upstream deps can 404 on stale S3 metadata otherwise.
        dbt_dir = _make_project(tmp_path, "select 1 as id, 'x' as name", "select 1 as id, 'x' as name")
        monkeypatch.setattr(diff_mod, "_run_dbt_show", lambda *a, **k: [{"in_a": True, "in_b": True, "count": 1}])
        calls: list[list[str]] = []

        def fake_run(cmd: list[str], **kwargs: Any) -> Any:
            calls.append(cmd)
            import subprocess

            return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="", stderr="")

        monkeypatch.setattr(diff_mod.subprocess, "run", fake_run)
        diff(old="m_old", new="m_new", refresh_glue=True, dbt_dir_path=str(dbt_dir))
        assert calls[0] == ["ol-dbt", "local", "register", "--all-layers"]


class TestDiffCommandGlue:
    def test_old_glue_and_old_raw_together_exits_1(self, tmp_path: Path) -> None:
        dbt_dir = _make_project(tmp_path, "select 1 as id", "select 1 as id")
        with pytest.raises(SystemExit) as exc:
            diff(
                old="m_old",
                new="m_new",
                old_raw=True,
                old_glue="ol_warehouse_production_reporting",
                dbt_dir_path=str(dbt_dir),
            )
        assert exc.value.code == 1

    def test_invalid_glue_database_rejected(self, tmp_path: Path) -> None:
        dbt_dir = _make_project(tmp_path, "select 1 as id", "select 1 as id")
        with pytest.raises(SystemExit) as exc:
            diff(old="m_old", new="m_new", old_glue="a; drop table x", dbt_dir_path=str(dbt_dir))
        assert exc.value.code == 1

    def test_old_glue_keeps_bare_name_and_rewrites_identifier(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        # --old and --new both stay "enrollment_detail_report" as the underlying
        # `old`/`new` identifiers — only the SQL identifier and the printed label
        # should carry the glue__/(glue:...) annotation.
        dbt_dir = _make_project(
            tmp_path,
            "select 1 as id, 'x' as name",
            "select 1 as id, 'x' as name",
            new_name="enrollment_detail_report",
        )
        seen_inline: list[str] = []

        def fake_show(inline_sql: str, *a: Any, **k: Any) -> list[dict[str, Any]]:
            seen_inline.append(inline_sql)
            if "audit_helper" not in inline_sql:
                return [{"id": 1, "name": "x"}]  # raw column sample
            return [{"in_a": True, "in_b": True, "count": 10}]

        monkeypatch.setattr(diff_mod, "_run_dbt_show", fake_show)
        diff(
            old="enrollment_detail_report",
            new="enrollment_detail_report",
            old_glue="ol_warehouse_production_reporting",
            dbt_dir_path=str(dbt_dir),
        )
        out = " ".join(capsys.readouterr().out.split())  # rich line-wraps the long header
        assert "enrollment_detail_report (glue:ol_warehouse_production_reporting) → enrollment_detail_report" in out
        assert not any("ref('glue__" in sql for sql in seen_inline)
        assert any(
            "identifier='glue__ol_warehouse_production_reporting__enrollment_detail_report'" in sql
            for sql in seen_inline
        )
        assert any("ref('enrollment_detail_report')" in sql for sql in seen_inline)

    def test_old_glue_materializes_scratch_table_before_comparing(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # DuckDB's Iceberg extension can't run audit_helper's UNION ALL/EXCEPT SQL
        # against a live glue__ view (IcebergScan serialization not implemented),
        # so the glue side must be copied into a real scratch table first, and
        # every downstream comparison query must reference that scratch table --
        # never the live glue__ identifier -- or the same crash resurfaces.
        dbt_dir = _make_project(
            tmp_path, "select 1 as id, 'x' as name", "select 1 as id, 'x' as name", new_name="enrollment_detail_report"
        )
        seen_inline: list[str] = []

        def fake_show(inline_sql: str, *a: Any, **k: Any) -> list[dict[str, Any]]:
            seen_inline.append(inline_sql)
            if "audit_helper" not in inline_sql:
                return [{"id": 1, "name": "x"}]
            return [{"in_a": True, "in_b": True, "count": 10}]

        monkeypatch.setattr(diff_mod, "_run_dbt_show", fake_show)
        diff(
            old="enrollment_detail_report",
            new="enrollment_detail_report",
            old_glue="ol_warehouse_production_reporting",
            dbt_dir_path=str(dbt_dir),
        )
        ctas_calls = [sql for sql in seen_inline if "create or replace table" in sql]
        assert len(ctas_calls) == 1
        assert (
            "identifier='_ol_dbt_diff_scratch__ol_warehouse_production_reporting__enrollment_detail_report'"
            in (ctas_calls[0])
        )
        assert "identifier='glue__ol_warehouse_production_reporting__enrollment_detail_report'" in ctas_calls[0]

        compare_calls = [sql for sql in seen_inline if "audit_helper" in sql]
        assert compare_calls  # summarize + (no mismatches -> no sample) at least one
        assert not any("glue__" in sql for sql in compare_calls)
        assert all(
            "identifier='_ol_dbt_diff_scratch__ol_warehouse_production_reporting__enrollment_detail_report'" in sql
            for sql in compare_calls
        )

    def test_old_glue_sampling_failure_surfaces_real_dbt_error(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        # If the Glue view doesn't exist (wrong glue database, not registered yet,
        # ...), materializing it (which now runs before column resolution, to work
        # around DuckDB's IcebergScan/UNION-ALL limitation) fails first — the real
        # dbt error must be surfaced, not the generic "unparsed / not in manifest"
        # note written for the static-parse path.
        dbt_dir = _make_project(tmp_path, "select 1 as id, 'x' as name", "select 1 as id, 'x' as name")

        def fake_show(inline_sql: str, *a: Any, **k: Any) -> list[dict[str, Any]]:
            msg = "dbt show failed: Relation glue__bad_db__m_old does not exist"
            raise RuntimeError(msg)

        monkeypatch.setattr(diff_mod, "_run_dbt_show", fake_show)
        with pytest.raises(SystemExit) as exc:
            diff(old="m_old", new="m_new", old_glue="bad_db", dbt_dir_path=str(dbt_dir))
        assert exc.value.code == 1
        err = " ".join(capsys.readouterr().err.split())  # rich line-wraps long notes
        assert "failed to materialize --old-glue relation" in err
        assert "Relation glue__bad_db__m_old does not exist" in err
        assert "not in manifest" not in err


class TestDiffCommandLabels:
    def test_plain_diff_labels_are_unannotated(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        # A plain ref()-vs-ref() diff (no --*-raw/--*-glue) is already unambiguous
        # — the header must not grow a "(raw)"/"(glue:...)" annotation.
        dbt_dir = _make_project(tmp_path, "select 1 as id", "select 1 as id")
        monkeypatch.setattr(diff_mod, "_run_dbt_show", lambda *a, **k: [{"in_a": True, "in_b": True, "count": 1}])
        diff(old="m_old", new="m_new", dbt_dir_path=str(dbt_dir))
        out = capsys.readouterr().out
        assert "m_old → m_new" in out
        assert "(raw)" not in out
        assert "(glue:" not in out

    def test_old_raw_label_annotated_with_raw(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        dbt_dir = _make_project(tmp_path, "select 1 as id, 'x' as name", "select 1 as id, 'x' as name")

        def fake_show(inline_sql: str, *a: Any, **k: Any) -> list[dict[str, Any]]:
            if "audit_helper" not in inline_sql:
                return [{"id": 1, "name": "x"}]
            return [{"in_a": True, "in_b": True, "count": 1}]

        monkeypatch.setattr(diff_mod, "_run_dbt_show", fake_show)
        diff(old="glue__x__y", new="m_new", old_raw=True, dbt_dir_path=str(dbt_dir))
        out = capsys.readouterr().out
        assert "glue__x__y (raw) → m_new" in out

    def test_old_glue_label_annotated_with_glue_database(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        dbt_dir = _make_project(tmp_path, "select 1 as id, 'x' as name", "select 1 as id, 'x' as name")

        def fake_show(inline_sql: str, *a: Any, **k: Any) -> list[dict[str, Any]]:
            if "audit_helper" not in inline_sql:
                return [{"id": 1, "name": "x"}]
            return [{"in_a": True, "in_b": True, "count": 1}]

        monkeypatch.setattr(diff_mod, "_run_dbt_show", fake_show)
        diff(old="m_old", new="m_new", old_glue="ol_warehouse_production_reporting", dbt_dir_path=str(dbt_dir))
        out = capsys.readouterr().out
        assert "m_old (glue:ol_warehouse_production_reporting) → m_new" in out
