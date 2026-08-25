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
    _split_columns,
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


class TestSplitColumns:
    """Comma-separated and repeated column arguments must be equivalent."""

    def test_repeated_flags(self) -> None:
        assert _split_columns(("a", "b", "c")) == ["a", "b", "c"]

    def test_comma_separated_single_token(self) -> None:
        assert _split_columns(("a,b,c",)) == ["a", "b", "c"]

    def test_comma_and_repeat_are_equivalent(self) -> None:
        assert _split_columns(("a,b,c",)) == _split_columns(("a", "b", "c"))

    def test_mixed_forms(self) -> None:
        assert _split_columns(("a,b", "c")) == ["a", "b", "c"]

    def test_surrounding_whitespace_is_stripped(self) -> None:
        # `-k "a, b"` is a single shell token; the space must not become part of
        # the identifier, or _validate_identifiers rejects a legitimate key.
        assert _split_columns(("a, b",)) == ["a", "b"]

    def test_order_is_preserved(self) -> None:
        assert _split_columns(("z,a,m",)) == ["z", "a", "m"]

    def test_duplicates_dropped_so_a_key_is_not_emitted_twice(self) -> None:
        assert _split_columns(("a,b,a",)) == ["a", "b"]
        assert _split_columns(("a", "a")) == ["a"]

    def test_empty_segments_discarded(self) -> None:
        # A trailing comma or an empty value must not reach _validate_identifiers
        # as an empty-string identifier.
        assert _split_columns(("a,",)) == ["a"]
        assert _split_columns(("a,,b",)) == ["a", "b"]
        assert _split_columns(("",)) == []

    def test_no_arguments(self) -> None:
        assert _split_columns(()) == []


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

    def test_compare_relations_composite_pk_is_comma_joined_not_a_jinja_list(self) -> None:
        # audit_helper interpolates primary_key straight into the summarize=false
        # `order by`, so a Jinja list reaches the database as the literal text
        # `['k1', 'k2']` and fails to parse. A comma-joined column list is the
        # only form that renders to valid SQL there.
        sql = _compare_relations_sql("a", "b", ["k1", "k2"], [], summarize=False)
        assert "primary_key='k1, k2'" in sql
        assert "['k1', 'k2']" not in sql
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

    def test_compare_column_sql_composite_pk_uses_a_surrogate_join_key(self) -> None:
        # compare_column_values only supports a scalar primary key -- it emits
        # `a_query.{{ primary_key }} = b_query.{{ primary_key }}`. A composite key
        # is collapsed into one hashed column inside a_query/b_query instead, so no
        # Jinja list (or comma-joined list) ever lands in the emitted SQL.
        sql = _compare_column_sql("old_m", "new_m", ["k1", "k2"], "some_col")
        assert "primary_key='ol_dbt_diff_surrogate_key'" in sql
        # Must NOT be dbt_utils.generate_surrogate_key -- it joins components with
        # a literal '-' without encoding boundaries, so ('a-b','c') and ('a','b-c')
        # collide and would pair two distinct keys as one row.
        assert "generate_surrogate_key" not in sql
        assert "{{ diff_composite_key(['k1', 'k2']) }} as ol_dbt_diff_surrogate_key" in sql
        assert "primary_key=['k1', 'k2']" not in sql
        assert "primary_key='k1, k2'" not in sql

    def test_compare_column_sql_composite_pk_selects_key_on_both_sides(self) -> None:
        # The surrogate must exist in a_query AND b_query for the join to resolve.
        sql = _compare_column_sql("old_m", "new_m", ["k1", "k2"], "some_col")
        select_clause = "select {{ diff_composite_key(['k1', 'k2']) }} as ol_dbt_diff_surrogate_key, some_col from"
        assert sql.count(select_clause) == 2

    def test_compare_column_sql_single_pk_keeps_the_real_column(self) -> None:
        # No surrogate for a single key -- the real column joins directly.
        sql = _compare_column_sql("old_m", "new_m", ["id"], "some_col")
        assert "primary_key='id'" in sql
        assert "surrogate_key" not in sql


class TestCompareSingleColumn:
    """Bucket accounting for audit_helper.compare_column_values."""

    # Glyphs must match real audit_helper 0.12 output — the parser keys off them.
    _ROWS = [
        {"match_status": "✅: perfect match", "count_records": 2},
        {"match_status": "🤷: missing from a", "count_records": 1},
        {"match_status": "🤷: missing from b", "count_records": 3},
        {"match_status": "❌: ‍values do not match", "count_records": 4},
    ]

    def _run(self, rows: list[dict[str, object]], tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setattr(diff_mod, "_run_dbt_show", lambda *a, **k: rows)
        return diff_mod._compare_single_column("old_m", "new_m", ["id"], "amt", tmp_path, "dev_local")

    def test_only_value_differences_count_toward_mismatch_rate(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        result = self._run(self._ROWS, tmp_path, monkeypatch)
        assert result is not None
        assert result.mismatched_rows == 4
        assert result.mismatch_rate == 0.4  # 4 of 10 joined rows
        assert result.missing_in_old == 1
        assert result.missing_in_new == 3

    def test_presence_only_differences_yield_no_value_mismatch(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        rows = [r for r in self._ROWS if not str(r["match_status"]).startswith("❌")]
        result = self._run(rows, tmp_path, monkeypatch)
        assert result is not None
        assert result.mismatched_rows == 0
        assert result.mismatch_rate == 0.0
        assert (result.missing_in_old, result.missing_in_new) == (1, 3)

    def test_returns_none_when_no_rows(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        assert self._run([], tmp_path, monkeypatch) is None


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

    def test_primary_key_not_compared_per_column(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        dbt_dir = _make_project(
            tmp_path,
            "select 1 as id, 'x' as name",
            "select 1 as id, 'x' as name",
        )
        compared: list[str] = []

        def fake_show(inline_sql: str, *a: Any, **k: Any) -> list[dict[str, Any]]:
            if "compare_column_values" in inline_sql:
                compared.append(inline_sql)
                return [{"match_status": "✅: perfect match", "count_records": 10}]
            return [{"in_a": True, "in_b": True, "count": 10}]

        monkeypatch.setattr(diff_mod, "_run_dbt_show", fake_show)
        diff(old="m_old", new="m_new", primary_key=("id",), dbt_dir_path=str(dbt_dir))
        assert len(compared) == 1
        assert "column_to_compare='name'" in compared[0]

    def test_comma_separated_primary_key_reaches_comparison_as_composite(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # End-to-end through the CLI entrypoint: `-k a,b` must behave exactly like
        # `-k a -k b`, i.e. produce the composite surrogate-key join rather than a
        # single key named "a,b" (which _validate_identifiers would reject).
        dbt_dir = _make_project(
            tmp_path,
            "select 1 as k1, 2 as k2, 'x' as name",
            "select 1 as k1, 2 as k2, 'x' as name",
        )
        seen: list[str] = []

        def fake_show(inline_sql: str, *a: Any, **k: Any) -> list[dict[str, Any]]:
            if "compare_column_values" in inline_sql:
                seen.append(inline_sql)
                return [{"match_status": "✅: perfect match", "count_records": 10}]
            return [{"in_a": True, "in_b": True, "count": 10}]

        monkeypatch.setattr(diff_mod, "_run_dbt_show", fake_show)
        diff(old="m_old", new="m_new", primary_key=("k1,k2",), dbt_dir_path=str(dbt_dir))

        # Only `name` is compared -- both key columns were recognised as keys.
        assert len(seen) == 1
        assert "column_to_compare='name'" in seen[0]
        assert "diff_composite_key(['k1', 'k2'])" in seen[0]

    def test_comma_and_repeated_primary_key_produce_identical_sql(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        dbt_dir = _make_project(
            tmp_path,
            "select 1 as k1, 2 as k2, 'x' as name",
            "select 1 as k1, 2 as k2, 'x' as name",
        )

        def run(pk: tuple[str, ...]) -> list[str]:
            captured: list[str] = []

            def fake_show(inline_sql: str, *a: Any, **k: Any) -> list[dict[str, Any]]:
                captured.append(inline_sql)
                if "compare_column_values" in inline_sql:
                    return [{"match_status": "✅: perfect match", "count_records": 10}]
                return [{"in_a": True, "in_b": True, "count": 10}]

            monkeypatch.setattr(diff_mod, "_run_dbt_show", fake_show)
            diff(old="m_old", new="m_new", primary_key=pk, dbt_dir_path=str(dbt_dir))
            return captured

        assert run(("k1,k2",)) == run(("k1", "k2"))

    def test_comma_separated_exclude_columns_are_all_excluded(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        dbt_dir = _make_project(
            tmp_path,
            "select 1 as id, 'x' as a, 'y' as b",
            "select 1 as id, 'x' as a, 'y' as b",
        )
        seen: list[str] = []

        def fake_show(inline_sql: str, *a: Any, **k: Any) -> list[dict[str, Any]]:
            seen.append(inline_sql)
            if "compare_column_values" in inline_sql:
                return [{"match_status": "✅: perfect match", "count_records": 10}]
            return [{"in_a": True, "in_b": True, "count": 10}]

        monkeypatch.setattr(diff_mod, "_run_dbt_show", fake_show)
        diff(
            old="m_old",
            new="m_new",
            primary_key=("id",),
            exclude_columns=("a,b",),
            dbt_dir_path=str(dbt_dir),
        )
        relations = [q for q in seen if "compare_relations" in q]
        assert "exclude_columns=['a', 'b']" in relations[0]
        # Both excluded, so nothing is left to compare per-column.
        assert not [q for q in seen if "compare_column_values" in q]

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


class TestDiffCommandLabels:
    def test_plain_diff_labels_are_unannotated(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        # A plain ref()-vs-ref() diff (no --*-raw) is already unambiguous — the
        # header must not grow a "(raw)" annotation.
        dbt_dir = _make_project(tmp_path, "select 1 as id", "select 1 as id")
        monkeypatch.setattr(diff_mod, "_run_dbt_show", lambda *a, **k: [{"in_a": True, "in_b": True, "count": 1}])
        diff(old="m_old", new="m_new", dbt_dir_path=str(dbt_dir))
        out = capsys.readouterr().out
        assert "m_old → m_new" in out
        assert "(raw)" not in out

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
