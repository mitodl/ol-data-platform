"""Tests for the StarRocks dbt retry classifier and MV-relation derivation.

The failure texts below are verbatim from the production Dagster run that
motivated this module (run acc2b10c, 2026-07-22), not invented -- the point of
the retry pattern is that it matches what StarRocks actually emits.
"""

import logging
import re

import pytest
from lakehouse.lib.starrocks_dbt import (
    MAX_BUILD_ATTEMPTS,
    MAX_MV_REFRESH_ATTEMPTS,
    MV_REFRESH_RETRY_DELAY_SECONDS,
    RETRIABLE_ERROR_PATTERN,
    RETRY_BASE_DELAY,
    ChangeTrackedView,
    MaterializedViewRefreshError,
    built_relations,
    change_tracked_views,
    definition_hashes,
    documented_columns,
    drifted_relations,
    live_column_query,
    live_columns,
    looks_retriable,
    materialized_view_relations,
    record_built_definitions,
    record_definitions_sql,
    recorded_definitions,
    redefined_relations,
    refresh_materialized_views,
    retry_delay,
    seed_change_log_sql,
    stamp_change_log,
    stamp_change_log_sql,
)
from lakehouse.resources.starrocks import _RETRIABLE_ERRORS

# Verbatim from the failed run: an FE rolling restart began 39s into the build,
# so the follower dbt was connected to could no longer forward DDL to the leader.
FE_ROLLOUT_FAILURE = """The dbt CLI process with command

`dbt build --target starrocks_production --select tag:starrocks`

failed with exit code `2`.

Errors parsed from dbt logs:

2 of 7 ERROR creating sql materialized_view model \
b2b_analytics.mv_b2b_contract_utilization  [ERROR in 46.99s]

  Database Error in model mv_b2b_contract_utilization
  1064 (HY000): java.net.SocketTimeoutException: Connect timed out

Encountered an error:
Database Error
  1064 (HY000): forward failed: unknown result
"""

# Verbatim from the 2026-09-03 17:31 UTC run, after the Trino project rebuilt the
# `dimensional` Iceberg tables.  dbt's first four concurrent models all failed on
# dim_organization; models 5-8 built OK in that same invocation and a re-run 60s
# later built all eight -- which is what makes this signature worth retrying.
BASE_TABLE_DROPPED_FAILURE = """The dbt CLI process with command

`dbt build --target starrocks_production --select tag:starrocks`

failed with exit code `1`.

Errors parsed from dbt logs:

3 of 8 ERROR creating sql materialized_view model \
b2b_analytics.mv_b2b_contract_monthly_engagement_trend  [ERROR in 0.61s]

  Database Error in model mv_b2b_contract_monthly_engagement_trend
  1064 (HY000): Getting analyzing error. Detail message: base-table dropped: \
dim_organization.
"""

CLEAN_BUILD_OUTPUT = """
1 of 7 OK created sql materialized_view model b2b_analytics.mv_b2b_program_funnel \
[SUCCESS in 12.06s]
Done. PASS=7 WARN=0 ERROR=0 SKIP=0 TOTAL=7
"""


class TestLooksRetriable:
    def test_fe_rollout_failure_is_retriable(self):
        """The whole point: both signatures arrive wrapped in a generic 1064,
        so a numbers-only pattern would let this fail the build outright.
        """
        assert looks_retriable(Exception(FE_ROLLOUT_FAILURE))

    @pytest.mark.parametrize(
        "message",
        [
            "1064 (HY000): forward failed: unknown result",
            "1064 (HY000): java.net.SocketTimeoutException: Connect timed out",
        ],
    )
    def test_fe_forwarding_signatures(self, message):
        assert looks_retriable(Exception(message))

    @pytest.mark.parametrize(
        ("code", "meaning"),
        [
            (1044, "ER_DBACCESS_DENIED_ERROR -- Vault user not yet propagated"),
            (1045, "ER_ACCESS_DENIED_ERROR -- Vault user not yet propagated"),
            (2003, "CR_CONN_HOST_ERROR -- fresh connect to an FE that is gone"),
            (2006, "CR_SERVER_GONE_ERROR"),
            (2013, "CR_SERVER_LOST"),
        ],
    )
    def test_wire_protocol_codes(self, code, meaning):
        assert looks_retriable(Exception(f"{code} (HY000): {meaning}"))

    def test_stale_external_base_table_is_retriable(self):
        """A rebuilt Iceberg base table leaves StarRocks' cached handle stale for
        a window.  The same invocation went on to build the remaining models and
        a re-run went green, so this is worth another attempt rather than a red
        asset.
        """
        assert looks_retriable(Exception(BASE_TABLE_DROPPED_FAILURE))

    def test_base_table_dropped_signature(self):
        assert looks_retriable(
            Exception(
                "1064 (HY000): Getting analyzing error. Detail message: "
                "base-table dropped: dim_organization."
            )
        )

    def test_successful_build_output_is_not_retriable(self):
        assert not looks_retriable(Exception(CLEAN_BUILD_OUTPUT))

    def test_unrelated_1064_is_not_retriable(self):
        """A genuine SQL error also surfaces as 1064. Retrying a bad query three
        more times just burns 210s before failing the same way.
        """
        assert not looks_retriable(
            Exception(
                "1064 (HY000): Getting analyzing error. Detail message: "
                "Unknown table 'mv_b2b_typo'."
            )
        )

    def test_embedded_digits_do_not_trip_the_word_boundary(self):
        assert not looks_retriable(Exception("Rows affected: 20130, 12006, 110445"))


class TestRetriableCodesAgreeWithTheResource:
    def test_same_wire_protocol_codes_on_both_paths(self):
        """The drift preflight runs through StarRocksResource, not through the
        dbt build's retry loop, so a code the classifier here treats as
        transient but the resource re-raises would abort the whole asset before
        the build ever starts. 2003 CR_CONN_HOST_ERROR was exactly that gap.
        """
        classifier_codes = {
            int(code) for code in re.findall(r"\d{4}", RETRIABLE_ERROR_PATTERN.pattern)
        }
        assert classifier_codes == set(_RETRIABLE_ERRORS)

    def test_a_failed_connect_is_retriable(self):
        assert 2003 in _RETRIABLE_ERRORS


class TestRetryDelay:
    def test_schedule_doubles(self):
        assert [retry_delay(a) for a in range(1, MAX_BUILD_ATTEMPTS)] == [30, 60, 120]

    def test_total_sleep_outlasts_an_fe_rolling_restart(self):
        """The 2026-07-22 rollout ran 20:24:37 -> 20:27:33, i.e. 176s. Every
        attempt has to not land inside the next one, or the retry is decorative:
        the previous 3-attempt/1s-base schedule slept 3s in total and failed.
        """
        observed_rollout_seconds = 176
        total = sum(retry_delay(a) for a in range(1, MAX_BUILD_ATTEMPTS))
        assert total > observed_rollout_seconds

    def test_initial_attempt_never_waits(self):
        """Attempt 0 is the initial build, not a retry. `2 ** -1` would make
        this 15.0 -- a float, and a nonsensical wait before the first try.
        """
        assert retry_delay(0) == 0
        assert isinstance(retry_delay(0), int)

    def test_every_delay_is_an_int(self):
        """time.sleep tolerates a float, but the annotation says int and a
        fractional delay would mean the schedule isn't what the comment claims.
        """
        assert all(isinstance(retry_delay(a), int) for a in range(MAX_BUILD_ATTEMPTS))

    def test_first_retry_is_not_instant(self):
        """A follower FE that just lost the leader needs the election to settle;
        retrying a second later just burns an attempt.
        """
        assert retry_delay(1) == RETRY_BASE_DELAY
        assert RETRY_BASE_DELAY >= 30


def _model_node(
    name,
    *,
    schema="b2b_analytics",
    materialized,
    tags,
    columns=None,
    meta=None,
    raw_code="select 1",
    macros=(),
    relations=(),
    unrendered_config=None,
):
    return {
        "resource_type": "model",
        "package_name": "open_learning",
        "schema": schema,
        "alias": name,
        "tags": tags,
        "config": {"materialized": materialized, "tags": tags, "meta": meta or {}},
        # dbt keys `columns` by name and nests the docs under it; only the keys
        # matter here.
        "columns": {name: {"name": name} for name in columns or []},
        "relation_name": f"`{schema}`.`{name}`",
        "raw_code": raw_code,
        "depends_on": {"macros": list(macros), "nodes": list(relations)},
        "unrendered_config": unrendered_config or {},
    }


def _macro(sql, *, package="open_learning", macros=()):
    return {
        "package_name": package,
        "macro_sql": sql,
        "depends_on": {"macros": list(macros)},
    }


def _manifest(nodes, macros=None, sources=None):
    return {
        "nodes": {f"model.open_learning.{n['alias']}": n for n in nodes},
        "macros": macros or {},
        "sources": sources or {},
    }


class TestMaterializedViewRelations:
    def test_qualifies_with_the_schema_dbt_resolved(self):
        """Not the connection's default database. This is the bug that produced
        `Can not find materialized view` -- dbt built into one schema while the
        refresh asset issued an unqualified statement against another.
        """
        manifest = _manifest(
            [
                _model_node(
                    "mv_b2b_contract_utilization",
                    materialized="materialized_view",
                    tags=["starrocks"],
                )
            ]
        )
        assert materialized_view_relations(manifest) == [
            "b2b_analytics.mv_b2b_contract_utilization"
        ]

    def test_follows_a_schema_change_without_a_python_edit(self):
        manifest = _manifest(
            [
                _model_node(
                    "mv_b2b_contract_utilization",
                    schema="b2b_analytics_b2b_analytics",
                    materialized="materialized_view",
                    tags=["starrocks"],
                )
            ]
        )
        assert materialized_view_relations(manifest) == [
            "b2b_analytics_b2b_analytics.mv_b2b_contract_utilization"
        ]

    def test_excludes_non_materialized_view_models(self):
        """A starrocks-tagged model that materializes as a table must be left
        out: REFRESH MATERIALIZED VIEW against a plain table is an error.
        """
        manifest = _manifest(
            [
                _model_node(
                    "mv_b2b_program_funnel",
                    materialized="materialized_view",
                    tags=["starrocks"],
                ),
                _model_node(
                    "b2b_seed_table",
                    materialized="table",
                    tags=["starrocks", "b2b_analytics"],
                ),
            ]
        )
        assert materialized_view_relations(manifest) == [
            "b2b_analytics.mv_b2b_program_funnel"
        ]

    def test_excludes_models_not_tagged_starrocks(self):
        manifest = _manifest(
            [
                _model_node(
                    "mv_b2b_program_funnel",
                    materialized="materialized_view",
                    tags=["starrocks"],
                ),
                _model_node(
                    "some_trino_mv",
                    schema="ol_warehouse_production_mart",
                    materialized="materialized_view",
                    tags=["mart"],
                ),
            ]
        )
        assert materialized_view_relations(manifest) == [
            "b2b_analytics.mv_b2b_program_funnel"
        ]

    def test_ignores_non_model_nodes(self):
        manifest = _manifest(
            [
                _model_node(
                    "mv_b2b_program_funnel",
                    materialized="materialized_view",
                    tags=["starrocks"],
                )
            ]
        )
        manifest["nodes"]["test.open_learning.not_null_x"] = {
            "resource_type": "test",
            "schema": "b2b_analytics",
            "alias": "not_null_x",
            "tags": ["starrocks"],
            "config": {"materialized": "test", "tags": ["starrocks"]},
        }
        assert materialized_view_relations(manifest) == [
            "b2b_analytics.mv_b2b_program_funnel"
        ]

    def test_result_is_sorted(self):
        manifest = _manifest(
            [
                _model_node(name, materialized="materialized_view", tags=["starrocks"])
                for name in ("mv_b2b_program_funnel", "mv_b2b_contract_utilization")
            ]
        )
        assert materialized_view_relations(manifest) == [
            "b2b_analytics.mv_b2b_contract_utilization",
            "b2b_analytics.mv_b2b_program_funnel",
        ]

    def test_raises_rather_than_silently_refreshing_nothing(self):
        """An empty list would let the asset report success while every MV goes
        stale -- the exact failure the hand-maintained list could produce.
        """
        manifest = _manifest(
            [_model_node("some_table", materialized="table", tags=["starrocks"])]
        )
        with pytest.raises(ValueError, match="No materialized_view models tagged"):
            materialized_view_relations(manifest)


def _mv_node(name, columns, *, schema="b2b_analytics"):
    return _model_node(
        name,
        schema=schema,
        materialized="materialized_view",
        tags=["starrocks"],
        columns=columns,
    )


def _rows(relation, columns):
    schema, table = relation.split(".")
    return [
        {"table_schema": schema, "table_name": table, "column_name": column}
        for column in columns
    ]


class TestDocumentedColumns:
    def test_keys_by_relation_and_lowercases(self):
        manifest = _manifest(
            [_mv_node("mv_b2b_program_funnel", ["Org_Key", "STARTED"])]
        )
        assert documented_columns(manifest) == {
            "b2b_analytics.mv_b2b_program_funnel": {"org_key", "started"}
        }

    def test_omits_models_with_no_documented_columns(self):
        """An empty set differs from every live MV, so treating "undocumented"
        as "expects nothing" would drop and recreate the view on every run.
        `+meta: required_docs: true` should keep this unreachable.
        """
        manifest = _manifest([_mv_node("mv_b2b_program_funnel", [])])
        assert documented_columns(manifest) == {}

    def test_ignores_tables_and_other_engines(self):
        manifest = _manifest(
            [
                _mv_node("mv_b2b_program_funnel", ["org_key"]),
                _model_node(
                    "b2b_seed_table",
                    materialized="table",
                    tags=["starrocks"],
                    columns=["org_key"],
                ),
                _model_node(
                    "some_trino_mv",
                    schema="ol_warehouse_production_mart",
                    materialized="materialized_view",
                    tags=["mart"],
                    columns=["org_key"],
                ),
            ]
        )
        assert set(documented_columns(manifest)) == {
            "b2b_analytics.mv_b2b_program_funnel"
        }


class TestLiveColumnQuery:
    def test_one_placeholder_per_distinct_schema(self):
        query, params = live_column_query(
            {
                "b2b_analytics.mv_a": set(),
                "b2b_analytics.mv_b": set(),
                "b2b_analytics_qa.mv_a": set(),
            }
        )
        assert params == ("b2b_analytics", "b2b_analytics_qa")
        assert query.count("%s") == len(params)

    def test_schema_names_are_bound_not_interpolated(self):
        query, params = live_column_query({"b2b_analytics.mv_a": set()})
        assert "b2b_analytics" not in query
        assert params == ("b2b_analytics",)


class TestLiveColumns:
    def test_folds_rows_into_relations(self):
        rows = _rows("b2b_analytics.mv_a", ["org_key", "started"]) + _rows(
            "b2b_analytics.mv_b", ["org_key"]
        )
        assert live_columns(rows) == {
            "b2b_analytics.mv_a": {"org_key", "started"},
            "b2b_analytics.mv_b": {"org_key"},
        }

    def test_lowercases_column_names(self):
        assert live_columns(_rows("b2b_analytics.mv_a", ["Org_Key"])) == {
            "b2b_analytics.mv_a": {"org_key"}
        }


class TestDriftedRelations:
    def test_added_column_is_drift(self):
        """PR #2520: the dbt model grew five cohort columns and the deployed MV
        kept the old SELECT, which a plain `dbt build` reports as success.
        """
        documented = {"b2b_analytics.mv_a": {"org_key", "video_watchers"}}
        live = {"b2b_analytics.mv_a": {"org_key"}}
        assert drifted_relations(documented, live) == ["b2b_analytics.mv_a"]

    def test_renamed_column_is_drift(self):
        documented = {"b2b_analytics.mv_a": {"org_key", "video_watchers"}}
        live = {"b2b_analytics.mv_a": {"org_key", "video_viewers"}}
        assert drifted_relations(documented, live) == ["b2b_analytics.mv_a"]

    def test_removed_column_is_drift(self):
        """Only reachable as equality, not as "documented columns are missing".
        Safe to assert because ol-dbt validate errors on a SQL column the YAML
        omits (#2555), so a live column absent from `documented` really is one
        the model no longer emits -- not one nobody got around to documenting.
        """
        documented = {"b2b_analytics.mv_a": {"org_key"}}
        live = {"b2b_analytics.mv_a": {"org_key", "dropped_col"}}
        assert drifted_relations(documented, live) == ["b2b_analytics.mv_a"]

    def test_matching_columns_are_not_drift(self):
        """The common case -- it must not force a full refresh, since that drops
        and recreates views ol-analytics-api is serving from.
        """
        columns = {"org_key", "video_watchers"}
        assert (
            drifted_relations(
                {"b2b_analytics.mv_a": columns}, {"b2b_analytics.mv_a": columns}
            )
            == []
        )

    def test_a_view_that_does_not_exist_yet_is_not_drift(self):
        """This build creates it with the current SELECT; nothing to refresh."""
        assert drifted_relations({"b2b_analytics.mv_new": {"org_key"}}, {}) == []

    def test_ignores_live_relations_dbt_does_not_own(self):
        """The query filters by schema, so tables created outside dbt come back
        in the same result set.
        """
        documented = {"b2b_analytics.mv_a": {"org_key"}}
        live = {
            "b2b_analytics.mv_a": {"org_key"},
            "b2b_analytics.some_manual_table": {"whatever"},
        }
        assert drifted_relations(documented, live) == []

    def test_result_is_sorted(self):
        documented = {
            "b2b_analytics.mv_b": {"org_key", "new_col"},
            "b2b_analytics.mv_a": {"org_key", "new_col"},
        }
        live = {"b2b_analytics.mv_a": {"org_key"}, "b2b_analytics.mv_b": {"org_key"}}
        assert drifted_relations(documented, live) == [
            "b2b_analytics.mv_a",
            "b2b_analytics.mv_b",
        ]


def _mv(name="mv_a", **kwargs):
    return _model_node(
        name, materialized="materialized_view", tags=["starrocks"], **kwargs
    )


def _hash(node, macros=None, name="b2b_analytics.mv_a", sources=None):
    return definition_hashes(_manifest([node], macros, sources))[name]


# Stand-ins for sha256 hex digests, which is the only shape a recorded hash may
# have.
OLD = "0" * 64
NEW = "1" * 64


class TestDefinitionHashes:
    def test_same_definition_same_hash(self):
        assert _hash(_mv(raw_code="select 1")) == _hash(_mv(raw_code="select 1"))

    def test_edited_select_with_the_same_columns_changes_the_hash(self):
        """PR #2913: a filter moved and no column did, so the column comparison
        saw nothing and the deployed MV would have kept the old SELECT.
        """
        before = _mv(raw_code="select user_fk from t where is_active")
        after = _mv(raw_code="select user_fk from t where is_active or is_certified")
        assert _hash(before) != _hash(after)

    def test_edited_macro_changes_only_the_models_that_call_it(self):
        """The same PR's edit was in a macro body. The model files that call it
        did not change at all.
        """
        nodes = [
            _mv("mv_caller", macros=["macro.open_learning.rows"]),
            _mv("mv_other"),
        ]
        before = definition_hashes(
            _manifest(nodes, {"macro.open_learning.rows": _macro("select 1")})
        )
        after = definition_hashes(
            _manifest(nodes, {"macro.open_learning.rows": _macro("select 2")})
        )
        assert before["b2b_analytics.mv_caller"] != after["b2b_analytics.mv_caller"]
        assert before["b2b_analytics.mv_other"] == after["b2b_analytics.mv_other"]

    def test_follows_a_macro_called_by_a_macro(self):
        def macros(inner_sql):
            return {
                "macro.open_learning.outer": _macro(
                    "{{ inner() }}", macros=["macro.open_learning.inner"]
                ),
                "macro.open_learning.inner": _macro(inner_sql),
            }

        node = _mv(macros=["macro.open_learning.outer"])
        assert _hash(node, macros("select 1")) != _hash(node, macros("select 2"))

    def test_survives_macros_that_call_each_other(self):
        macros = {
            "macro.open_learning.a": _macro("a", macros=["macro.open_learning.b"]),
            "macro.open_learning.b": _macro("b", macros=["macro.open_learning.a"]),
        }
        assert _hash(_mv(macros=["macro.open_learning.a"]), macros)

    def test_package_macro_change_is_not_a_change(self):
        """Otherwise a dbt or dbt-starrocks upgrade would drop and recreate
        every view ol-analytics-api is serving from.
        """
        node = _mv(macros=["macro.dbt.dateadd"])

        def macros(sql):
            return {"macro.dbt.dateadd": _macro(sql, package="dbt")}

        assert _hash(node, macros("v1")) == _hash(node, macros("v2"))

    def test_project_macro_reached_through_a_package_macro_counts(self):
        def macros(sql):
            return {
                "macro.dbt.dispatching": _macro(
                    "x", package="dbt", macros=["macro.open_learning.impl"]
                ),
                "macro.open_learning.impl": _macro(sql),
            }

        node = _mv(macros=["macro.dbt.dispatching"])
        assert _hash(node, macros("v1")) != _hash(node, macros("v2"))

    def test_comment_only_edits_are_not_a_change(self):
        """A full refresh leaves each view briefly absent. Rewording a comment
        should not cost that.
        """
        before = _mv(raw_code="-- Grain: org\n{# why #}\nselect 1\n")
        after = _mv(raw_code="-- Grain: org x month\n\n{# why,\nat length #}\nselect 1")
        assert _hash(before) == _hash(after)

    def test_whitespace_control_on_a_jinja_comment_is_a_change(self):
        """`1 - {# c #} -1` renders as a subtraction. With `{#- c -#}` the
        whitespace on both sides goes and it renders `1 --1`, a comment.
        """
        plain = _mv(raw_code="select 1 - {# c #} -1")
        assert _hash(plain) != _hash(_mv(raw_code="select 1 - {#- c -#} -1"))
        assert _hash(plain) != _hash(_mv(raw_code="select 1 - {# c -#} -1"))
        assert _hash(plain) == _hash(_mv(raw_code="select 1 - {# reworded #} -1"))

    def test_whitespace_inside_a_line_is_a_change(self):
        """It could be inside a string literal, and a missed change is the bug
        this exists to prevent.
        """
        assert _hash(_mv(raw_code="select 'a b'")) != _hash(
            _mv(raw_code="select 'a  b'")
        )

    def test_a_trailing_comment_is_a_change(self):
        """Only whole-line comments are dropped: `--` after code could be inside
        a string literal.
        """
        assert _hash(_mv(raw_code="select 1 -- one")) != _hash(
            _mv(raw_code="select 1 -- uno")
        )

    def test_renamed_source_is_a_change(self):
        """The model's SQL says `source('dimensional', 'x')` either way; the
        relation it resolves to is in the sources YAML.
        """
        node = _mv(relations=["source.open_learning.dimensional.x"])

        def sources(relation_name):
            return {
                "source.open_learning.dimensional.x": {"relation_name": relation_name}
            }

        assert _hash(node, sources=sources("`lake`.`dim`.`x`")) != _hash(
            node, sources=sources("`lake`.`dim`.`x_v2`")
        )

    def test_build_config_is_part_of_the_definition(self):
        before = _mv(unrendered_config={"buckets": "8"})
        after = _mv(unrendered_config={"buckets": "16"})
        assert _hash(before) != _hash(after)

    def test_config_dbt_applies_without_a_rebuild_is_not(self):
        before = _mv(unrendered_config={"buckets": "8"})
        after = _mv(
            unrendered_config={
                "buckets": "8",
                "meta": {"change_tracking": {"key": ["org_key"]}},
                "tags": ["starrocks", "pii"],
                "grants": {"select": ["reader"]},
            }
        )
        assert _hash(before) == _hash(after)

    def test_only_starrocks_materialized_views(self):
        manifest = _manifest(
            [
                _mv("mv_a"),
                _model_node("a_table", materialized="table", tags=["starrocks"]),
                _model_node("trino_mv", materialized="materialized_view", tags=[]),
            ]
        )
        assert list(definition_hashes(manifest)) == ["b2b_analytics.mv_a"]


class TestRedefinedRelations:
    LIVE = {"b2b_analytics.mv_a": {"org_key"}}  # noqa: RUF012

    def test_recorded_hash_matches(self):
        """The common case, and like matching columns it must not force a full
        refresh.
        """
        definitions = {"b2b_analytics.mv_a": "abc"}
        assert redefined_relations(definitions, definitions, self.LIVE) == []

    def test_recorded_hash_differs(self):
        assert redefined_relations(
            {"b2b_analytics.mv_a": "new"}, {"b2b_analytics.mv_a": "old"}, self.LIVE
        ) == ["b2b_analytics.mv_a"]

    def test_a_live_view_with_no_record_is_rebuilt(self):
        """Nothing says which definition it was built from. This is every view
        on the first build after the check ships.
        """
        assert redefined_relations({"b2b_analytics.mv_a": "new"}, {}, self.LIVE) == [
            "b2b_analytics.mv_a"
        ]

    def test_a_view_that_does_not_exist_yet_is_not_rebuilt(self):
        assert redefined_relations({"b2b_analytics.mv_new": "new"}, {}, self.LIVE) == []


class TestRecordedDefinitions:
    RELATIONS = ("b2b_analytics.mv_a", "b2b_learner_records.mv_b")

    def test_a_schema_with_no_table_is_not_queried(self):
        """Before the first recording the table does not exist, and selecting
        from it would fail the build.
        """
        queries: list[str] = []
        live = {"b2b_analytics.mv_a": {"org_key"}}
        assert recorded_definitions(self.RELATIONS, live, queries.append) == {}
        assert queries == []

    def test_reads_each_schema_that_has_one(self):
        rows = {
            "b2b_analytics.dbt_mv_definitions": [
                {"relation_name": "b2b_analytics.mv_a", "definition_hash": OLD}
            ],
            "b2b_learner_records.dbt_mv_definitions": [
                {"relation_name": "b2b_learner_records.mv_b", "definition_hash": NEW}
            ],
        }
        live = {table: {"relation_name", "definition_hash"} for table in rows}
        assert recorded_definitions(
            self.RELATIONS, live, lambda sql: rows[sql.rsplit(" ", 1)[1]]
        ) == {"b2b_analytics.mv_a": OLD, "b2b_learner_records.mv_b": NEW}

    def test_drops_a_row_that_could_not_be_written_back_as_a_literal(self):
        """The table is plain data in StarRocks, and its values go back into a
        statement on the next recording. A view with no usable record is
        rebuilt, which replaces the row.
        """
        table = "b2b_analytics.dbt_mv_definitions"
        rows = [
            {"relation_name": "b2b_analytics.mv_a", "definition_hash": "x' or '1"},
            {"relation_name": "b2b_analytics.mv'--", "definition_hash": OLD},
            {"relation_name": "b2b_analytics.mv_ok", "definition_hash": OLD},
        ]
        live = {table: {"relation_name", "definition_hash"}}
        assert recorded_definitions(self.RELATIONS, live, lambda _sql: rows) == {
            "b2b_analytics.mv_ok": OLD
        }


class TestBuiltRelations:
    def test_successful_materialized_views_only(self):
        manifest = _manifest(
            [
                _mv("mv_ok"),
                _mv("mv_failed"),
                _mv("mv_not_selected"),
                _model_node("a_table", materialized="table", tags=["starrocks"]),
            ]
        )
        run_results = {
            "results": [
                {"unique_id": "model.open_learning.mv_ok", "status": "success"},
                {"unique_id": "model.open_learning.mv_failed", "status": "error"},
                {"unique_id": "model.open_learning.a_table", "status": "success"},
                {"unique_id": "test.open_learning.not_null_x", "status": "pass"},
            ]
        }
        assert built_relations(manifest, run_results) == {"b2b_analytics.mv_ok"}


class TestRecordDefinitionsSql:
    def test_one_create_and_one_overwrite_per_schema(self):
        statements = record_definitions_sql(
            {
                "b2b_analytics.mv_b": NEW,
                "b2b_analytics.mv_a": OLD,
                "b2b_learner_records.mv_c": NEW,
            }
        )
        rows = (
            "select cast('b2b_analytics.mv_a' as varchar(255)) as relation_name, "
            f"cast('{OLD}' as varchar(64)) as definition_hash union all "
            "select cast('b2b_analytics.mv_b' as varchar(255)) as relation_name, "
            f"cast('{NEW}' as varchar(64)) as definition_hash"
        )
        table = "b2b_analytics.dbt_mv_definitions"
        columns = "relation_name, definition_hash"
        assert statements[:2] == [
            f"create table if not exists {table} as {rows}",
            f"insert overwrite {table} ({columns}) select {columns} from ({rows}) d",  # noqa: S608
        ]
        assert [s.split(" as ")[0].split(" (")[0] for s in statements[2:]] == [
            "create table if not exists b2b_learner_records.dbt_mv_definitions",
            "insert overwrite b2b_learner_records.dbt_mv_definitions",
        ]

    def test_refuses_a_name_it_cannot_write_as_a_literal(self):
        with pytest.raises(ValueError, match="schema-qualified"):
            record_definitions_sql({"b2b_analytics.mv'; drop table x": NEW})

    def test_refuses_a_hash_it_cannot_write_as_a_literal(self):
        with pytest.raises(ValueError, match="sha256"):
            record_definitions_sql({"b2b_analytics.mv_a": "x' or '1"})


class TestRecordBuiltDefinitions:
    def test_keeps_what_was_not_built_and_forgets_what_left_the_manifest(self):
        """The overwrite replaces the whole table, so a view this run did not
        build has to be written back with the hash it already had, not its
        current one: it still has its old SELECT.
        """
        definitions = {
            "b2b_analytics.mv_built": NEW,
            "b2b_analytics.mv_not_built": NEW,
        }
        recorded = {
            "b2b_analytics.mv_built": OLD,
            "b2b_analytics.mv_not_built": OLD,
            "b2b_analytics.mv_removed": OLD,
        }
        statements: list[str] = []
        record_built_definitions(
            definitions, recorded, ["b2b_analytics.mv_built"], statements.append
        )
        assert statements == record_definitions_sql(
            {"b2b_analytics.mv_built": NEW, "b2b_analytics.mv_not_built": OLD}
        )

    def test_a_build_that_changed_no_definition_writes_nothing(self):
        """Every ordinary nightly build. Each statement costs a Vault credential
        and a connection.
        """
        definitions = {"b2b_analytics.mv_a": NEW, "b2b_analytics.mv_b": NEW}
        statements: list[str] = []
        record_built_definitions(
            definitions, dict(definitions), list(definitions), statements.append
        )
        assert statements == []

    def test_a_new_view_is_recorded_beside_the_existing_ones(self):
        definitions = {"b2b_analytics.mv_a": NEW, "b2b_analytics.mv_new": NEW}
        statements: list[str] = []
        record_built_definitions(
            definitions,
            {"b2b_analytics.mv_a": NEW},
            list(definitions),
            statements.append,
        )
        assert statements == record_definitions_sql(definitions)

    def test_ignores_a_built_relation_the_manifest_does_not_define(self):
        statements: list[str] = []
        record_built_definitions(
            {"b2b_analytics.mv_a": NEW},
            {"b2b_analytics.mv_a": NEW},
            ["b2b_analytics.mv_a", "b2b_analytics.mv_elsewhere"],
            statements.append,
        )
        assert statements == []

    def test_nothing_built_and_nothing_recorded_writes_nothing(self):
        statements: list[str] = []
        record_built_definitions({"b2b_analytics.mv_a": NEW}, {}, [], statements.append)
        assert statements == []


# Verbatim (stack traces trimmed) from the 2026-09-25 b2b_analytics_starrocks_job
# runs 4b143bb3 and 46b45b4f, which overlapped a Trino rebuild of
# bridge_organization_courserun.
BASE_TABLE_MID_SWAP_FAILURE = (
    "(1064, 'execute task mv-1199507 failed: Refresh mv mv_b2b_learner_enrollment "
    "failed after 1 times, try lock failed: 0, error-msg : "
    "com.starrocks.sql.common.DmlException: Materialized view "
    "b2b_learner_records.mv_b2b_learner_enrollment refresh failed: base table "
    "ol_data_lake_production.ol_warehouse_production_dimensional."
    "bridge_organization_courserun does not exist when collecting snapshot infos')"
)
BASE_TABLE_RECREATED_FAILURE = (
    '(1064, "execute task mv-1199471 failed: Refresh mv mv_b2b_contract_utilization '
    "failed after 1 times, try lock failed: 0, error-msg : "
    "com.starrocks.sql.common.DmlException: Materialized view "
    "b2b_analytics.mv_b2b_contract_utilization set inactive: base table "
    "'bridge_organization_courserun' (catalog=ol_data_lake_production, "
    "db=ol_warehouse_production_dimensional) was recreated but its table type is "
    "not supported for automatic meta repair. Only Hive tables support automatic "
    'repair. Please manually refresh the MV.")'
)
UNRELATED_REFRESH_FAILURE = (
    "(1064, 'Getting analyzing error. Detail message: Unknown column org_key.')"
)


class ScriptedStarRocks:
    """Plays back a per-relation sequence of failures, then succeeds."""

    def __init__(self, failures: dict[str, list[str]]) -> None:
        self.failures = {name: list(msgs) for name, msgs in failures.items()}
        self.statements: list[str] = []

    def execute(self, sql: str) -> None:
        self.statements.append(sql)
        relation = sql.split()[3]
        pending = self.failures.get(relation)
        if pending:
            raise RuntimeError(pending.pop(0))


def _refresh(starrocks, relations, sleeps):
    refresh_materialized_views(
        relations,
        starrocks.execute,
        log=logging.getLogger("test"),
        sleep=sleeps.append,
    )


class TestRefreshMaterializedViews:
    @pytest.mark.parametrize(
        "message", [BASE_TABLE_MID_SWAP_FAILURE, BASE_TABLE_RECREATED_FAILURE]
    )
    def test_a_rebuilt_base_table_is_retried(self, message):
        """Both 09-25 failures clear on the next REFRESH once the new table exists."""
        starrocks = ScriptedStarRocks({"b2b_analytics.mv_a": [message]})
        sleeps: list[float] = []
        _refresh(starrocks, ["b2b_analytics.mv_a"], sleeps)
        assert (
            starrocks.statements
            == ["REFRESH MATERIALIZED VIEW b2b_analytics.mv_a WITH SYNC MODE"] * 2
        )
        assert sleeps == [MV_REFRESH_RETRY_DELAY_SECONDS]

    def test_the_recreate_error_after_a_mid_swap_failure(self):
        """The 09-25 sequence on one MV: first the swap, then the meta-repair
        refusal on the first REFRESH that sees the new table.
        """
        starrocks = ScriptedStarRocks(
            {
                "b2b_analytics.mv_a": [
                    BASE_TABLE_MID_SWAP_FAILURE,
                    BASE_TABLE_RECREATED_FAILURE,
                ]
            }
        )
        _refresh(starrocks, ["b2b_analytics.mv_a"], [])
        assert len(starrocks.statements) == MAX_MV_REFRESH_ATTEMPTS

    def test_an_unrelated_error_is_not_retried(self):
        starrocks = ScriptedStarRocks(
            {"b2b_analytics.mv_a": [UNRELATED_REFRESH_FAILURE]}
        )
        sleeps: list[float] = []
        with pytest.raises(MaterializedViewRefreshError) as excinfo:
            _refresh(starrocks, ["b2b_analytics.mv_a"], sleeps)
        assert len(starrocks.statements) == 1
        assert sleeps == []
        assert set(excinfo.value.failures) == {"b2b_analytics.mv_a"}

    def test_gives_up_after_the_last_attempt(self):
        starrocks = ScriptedStarRocks(
            {"b2b_analytics.mv_a": [BASE_TABLE_RECREATED_FAILURE] * 10}
        )
        sleeps: list[float] = []
        with pytest.raises(MaterializedViewRefreshError):
            _refresh(starrocks, ["b2b_analytics.mv_a"], sleeps)
        assert len(starrocks.statements) == MAX_MV_REFRESH_ATTEMPTS
        # No sleep after the final attempt.
        assert sleeps == [MV_REFRESH_RETRY_DELAY_SECONDS] * (
            MAX_MV_REFRESH_ATTEMPTS - 1
        )

    def test_one_failure_does_not_stop_the_rest(self):
        """On 09-25 the first failure ended the asset, so every MV after it in
        the list kept the previous day's data.
        """
        starrocks = ScriptedStarRocks(
            {"b2b_analytics.mv_a": [UNRELATED_REFRESH_FAILURE]}
        )
        with pytest.raises(MaterializedViewRefreshError) as excinfo:
            _refresh(starrocks, ["b2b_analytics.mv_a", "b2b_analytics.mv_b"], [])
        assert starrocks.statements[-1] == (
            "REFRESH MATERIALIZED VIEW b2b_analytics.mv_b WITH SYNC MODE"
        )
        assert set(excinfo.value.failures) == {"b2b_analytics.mv_a"}

    def test_clean_run_does_not_sleep(self):
        starrocks = ScriptedStarRocks({})
        sleeps: list[float] = []
        _refresh(starrocks, ["b2b_analytics.mv_a", "b2b_analytics.mv_b"], sleeps)
        assert len(starrocks.statements) == 2
        assert sleeps == []


LEARNER_VIEW = ChangeTrackedView(
    relation="b2b_learner_records.mv_b2b_learner",
    key=("organization_key", "user_pk"),
    identity=("sso_organization_id", "user_global_id"),
    tracked=("email", "courses_enrolled"),
)


def _tracked_node(**tracking):
    return _model_node(
        "mv_b2b_learner",
        schema="b2b_learner_records",
        materialized="materialized_view",
        tags=["starrocks"],
        columns=[
            "organization_key",
            "sso_organization_id",
            "user_pk",
            "user_global_id",
            "email",
            "courses_enrolled",
        ],
        meta={"change_tracking": tracking},
    )


class TestChangeTrackedViews:
    def test_tracks_every_column_outside_the_key_and_identity(self):
        manifest = _manifest(
            [
                _tracked_node(
                    key=["organization_key", "user_pk"],
                    identity=["sso_organization_id", "user_global_id"],
                )
            ]
        )
        assert change_tracked_views(manifest) == [LEARNER_VIEW]

    def test_a_view_without_the_meta_key_is_not_tracked(self):
        manifest = _manifest(
            [
                _model_node(
                    "mv_b2b_program_funnel",
                    materialized="materialized_view",
                    tags=["starrocks"],
                    columns=["organization_key"],
                )
            ]
        )
        assert change_tracked_views(manifest) == []

    def test_an_undocumented_key_column_is_refused(self):
        """Otherwise it surfaces as an unknown column in StarRocks, after the
        refresh, with nothing pointing at the YAML.
        """
        manifest = _manifest(
            [_tracked_node(key=["organization_key", "learner_pk"], identity=[])]
        )
        with pytest.raises(ValueError, match="learner_pk"):
            change_tracked_views(manifest)

    def test_an_empty_key_is_refused(self):
        manifest = _manifest([_tracked_node(key=[], identity=[])])
        with pytest.raises(ValueError, match="non-empty key"):
            change_tracked_views(manifest)


class TestChangeLogSql:
    """The statements' behavior was checked against StarRocks 4.1.6, the
    deployed version, not only their text: an unchanged row keeps changed_on, a
    changed, new, removed or returning row takes the stamp's time, a removed
    row stays with is_deleted set, and a null turning into '' is a change.
    """

    def test_seed_only_creates_a_missing_log(self):
        sql = seed_change_log_sql(LEARNER_VIEW)
        assert sql.startswith(
            "create table if not exists b2b_learner_records.mv_b2b_learner_changes as"
        )

    def test_current_rows_are_grouped_by_the_key(self):
        """A key duplicated in the MV would otherwise be written to the log
        twice and multiplied again by every later stamp's join.
        """
        for sql in (
            seed_change_log_sql(LEARNER_VIEW),
            stamp_change_log_sql(LEARNER_VIEW),
        ):
            assert "group by `organization_key`, `user_pk`" in sql

    def test_a_duplicated_key_is_hashed_over_all_its_rows(self):
        """max() over the rows' hashes would not move when a row other than
        the greatest changed, and per-column maxima could pair identity values
        from different rows.
        """
        sql = stamp_change_log_sql(LEARNER_VIEW)
        assert (
            "md5(array_join(array_sort(array_agg(content_hash)), ',')) as row_hash"
            in sql
        )
        assert "max_by(`sso_organization_id`, content_hash)" in sql
        assert "max_by(`user_global_id`, content_hash)" in sql
        assert "max(`" not in sql

    def test_identity_columns_are_hashed_and_key_columns_are_not(self):
        sql = stamp_change_log_sql(LEARNER_VIEW)
        hashed = sql[sql.index("md5(concat_ws") : sql.index(" as content_hash")]
        assert re.findall(r"cast\(`(\w+)` as varchar\)", hashed) == [
            "sso_organization_id",
            "user_global_id",
            "email",
            "courses_enrolled",
        ]

    def test_stamp_overwrites_from_a_null_safe_join_on_the_key(self):
        sql = stamp_change_log_sql(LEARNER_VIEW)
        assert sql.startswith(
            "insert overwrite b2b_learner_records.mv_b2b_learner_changes "
            "(`organization_key`, `user_pk`, `sso_organization_id`, `user_global_id`, "
            "`row_hash`, `changed_on`, `is_deleted`) select"
        )
        assert sql.endswith(
            "full outer join b2b_learner_records.mv_b2b_learner_changes c "
            "on m.`organization_key` <=> c.`organization_key` "
            "and m.`user_pk` <=> c.`user_pk`"
        )


class TestStampChangeLog:
    def test_seeds_then_stamps(self):
        statements: list[str] = []
        stamp_change_log(LEARNER_VIEW, statements.append, log=logging.getLogger("test"))
        assert statements == [
            seed_change_log_sql(LEARNER_VIEW),
            stamp_change_log_sql(LEARNER_VIEW),
        ]


class TestAfterRefresh:
    def _run(self, starrocks, relations, after_refresh):
        refresh_materialized_views(
            relations,
            starrocks.execute,
            log=logging.getLogger("test"),
            sleep=lambda _: None,
            after_refresh=after_refresh,
        )

    def test_a_failed_refresh_is_not_followed_up(self):
        """`dbt build --full-refresh` recreates an MV empty. Stamping a change
        log from one whose refresh then failed would mark every record deleted.
        """
        starrocks = ScriptedStarRocks(
            {"b2b_analytics.mv_a": [UNRELATED_REFRESH_FAILURE]}
        )
        seen: list[str] = []
        with pytest.raises(MaterializedViewRefreshError):
            self._run(
                starrocks, ["b2b_analytics.mv_a", "b2b_analytics.mv_b"], seen.append
            )
        assert seen == ["b2b_analytics.mv_b"]

    def test_runs_once_after_a_retried_refresh(self):
        starrocks = ScriptedStarRocks(
            {"b2b_analytics.mv_a": [BASE_TABLE_RECREATED_FAILURE]}
        )
        seen: list[str] = []
        self._run(starrocks, ["b2b_analytics.mv_a"], seen.append)
        assert seen == ["b2b_analytics.mv_a"]

    def test_a_failing_follow_up_does_not_rerun_the_refresh(self):
        """Even when its error reads like a rebuilt base table. The REFRESH
        already succeeded, and the retry loop is for the REFRESH alone.
        """

        def after_refresh(_relation):
            raise RuntimeError(BASE_TABLE_RECREATED_FAILURE)

        starrocks = ScriptedStarRocks({})
        with pytest.raises(MaterializedViewRefreshError) as excinfo:
            self._run(starrocks, ["b2b_analytics.mv_a"], after_refresh)
        assert starrocks.statements == [
            "REFRESH MATERIALIZED VIEW b2b_analytics.mv_a WITH SYNC MODE"
        ]
        assert set(excinfo.value.failures) == {"b2b_analytics.mv_a"}

    def test_a_failing_follow_up_is_reported_and_does_not_stop_the_rest(self):
        def after_refresh(relation):
            if relation == "b2b_analytics.mv_a":
                msg = "stamp failed"
                raise RuntimeError(msg)

        starrocks = ScriptedStarRocks({})
        with pytest.raises(MaterializedViewRefreshError) as excinfo:
            self._run(
                starrocks, ["b2b_analytics.mv_a", "b2b_analytics.mv_b"], after_refresh
            )
        assert set(excinfo.value.failures) == {"b2b_analytics.mv_a"}
        assert starrocks.statements[-1] == (
            "REFRESH MATERIALIZED VIEW b2b_analytics.mv_b WITH SYNC MODE"
        )
