"""Tests for the StarRocks dbt retry classifier and MV-relation derivation.

The failure texts below are verbatim from the production Dagster run that
motivated this module (run acc2b10c, 2026-07-22), not invented -- the point of
the retry pattern is that it matches what StarRocks actually emits.
"""

import pytest
from lakehouse.lib.starrocks_dbt import (
    MAX_BUILD_ATTEMPTS,
    RETRY_BASE_DELAY,
    looks_retriable,
    materialized_view_relations,
    retry_delay,
)

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


def _model_node(name, *, schema="b2b_analytics", materialized, tags):
    return {
        "resource_type": "model",
        "schema": schema,
        "alias": name,
        "tags": tags,
        "config": {"materialized": materialized, "tags": tags},
    }


def _manifest(nodes):
    return {"nodes": {f"model.open_learning.{n['alias']}": n for n in nodes}}


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
        """smoke_test is tagged starrocks but materializes as a table; REFRESH
        MATERIALIZED VIEW against it is an error.
        """
        manifest = _manifest(
            [
                _model_node(
                    "mv_b2b_program_funnel",
                    materialized="materialized_view",
                    tags=["starrocks"],
                ),
                _model_node(
                    "smoke_test",
                    materialized="table",
                    tags=["starrocks", "b2b_analytics", "smoke_test"],
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
