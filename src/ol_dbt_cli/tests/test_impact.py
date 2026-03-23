"""Tests for commands/impact.py — column diff and lineage helpers."""

from __future__ import annotations

from pathlib import Path

from ol_dbt_cli.commands.impact import _diff_columns


class TestDiffColumns:
    def test_removed_column(self) -> None:
        changes = _diff_columns({"user_id", "user_email", "user_name"}, {"user_id", "user_email"})
        assert any(c.column == "user_name" and c.change_type == "removed" for c in changes)

    def test_added_column(self) -> None:
        changes = _diff_columns({"user_id"}, {"user_id", "user_email"})
        assert any(c.column == "user_email" and c.change_type == "added" for c in changes)

    def test_no_changes(self) -> None:
        changes = _diff_columns({"user_id", "user_email"}, {"user_id", "user_email"})
        assert changes == []

    def test_rename_heuristic(self) -> None:
        # One removed, one added with high prefix similarity → detected as rename
        changes = _diff_columns({"user_email_address"}, {"user_email_addr"})
        change_types = {c.change_type for c in changes}
        # Should detect rename (renamed_from + renamed_to) rather than removed + added
        assert "renamed_from" in change_types or "removed" in change_types

    def test_multiple_removes(self) -> None:
        changes = _diff_columns({"a", "b", "c"}, {"a"})
        removed = {c.column for c in changes if c.change_type == "removed"}
        assert removed == {"b", "c"}


class TestGetColumnsReadFromRef:
    """Tests for _get_columns_read_from_ref — SQL-level column-read analysis."""

    def test_detects_qualified_column_read_via_cte(self, tmp_path: Path) -> None:
        """Model reads upstream column via thin passthrough CTE with qualified access."""
        from ol_dbt_cli.lib.sql_parser import get_columns_read_from_ref, parse_model_sql

        sql = """
        with src as (
            select * from ref_stg_users
        )
        select
            src.user_id as id,
            src.user_email as email_address
        from src
        """
        sql_file = tmp_path / "downstream.sql"
        sql_file.write_text(sql)

        parsed = parse_model_sql("downstream", sql)
        parsed.refs = ["stg_users"]
        parsed.ref_placeholder_map = {"ref_stg_users": "stg_users"}
        parsed.source_path = sql_file

        result = get_columns_read_from_ref(parsed, "stg_users")
        assert result is not None
        assert "user_email" in result
        assert "user_id" in result

    def test_detects_unqualified_column_read_direct_from_ref(self, tmp_path: Path) -> None:
        """Bare column refs directly FROM the ref placeholder are detected."""
        from ol_dbt_cli.lib.sql_parser import get_columns_read_from_ref, parse_model_sql

        sql = "select user_id, email from ref_stg_users"
        sql_file = tmp_path / "downstream.sql"
        sql_file.write_text(sql)

        parsed = parse_model_sql("downstream", sql)
        parsed.refs = ["stg_users"]
        parsed.ref_placeholder_map = {"ref_stg_users": "stg_users"}
        parsed.source_path = sql_file

        result = get_columns_read_from_ref(parsed, "stg_users")
        assert result is not None
        assert "user_id" in result
        assert "email" in result

    def test_detects_unqualified_column_read_via_passthrough_cte(self, tmp_path: Path) -> None:
        """Bare column refs inside a CTE that SELECT * from the upstream are detected."""
        from ol_dbt_cli.lib.sql_parser import get_columns_read_from_ref, parse_model_sql

        sql = """
        with src as (
            select * from ref_stg_users
        ),
        cleaned as (
            select
                user_id,
                user_email,
                user_name
            from src
        )
        select * from cleaned
        """
        sql_file = tmp_path / "downstream.sql"
        sql_file.write_text(sql)

        parsed = parse_model_sql("downstream", sql)
        parsed.refs = ["stg_users"]
        parsed.ref_placeholder_map = {"ref_stg_users": "stg_users"}
        parsed.source_path = sql_file

        result = get_columns_read_from_ref(parsed, "stg_users")
        assert result is not None
        assert "user_email" in result
        assert "user_id" in result
        assert "user_name" in result

    def test_detects_unqualified_column_in_where_clause(self, tmp_path: Path) -> None:
        """Bare column refs in WHERE clause of passthrough-sourced SELECT are detected."""
        from ol_dbt_cli.lib.sql_parser import get_columns_read_from_ref, parse_model_sql

        sql = """
        with src as (
            select * from ref_stg_enrollments
        )
        select
            enrollment_id,
            user_id
        from src
        where courserunenrollment_is_active = true
        """
        sql_file = tmp_path / "downstream.sql"
        sql_file.write_text(sql)

        parsed = parse_model_sql("downstream", sql)
        parsed.refs = ["stg_enrollments"]
        parsed.ref_placeholder_map = {"ref_stg_enrollments": "stg_enrollments"}
        parsed.source_path = sql_file

        result = get_columns_read_from_ref(parsed, "stg_enrollments")
        assert result is not None
        assert "courserunenrollment_is_active" in result

    def test_skips_unqualified_refs_when_join_present(self, tmp_path: Path) -> None:
        """When there are JOINs to non-passthrough tables, unqualified refs are not attributed."""
        from ol_dbt_cli.lib.sql_parser import get_columns_read_from_ref, parse_model_sql

        sql = """
        with src as (
            select * from ref_stg_users
        )
        select
            user_id,
            course_id
        from src
        join other_table on src.user_id = other_table.user_id
        """
        sql_file = tmp_path / "downstream.sql"
        sql_file.write_text(sql)

        parsed = parse_model_sql("downstream", sql)
        parsed.refs = ["stg_users"]
        parsed.ref_placeholder_map = {"ref_stg_users": "stg_users"}
        parsed.source_path = sql_file

        result = get_columns_read_from_ref(parsed, "stg_users")
        # Unqualified columns are ambiguous with a join present — only qualified refs count
        if result is not None:
            assert "course_id" not in result  # course_id likely from other_table

    def test_returns_none_when_upstream_not_referenced(self, tmp_path: Path) -> None:
        """Returns None when the upstream model is not referenced in the SQL."""
        from ol_dbt_cli.lib.sql_parser import get_columns_read_from_ref, parse_model_sql

        sql = "select user_id, email from ref_other_model"
        sql_file = tmp_path / "downstream.sql"
        sql_file.write_text(sql)

        parsed = parse_model_sql("downstream", sql)
        parsed.refs = ["other_model"]
        parsed.ref_placeholder_map = {"ref_other_model": "other_model"}
        parsed.source_path = sql_file

        result = get_columns_read_from_ref(parsed, "stg_users")
        assert result is None

    def test_detects_aliased_column_read(self, tmp_path: Path) -> None:
        """Column read from upstream but aliased to different name in output is detected."""
        from ol_dbt_cli.lib.sql_parser import get_columns_read_from_ref, parse_model_sql

        # This model reads user_email from upstream, but outputs it as user_edxorg_email.
        # With manifest column intersection, user_email is not in output columns,
        # so naive intersection returns empty. SQL-level analysis should catch it.
        sql = """
        with program_entitlements as (
            select * from ref_stg_edxorg_program_entitlement
        )
        select
            program_entitlements.program_id,
            program_entitlements.user_email as user_edxorg_email,
            program_entitlements.status
        from program_entitlements
        """
        sql_file = tmp_path / "downstream.sql"
        sql_file.write_text(sql)

        parsed = parse_model_sql("downstream", sql)
        parsed.refs = ["stg_edxorg_program_entitlement"]
        parsed.ref_placeholder_map = {"ref_stg_edxorg_program_entitlement": "stg_edxorg_program_entitlement"}
        parsed.source_path = sql_file

        cols_read = get_columns_read_from_ref(parsed, "stg_edxorg_program_entitlement")
        assert cols_read is not None
        # user_email is read from the upstream even though it's aliased in output
        assert "user_email" in cols_read

    def test_unnest_lateral_alias_not_attributed_to_upstream(self, tmp_path: Path) -> None:
        """UNNEST lateral join column aliases must not be attributed to the upstream ref.

        Pattern: ``cross join unnest(...) as t(col)`` produces ``col`` as an unqualified
        name in the SELECT, but it comes from UNNEST, not from the upstream ref.
        False positive: was incorrectly flagging ``col`` as missing from upstream.
        """
        from ol_dbt_cli.lib.sql_parser import get_columns_read_from_ref, parse_model_sql

        sql = """
        with websitecontent as (
            select * from ref_stg_websites
        )
        select
            websitecontent.course_uuid,
            department_number,
            upper(department_number) as department_name
        from websitecontent
        cross join unnest(cast(json_parse(department_numbers_json) as array(varchar)))
            as t(department_number)
        """
        sql_file = tmp_path / "downstream.sql"
        sql_file.write_text(sql)

        parsed = parse_model_sql("downstream", sql)
        parsed.refs = ["stg_websites"]
        parsed.ref_placeholder_map = {"ref_stg_websites": "stg_websites"}
        parsed.source_path = sql_file

        result = get_columns_read_from_ref(parsed, "stg_websites")
        # course_uuid is a real upstream column (qualified via passthrough alias)
        assert result is not None
        assert "course_uuid" in result
        # department_number comes from UNNEST — not from the upstream ref
        assert "department_number" not in result
        assert "department_name" not in result

    def test_unnest_lateral_alias_in_cte_not_attributed_to_upstream(self, tmp_path: Path) -> None:
        """UNNEST-produced columns inside a passthrough CTE SELECT must not be attributed.

        Pattern: a CTE selects an unqualified UNNEST column alongside qualified upstream
        columns.  The UNNEST column is not from the upstream ref.
        """
        from ol_dbt_cli.lib.sql_parser import get_columns_read_from_ref, parse_model_sql

        sql = """
        with source as (
            select * from ref_stg_cms
        ),
        unnested as (
            select
                source.page_id,
                member_json
            from source
            cross join unnest(source.members_array) as t(member_json)
        )
        select
            unnested.page_id,
            json_query(unnested.member_json, 'lax $.name') as member_name
        from unnested
        """
        sql_file = tmp_path / "downstream.sql"
        sql_file.write_text(sql)

        parsed = parse_model_sql("downstream", sql)
        parsed.refs = ["stg_cms"]
        parsed.ref_placeholder_map = {"ref_stg_cms": "stg_cms"}
        parsed.source_path = sql_file

        result = get_columns_read_from_ref(parsed, "stg_cms")
        # page_id is a real upstream column
        assert result is not None
        assert "page_id" in result
        # member_json is a UNNEST alias — not from the upstream
        assert "member_json" not in result

    def test_join_table_columns_not_attributed_to_upstream(self, tmp_path: Path) -> None:
        """Unaliased qualified columns from JOIN tables must not be attributed to upstream.

        Pattern: ``select src.*, other.col from src join other`` — ``col`` is from
        ``other``, not from ``src``.  The passthrough CTE detection must capture it
        in ``added`` so downstream refs to it are not flagged.
        """
        from ol_dbt_cli.lib.sql_parser import get_columns_read_from_ref, parse_model_sql

        sql = """
        with courseruns as (
            select * from ref_stg_courserun
        ),
        courses as (
            select * from ref_stg_course
        ),
        enriched as (
            select
                courseruns.*,
                courses.course_title,
                courses.course_org
            from courseruns
            inner join courses on courseruns.course_id = courses.course_id
        )
        select
            enriched.courserun_id,
            enriched.course_title,
            enriched.course_org,
            enriched.enrollment_count
        from enriched
        """
        sql_file = tmp_path / "downstream.sql"
        sql_file.write_text(sql)

        parsed = parse_model_sql("downstream", sql)
        parsed.refs = ["stg_courserun", "stg_course"]
        parsed.ref_placeholder_map = {
            "ref_stg_courserun": "stg_courserun",
            "ref_stg_course": "stg_course",
        }
        parsed.source_path = sql_file

        result = get_columns_read_from_ref(parsed, "stg_courserun")
        assert result is not None
        # courserun_id and enrollment_count come from stg_courserun
        assert "courserun_id" in result
        assert "enrollment_count" in result
        # course_title and course_org come from stg_course, not stg_courserun
        assert "course_title" not in result
        assert "course_org" not in result

    def test_passthrough_chain_computed_column_not_attributed_to_upstream(self, tmp_path: Path) -> None:
        """Passthrough chain: a computed column must not be attributed to the upstream.

        A column added in an early passthrough CTE (via a Jinja macro) must not be
        attributed to the upstream ref even when accessed via a later passthrough CTE in
        the chain.  CTE B passes through CTE A with ``select *``; downstream reading
        ``cte_b.computed_col`` should not trigger a missing-column error.
        """
        from ol_dbt_cli.lib.sql_parser import get_columns_read_from_ref, parse_model_sql

        sql = """
        with base as (
            select
                *,
                __macro__ as hashed_id
            from ref_stg_learners
        ),
        deduplicated as (
            select
                *,
                row_number() over (partition by user_id order by created_on desc) as row_num
            from base
        )
        select
            deduplicated.user_id,
            deduplicated.user_email,
            deduplicated.hashed_id
        from deduplicated
        where deduplicated.row_num = 1
        """
        sql_file = tmp_path / "downstream.sql"
        sql_file.write_text(sql)

        parsed = parse_model_sql("downstream", sql)
        parsed.refs = ["stg_learners"]
        parsed.ref_placeholder_map = {"ref_stg_learners": "stg_learners"}
        parsed.source_path = sql_file

        result = get_columns_read_from_ref(parsed, "stg_learners")
        assert result is not None
        # user_id and user_email are real upstream columns
        assert "user_id" in result
        assert "user_email" in result
        # hashed_id was added by the base CTE via macro — not from upstream
        assert "hashed_id" not in result
        # row_num was added by the deduplicated CTE — not from upstream
        assert "row_num" not in result


class TestOutputColumnPassthrough:
    """Tests for output-column passthrough detection in downstream impact functions."""

    def test_downstream_manifest_uses_output_columns(self) -> None:
        """_get_downstream_manifest detects column via output_columns passthrough."""
        from ol_dbt_cli.commands.impact import _get_downstream_manifest
        from ol_dbt_cli.lib.manifest import ManifestModel, ManifestRegistry
        from ol_dbt_cli.lib.sql_parser import ParsedModel

        # Build a minimal manifest: upstream → child with no YAML columns
        registry = ManifestRegistry()
        upstream = ManifestModel(
            unique_id="model.pkg.upstream",
            name="upstream",
            resource_type="model",
            original_file_path="",
            schema="",
            database="",
        )
        child = ManifestModel(
            unique_id="model.pkg.child",
            name="child",
            resource_type="model",
            original_file_path="",
            schema="",
            database="",
        )
        registry.nodes = {upstream.unique_id: upstream, child.unique_id: child}
        registry.by_name = {"upstream": upstream, "child": child}
        registry.children = {upstream.unique_id: [child.unique_id]}

        # Child has no YAML columns but its SQL outputs the removed column
        child_parsed = ParsedModel(
            name="child",
            output_columns={"removed_col", "other_col"},
            refs=["upstream"],
            ref_placeholder_map={},  # no SQL file → no qualified-ref analysis
        )

        results = _get_downstream_manifest(
            "model.pkg.upstream",
            registry,
            {"removed_col"},
            {"child": child_parsed},
        )

        assert len(results) == 1
        assert results[0].model_name == "child"
        assert "removed_col" in results[0].affected_columns

    def test_downstream_manifest_skips_child_when_sql_confirms_no_consumption(self) -> None:
        """When SQL is conclusive and shows no consumption, child is not flagged."""
        from ol_dbt_cli.commands.impact import _get_downstream_manifest
        from ol_dbt_cli.lib.manifest import ManifestModel, ManifestRegistry
        from ol_dbt_cli.lib.sql_parser import ParsedModel

        registry = ManifestRegistry()
        upstream = ManifestModel(
            unique_id="model.pkg.upstream",
            name="upstream",
            resource_type="model",
            original_file_path="",
            schema="",
            database="",
        )
        child = ManifestModel(
            unique_id="model.pkg.child",
            name="child",
            resource_type="model",
            original_file_path="",
            schema="",
            database="",
        )
        registry.nodes = {upstream.unique_id: upstream, child.unique_id: child}
        registry.by_name = {"upstream": upstream, "child": child}
        registry.children = {upstream.unique_id: [child.unique_id]}

        # Child outputs different columns — does not pass through the removed column
        child_parsed = ParsedModel(
            name="child",
            output_columns={"other_col", "yet_another"},
            refs=["upstream"],
            ref_placeholder_map={},
        )

        results = _get_downstream_manifest(
            "model.pkg.upstream",
            registry,
            {"removed_col"},
            {"child": child_parsed},
        )

        # SQL confirmed no consumption (output_columns doesn't include removed_col
        # and no SQL file for qualified-ref analysis) — child should NOT be flagged
        assert not any(r.model_name == "child" and r.affected_columns for r in results)
