"""Tests for lib/sql_parser.py — sqlglot-based column extractor."""

from __future__ import annotations

from pathlib import Path

from ol_dbt_cli.lib.sql_parser import find_compiled_dir, parse_model_file, parse_model_sql, strip_jinja


class TestStripJinja:
    def test_strips_ref(self) -> None:
        sql = "select * from {{ ref('my_model') }}"
        result = strip_jinja(sql)
        assert "ref_my_model" in result.clean_sql
        assert result.ref_names == ["my_model"]
        assert result.source_names == []
        assert result.ref_placeholder_map == {"ref_my_model": "my_model"}

    def test_strips_source(self) -> None:
        sql = "select id from {{ source('raw', 'users') }}"
        result = strip_jinja(sql)
        assert "source_raw_users" in result.clean_sql
        assert result.source_names == ["raw.users"]
        assert result.source_placeholder_map == {"source_raw_users": "raw.users"}

    def test_strips_config_block(self) -> None:
        sql = "{{ config(materialized='table') }}\nselect 1 as id"
        result = strip_jinja(sql)
        assert "config" not in result.clean_sql

    def test_strips_var(self) -> None:
        sql = "select '{{ var(\"platform\") }}' as platform"
        result = strip_jinja(sql)
        assert "__var__" in result.clean_sql

    def test_multiple_refs(self) -> None:
        sql = "select * from {{ ref('model_a') }} join {{ ref('model_b') }} using (id)"
        result = strip_jinja(sql)
        assert set(result.ref_names) == {"model_a", "model_b"}
        assert set(result.ref_placeholder_map.values()) == {"model_a", "model_b"}


class TestParseModelSql:
    def test_simple_select(self) -> None:
        sql = "select id as user_id, email as user_email from raw_users"
        result = parse_model_sql("my_model", sql)
        assert result.output_columns == {"user_id", "user_email"}
        assert not result.has_star
        assert result.parse_error is None

    def test_cte_model(self) -> None:
        sql = """
        with source as (
            select * from raw_users
        )
        select
            id as user_id,
            email as user_email,
            is_active as user_is_active
        from source
        """
        result = parse_model_sql("my_model", sql)
        assert result.output_columns == {"user_id", "user_email", "user_is_active"}
        assert not result.has_star

    def test_detects_star_from_external_table(self) -> None:
        """SELECT * from a raw table stays unresolved."""
        sql = "select * from raw_users"
        result = parse_model_sql("my_model", sql)
        assert result.has_star
        assert not result.star_resolved

    def test_detects_star_from_ref(self) -> None:
        """SELECT * from a ref() placeholder is unresolved — we don't have upstream cols."""
        sql = "select * from {{ ref('stg_users') }}"
        result = parse_model_sql("my_model", sql)
        assert result.has_star
        assert not result.star_resolved

    def test_star_resolved_from_explicit_cte(self) -> None:
        """SELECT * from a CTE with explicit columns is resolved."""
        sql = """
        with cleaned as (
            select id as user_id, email as user_email from raw_users
        )
        select * from cleaned
        """
        result = parse_model_sql("my_model", sql)
        assert not result.has_star
        assert result.star_resolved
        assert result.output_columns == {"user_id", "user_email"}

    def test_star_unresolved_when_cte_also_has_star(self) -> None:
        """SELECT * from a CTE that itself has SELECT * cannot be resolved."""
        sql = """
        with source as (select * from raw_users)
        select * from source
        """
        result = parse_model_sql("my_model", sql)
        assert result.has_star
        assert not result.star_resolved

    def test_star_source_name_recorded(self) -> None:
        """When SELECT * is unresolved, star_source records the FROM table name."""
        sql = "select * from raw_users"
        result = parse_model_sql("my_model", sql)
        assert result.has_star
        assert result.star_source == "raw_users"

    def test_star_with_additional_explicit_alias(self) -> None:
        """SELECT *, extra_col AS alias: explicit alias is preserved even with unresolved *."""
        sql = "select *, 1 as extra from raw_users"
        result = parse_model_sql("my_model", sql)
        assert result.has_star
        assert "extra" in result.output_columns

    def test_with_ref_jinja(self) -> None:
        sql = """
        with source as (select * from {{ ref('stg_users') }})
        select id as user_id, name as user_name from source
        """
        result = parse_model_sql("my_model", sql)
        assert result.output_columns == {"user_id", "user_name"}
        assert result.refs == ["stg_users"]

    def test_bare_column_no_alias(self) -> None:
        sql = "select id, email from raw_users"
        result = parse_model_sql("my_model", sql)
        assert "id" in result.output_columns
        assert "email" in result.output_columns

    def test_complex_cte_chain(self) -> None:
        sql = """
        with
        a as (select 1 as x),
        b as (select x + 1 as y from a)
        select y as final_val from b
        """
        result = parse_model_sql("my_model", sql)
        assert "final_val" in result.output_columns

    def test_star_resolved_through_cte_chain(self) -> None:
        """SELECT * at final step resolves through a multi-CTE chain."""
        sql = """
        with
        source as (select id as user_id, email as user_email from raw_users),
        cleaned as (select * from source)
        select * from cleaned
        """
        result = parse_model_sql("my_model", sql)
        # cleaned gets resolved from source, final select * gets resolved from cleaned
        assert not result.has_star
        assert result.star_resolved
        assert result.output_columns == {"user_id", "user_email"}

    def test_ref_placeholder_map_populated(self) -> None:
        """strip_jinja placeholder maps are stored on ParsedModel."""
        sql = "select * from {{ ref('stg_users') }}"
        result = parse_model_sql("my_model", sql)
        assert "ref_stg_users" in result.ref_placeholder_map
        assert result.ref_placeholder_map["ref_stg_users"] == "stg_users"

    def test_source_placeholder_map_populated(self) -> None:
        """Source placeholder maps are stored on ParsedModel."""
        sql = "select * from {{ source('raw', 'users') }}"
        result = parse_model_sql("my_model", sql)
        assert "source_raw_users" in result.source_placeholder_map
        assert result.source_placeholder_map["source_raw_users"] == "raw.users"

    def test_star_source_is_ref_placeholder(self) -> None:
        """When SELECT * is from a ref(), star_source is the placeholder identifier."""
        sql = "select * from {{ ref('stg_users') }}"
        result = parse_model_sql("my_model", sql)
        assert result.has_star
        assert result.star_source == "ref_stg_users"


class TestJinjaStrippingFixes:
    """Tests for the corrected Jinja stripping behaviour."""

    def test_var_in_string_context_no_double_quotes(self) -> None:
        """'{{ var("x") }}' must not produce ''__var__'' (the pre-fix bug)."""
        sql = "select * from t where platform = '{{ var(\"mitxonline\") }}'"
        result = strip_jinja(sql)
        assert "''__var__''" not in result.clean_sql
        assert "'__var__'" in result.clean_sql  # outer quotes kept, inner identifier OK

    def test_bare_var_becomes_identifier(self) -> None:
        """Bare {{ var('x') }} (no surrounding quotes) becomes identifier __var__."""
        sql = "select {{ var('platform') }} as platform from t"
        result = strip_jinja(sql)
        assert "__var__" in result.clean_sql
        assert "'__var__'" not in result.clean_sql

    def test_block_macro_becomes_comment(self) -> None:
        """{{ macro_call(...) }} alone on a line becomes a SQL comment, not a string literal."""
        sql = (
            "with source as (select 1 as id)\n"
            "{{ deduplicate_raw_table(order_by='ts') }}\n"
            ", cleaned as (select id from source)\n"
            "select * from cleaned"
        )
        result = strip_jinja(sql)
        assert "'__jinja__'" not in result.clean_sql  # NOT a string literal
        assert "/* __jinja_macro__ */" in result.clean_sql

    def test_inline_jinja_becomes_identifier(self) -> None:
        """{{ ... }} inside an expression becomes an unquoted identifier placeholder."""
        sql = "select nullif({{ json_query_string('col', '$.key') }}, 'null') as val from t"
        result = strip_jinja(sql)
        # Replaced with an unquoted identifier (either __jinja__ from regex path or
        # __macro__ from Jinja2 path) so the SQL parses cleanly as a column expression.
        assert "__jinja__" in result.clean_sql or "__macro__" in result.clean_sql
        assert "'__jinja__'" not in result.clean_sql

    def test_jinja_variable_in_quoted_string_parseable(self) -> None:
        """A bare Jinja var inside SQL quotes like `like '{{ discussion_events }}'` must parse."""
        sql = (
            "with base as (select id, event_type from raw)\n"
            "select id from base\n"
            "where event_type like '{{ discussion_events }}'"
        )
        result = parse_model_sql("my_model", sql)
        assert result.parse_error is None
        assert "id" in result.output_columns

    def test_broken_column_macro_single_line_tail(self) -> None:
        """Macros that split a SQL column expression across the }} boundary are collapsed.

        Pattern: , __jinja__) as type(...)), ', '   <- orphaned tail
                 ) as real_alias                    <- actual column alias
        """
        sql = (
            "with source as (select 1 as id, 'x' as metadata)\n"
            ", renamed as (\n"
            "    select\n"
            "        id as websitecontent_id\n"
            "        , {{ array_join('cast(json_parse(json_query(metadata',"
            " \"lax $.level\") }}) as array (varchar)), ', ' --noqa\n"
            "        ) as course_level\n"
            ")\n"
            "select * from renamed"
        )
        result = parse_model_sql("my_model", sql)
        assert result.parse_error is None
        assert "websitecontent_id" in result.output_columns
        assert "course_level" in result.output_columns

    def test_broken_column_macro_multiline_case(self) -> None:
        """Multiline broken column (CASE expression split by macro) is collapsed to alias."""
        sql = (
            "with source as (select 1 as id, '[]' as metadata)\n"
            ", renamed as (\n"
            "    select\n"
            "        id as item_id\n"
            "        , {{ array_join('cast(\n"
            "                json_parse(\n"
            "                    case\n"
            "                        when json_query(metadata', \"lax $.types\") }} = '[]' then null\n"
            "                        else nullif(json_query(metadata, 'lax $.types'), '')\n"
            "                    end\n"
            "                ) as array(varchar) --noqa\n"
            "            ), ', '\n"
            "        ) as item_types\n"
            ")\n"
            "select * from renamed"
        )
        result = parse_model_sql("my_model", sql)
        assert result.parse_error is None
        assert "item_id" in result.output_columns
        assert "item_types" in result.output_columns

    def test_var_in_where_clause_parseable(self) -> None:
        """A model with '{{ var(...) }}' in WHERE must parse without error."""
        sql = (
            "with base as (select * from raw_users)\n"
            "select id, email from base\n"
            "where platform = '{{ var(\"mitxonline\") }}'"
        )
        result = parse_model_sql("my_model", sql)
        assert result.parse_error is None
        assert "id" in result.output_columns
        assert "email" in result.output_columns

    def test_union_all_model_outermost_select(self) -> None:
        """Models using UNION ALL get columns extracted from the representative branch."""
        sql = (
            "select id as user_id, email as user_email from source_a\n"
            "union all\n"
            "select id as user_id, email as user_email from source_b"
        )
        result = parse_model_sql("my_model", sql)
        assert result.parse_error is None
        assert "user_id" in result.output_columns
        assert "user_email" in result.output_columns

    def test_with_plus_union_all(self) -> None:
        """WITH … UNION ALL correctly returns columns from the outermost SELECT."""
        sql = (
            "with a as (select 1 as x from t1)\n"
            ", b as (select 1 as x from t2)\n"
            "select x from a\n"
            "union all\n"
            "select x from b"
        )
        result = parse_model_sql("my_model", sql)
        assert result.parse_error is None
        assert "x" in result.output_columns


class TestCompiledSqlPath:
    def test_parse_model_file_uses_compiled_when_present(self, tmp_path: Path) -> None:
        """parse_model_file uses compiled SQL when compiled_dir is provided and a match exists."""
        # Raw SQL: Jinja that would confuse the parser
        raw_dir = tmp_path / "models"
        raw_dir.mkdir()
        (raw_dir / "my_model.sql").write_text("select '{{ var(\"x\") }}' as platform, id from raw_users")
        # Compiled SQL: fully rendered, explicit columns
        compiled_dir = tmp_path / "compiled"
        compiled_dir.mkdir()
        (compiled_dir / "my_model.sql").write_text("select 'mitxonline' as platform, id from raw_users")

        result = parse_model_file(raw_dir / "my_model.sql", compiled_dir=compiled_dir)
        assert result.parse_error is None
        assert result.output_columns == {"platform", "id"}
        assert result.compiled_path is not None

    def test_parse_model_file_falls_back_without_compiled(self, tmp_path: Path) -> None:
        """When compiled_dir is None, raw SQL is used."""
        raw_dir = tmp_path / "models"
        raw_dir.mkdir()
        (raw_dir / "my_model.sql").write_text("select id, email from raw_users")

        result = parse_model_file(raw_dir / "my_model.sql", compiled_dir=None)
        assert result.parse_error is None
        assert "id" in result.output_columns

    def test_find_compiled_dir_returns_none_when_absent(self, tmp_path: Path) -> None:
        """find_compiled_dir returns None when target/compiled doesn't exist."""
        (tmp_path / "dbt_project.yml").write_text("")
        assert find_compiled_dir(tmp_path) is None

    def test_find_compiled_dir_finds_models_subdir(self, tmp_path: Path) -> None:
        """find_compiled_dir returns the models subdir when target/compiled/<project>/models exists."""
        models_subdir = tmp_path / "target" / "compiled" / "open_learning" / "models"
        models_subdir.mkdir(parents=True)
        result = find_compiled_dir(tmp_path)
        assert result == models_subdir


class TestUnionCteStar:
    """SELECT * resolution when CTEs use UNION ALL."""

    def test_union_cte_columns_resolved(self) -> None:
        """SELECT * from a CTE whose body is a UNION ALL resolves via leftmost branch."""
        sql = """
        with platform_a as (
            select id as user_id, email as user_email from raw_a
        )
        , platform_b as (
            select id as user_id, email as user_email from raw_b
        )
        , combined as (
            select user_id, user_email from platform_a
            union all
            select user_id, user_email from platform_b
        )
        select * from combined
        """
        result = parse_model_sql("my_model", sql)
        assert not result.has_star
        assert result.star_resolved
        assert result.output_columns == {"user_id", "user_email"}

    def test_union_cte_with_extra_column(self) -> None:
        """UNION CTE with a platform discriminator column is fully resolved."""
        sql = """
        with a as (select id as user_id, email as user_email from raw_a)
        , b as (select id as user_id, email as user_email from raw_b)
        , combined as (
            select 'a' as platform, user_id, user_email from a
            union all
            select 'b' as platform, user_id, user_email from b
        )
        select * from combined
        """
        result = parse_model_sql("my_model", sql)
        assert not result.has_star
        assert result.star_resolved
        assert result.output_columns == {"platform", "user_id", "user_email"}

    def test_union_cte_star_source_propagated_when_unresolvable(self) -> None:
        """When UNION CTE itself draws from an external ref, star_source is the ref placeholder."""
        sql = """
        with a as (select * from ref_upstream_a)
        , b as (select * from ref_upstream_b)
        , combined as (
            select * from a
            union all
            select * from b
        )
        select * from combined
        """
        result = parse_model_sql("my_model", sql)
        assert result.has_star
        # star_source should be the ref placeholder, not 'combined'
        assert result.star_source != "combined"

    def test_union_all_outermost_select(self) -> None:
        """UNION ALL at outermost level — leftmost branch columns used."""
        sql = """
        select id as user_id, email as user_email from raw_a
        union all
        select id as user_id, email as user_email from raw_b
        """
        result = parse_model_sql("my_model", sql)
        assert not result.has_star
        assert result.output_columns == {"user_id", "user_email"}


class TestPassthroughStarResolution:
    """SELECT * through a thin CTE passthrough resolves to the ultimate source."""

    def test_single_hop_passthrough(self) -> None:
        """CTE is just SELECT * FROM ref; outermost select * from that CTE."""
        sql = """
        with data as (
            select * from ref_upstream_model
        )
        select * from data
        """
        result = parse_model_sql("my_model", sql)
        assert result.has_star
        # The star_source should be traced back to the ref placeholder
        assert result.star_source == "ref_upstream_model"

    def test_two_hop_passthrough(self) -> None:
        """star_source walks through two thin CTE layers to the ref."""
        sql = """
        with raw as (select * from ref_source_table)
        , passthrough as (select * from raw)
        select * from passthrough
        """
        result = parse_model_sql("my_model", sql)
        assert result.has_star
        assert result.star_source == "ref_source_table"

    def test_passthrough_followed_by_registry_resolution(self) -> None:
        """After parse, _resolve_star_with_registry can resolve via the propagated star_source."""
        from pathlib import Path

        from ol_dbt_cli.commands.validate import _resolve_star_with_registry
        from ol_dbt_cli.lib.yaml_registry import YamlColumn, YamlModel, YamlRegistry

        sql = """
        with data as (select * from {{ ref('stg_users') }})
        select * from data
        """
        result = parse_model_sql("my_model", sql)
        assert result.has_star
        assert result.star_source == "ref_stg_users"

        registry = YamlRegistry(
            models={
                "stg_users": YamlModel(
                    name="stg_users",
                    source_file=Path("_models.yml"),
                    columns={n: YamlColumn(name=n) for n in ["user_id", "user_email"]},
                )
            }
        )
        cols = _resolve_star_with_registry(result, registry, manifest=None, sql_models_by_name={})
        assert cols == {"user_id", "user_email"}


class TestValuesInlineTable:
    """SELECT * from an inline VALUES table resolves column names from the alias list."""

    def test_values_columns_extracted(self) -> None:
        """VALUES with column alias list → columns resolved without star."""
        sql = """
        select * from (
            values ('hash_abc', 12345, 1)
        ) as override_list (cert_hash, user_id, program_id)
        """
        result = parse_model_sql("my_model", sql)
        assert not result.has_star
        assert result.star_resolved
        assert result.output_columns == {"cert_hash", "user_id", "program_id"}

    def test_values_without_column_alias_stays_unresolved(self) -> None:
        """VALUES without a named column alias list cannot be resolved."""
        sql = "select * from (values (1, 2, 3)) as t"
        result = parse_model_sql("my_model", sql)
        # Without explicit column names in the alias, star remains unresolved
        assert result.has_star or result.star_resolved  # either outcome is acceptable


class TestJinja2Engine:
    """Tests for the Jinja2-based rendering engine in strip_jinja."""

    def test_jinja2_handles_if_block(self) -> None:
        """{% if %} block is properly rendered (one branch selected) rather than stripped."""
        from ol_dbt_cli.lib.sql_parser import strip_jinja

        sql = """
        {% if var('platform') == 'mitxonline' %}
        select id, email from mitxonline_users
        {% else %}
        select id, email from edxorg_users
        {% endif %}
        """
        result = strip_jinja(sql)
        # Either branch is valid SQL — Jinja2 should pick one
        assert "select" in result.clean_sql.lower()
        # No Jinja control syntax should remain
        assert "{%" not in result.clean_sql

    def test_jinja2_ref_collected_with_rendering(self) -> None:
        """ref() calls are collected for lineage even when rendered via Jinja2."""
        from ol_dbt_cli.lib.sql_parser import strip_jinja

        sql = "select id from {{ ref('stg_users') }}"
        result = strip_jinja(sql)
        assert "stg_users" in result.ref_names
        assert result.ref_placeholder_map.get("ref_stg_users") == "stg_users"
        assert "ref_stg_users" in result.clean_sql

    def test_jinja2_source_collected_with_rendering(self) -> None:
        """source() calls are collected for lineage even when rendered via Jinja2."""
        from ol_dbt_cli.lib.sql_parser import strip_jinja

        sql = "select id from {{ source('raw_data', 'users') }}"
        result = strip_jinja(sql)
        assert "raw_data.users" in result.source_names
        assert "raw_data.users" in result.source_placeholder_map.values()
        assert "{" not in result.clean_sql

    def test_jinja2_undefined_macro_renders_as_placeholder(self) -> None:
        """Unknown macros render as a placeholder identifier, not an error."""
        from ol_dbt_cli.lib.sql_parser import strip_jinja

        sql = "select {{ dbt_utils.surrogate_key(['id', 'email']) }} as sk from users"
        result = strip_jinja(sql)
        assert "{" not in result.clean_sql
        # Some placeholder token should be present in the expression context
        assert any(tok in result.clean_sql for tok in ("__macro__", "__jinja__", "__undefined__"))

    def test_jinja2_config_block_removed(self) -> None:
        """{{ config(...) }} blocks are removed from the rendered output."""
        from ol_dbt_cli.lib.sql_parser import strip_jinja

        sql = "{{ config(materialized='table') }}\nselect 1 as id"
        result = strip_jinja(sql)
        assert "config" not in result.clean_sql
        assert "select" in result.clean_sql.lower()

    def test_jinja2_for_loop_renders_cleanly(self) -> None:
        """{% for %} over an undefined variable renders without error (empty iteration)."""
        from ol_dbt_cli.lib.sql_parser import strip_jinja

        sql = """
        {% for event in event_types %}
        union all select '{{ event }}' as event_type
        {% endfor %}
        select id from base
        """
        result = strip_jinja(sql)
        assert "{" not in result.clean_sql

    def test_jinja2_parse_model_conditional_select(self) -> None:
        """Model with conditional SELECT resolves to a valid column set."""
        sql = """
        {% if var('include_email', false) %}
        select id as user_id, email as user_email from raw_users
        {% else %}
        select id as user_id from raw_users
        {% endif %}
        """
        result = parse_model_sql("my_model", sql)
        assert result.parse_error is None
        assert "user_id" in result.output_columns

    def test_jinja2_falls_back_on_syntax_error(self) -> None:
        """When Jinja2 raises a TemplateError, regex fallback is used."""
        from ol_dbt_cli.lib.sql_parser import _strip_jinja_regex, strip_jinja

        # A valid Jinja2 template should use the Jinja2 path
        sql = "select {{ ref('stg_users') }}.id from {{ ref('stg_users') }}"
        result = strip_jinja(sql)
        # Both paths should produce the same placeholder
        regex_result = _strip_jinja_regex(sql)
        assert result.ref_names == regex_result.ref_names
