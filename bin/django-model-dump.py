#!/usr/bin/env python3
"""Write what a Django app's models say about their tables, as JSON.

`ol-dbt fixtures scaffold` reads the output. Run this with the app's own Python
from the app's checkout, so its settings and models import. It reads no rows
and opens no database connection: importing the models is all it does.
Standard library and Django only, because it runs in the app's environment,
not this repository's::

    cd ../mitxonline
    .venv/bin/python ../ol-data-platform/bin/django-model-dump.py \\
        --settings main.settings --commit "$(git rev-parse HEAD)" \\
        > "$FACTORIES/mitxonline__app_postgres/models.json"

where ``$FACTORIES`` is ``src/ol_dbt_cli/ol_dbt_cli/fixture_factories``.

An app whose settings require environment variables (a secret key, a database
URL) needs them set, and placeholders do: nothing connects. The tables are
sorted, so a second run on the same commit writes the same file.
"""

# `_meta` is Django's public model API, underscore and all.
# ruff: noqa: SLF001

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

FORMAT_VERSION = 1


def _jsonable(value: Any) -> Any:
    """Return ``value`` if JSON can hold it, else its ``str`` (a Decimal)."""
    try:
        json.dumps(value)
    except (TypeError, ValueError):
        return str(value)
    return value


def _column(field: Any) -> dict[str, Any]:
    from django.db import models  # noqa: PLC0415

    internal_type = field.get_internal_type()
    column: dict[str, Any] = {
        "field": type(field).__name__,
        "internal_type": internal_type,
        "null": field.null,
        "primary_key": field.primary_key,
        "unique": field.unique,
    }
    if field.max_length is not None:
        column["max_length"] = field.max_length
    if getattr(field, "max_digits", None) is not None:
        column["max_digits"], column["decimal_places"] = (
            field.max_digits,
            field.decimal_places,
        )
    if field.choices:
        column["choices"] = [_jsonable(value) for value, _ in field.flatchoices]
    if field.has_default() and not callable(field.default):
        column["default"] = _jsonable(field.default)
    if patterns := [
        validator.regex.pattern
        for validator in field.validators
        if hasattr(validator, "regex")
    ]:
        column["regex"] = patterns
    # StreamField reports JSONField from Wagtail 5 on, but say it either way.
    if internal_type == "JSONField" or type(field).__name__ == "StreamField":
        column["json"] = True
    if (field.is_relation and field.many_to_one) or field.one_to_one:
        target = field.target_field
        column["foreign_key"] = {
            "table": target.model._meta.db_table,
            "column": target.column,
        }
        if field.one_to_one:
            column["one_to_one"] = True
        if isinstance(field, models.OneToOneField) and field.remote_field.parent_link:
            column["parent_link"] = True
    return column


def _table(model: Any) -> dict[str, Any]:
    meta = model._meta
    # The table's own columns. A multi-table-inheritance child's concrete_fields
    # also hold its parents', which live in the parents' tables.
    # A constraint can name a foreign key by its attname (order_id) or its name.
    columns_by_name = {
        name: field.column
        for field in meta.local_concrete_fields
        for name in (field.name, field.attname)
    }
    unique_together = [
        [columns_by_name[name] for name in fields] for fields in meta.unique_together
    ]
    for constraint in meta.constraints:
        # A UniqueConstraint on expressions (Lower("email")) or with a condition
        # holds for only some rows, so only plain column sets are recorded.
        fields = getattr(constraint, "fields", ())
        if (
            fields
            and not getattr(constraint, "condition", None)
            and type(constraint).__name__ == "UniqueConstraint"
        ):
            unique_together.append([columns_by_name[name] for name in fields])
    table: dict[str, Any] = {
        "model": f"{meta.app_label}.{model.__name__}",
        "managed": meta.managed,
        "columns": {
            field.column: _column(field) for field in meta.local_concrete_fields
        },
    }
    if unique_together:
        table["unique_together"] = sorted(unique_together)
    # A page type subclasses MP_Node through wagtail's Page, but the tree's
    # path lives in wagtailcore_page, not in the page type's own table.
    if "path" in columns_by_name and any(
        cls.__name__ == "MP_Node" for cls in model.__mro__
    ):
        table["treebeard"] = {"alphabet": model.alphabet, "steplen": model.steplen}
    generic = [
        {
            "name": field.name,
            "content_type": columns_by_name[field.ct_field],
            "object_id": columns_by_name[field.fk_field],
        }
        for field in meta.private_fields
        if type(field).__name__ == "GenericForeignKey"
        and field.ct_field in columns_by_name
        and field.fk_field in columns_by_name
    ]
    if generic:
        table["generic_foreign_keys"] = sorted(generic, key=lambda entry: entry["name"])
    return table


def dump(app: str, commit: str | None) -> dict[str, Any]:
    """Return the tables of every installed model, keyed by ``db_table``.

    :param app: A name for the app, recorded in the output.
    :param commit: The app's commit, recorded so a reader knows what was dumped.
    :returns: The JSON document.
    :rtype: dict[str, Any]
    """
    import django  # noqa: PLC0415
    from django.apps import apps  # noqa: PLC0415

    django.setup()
    tables: dict[str, Any] = {}
    # Auto-created many-to-many tables too: a fixture can hold one.
    for model in apps.get_models(include_auto_created=True):
        if model._meta.proxy:
            continue
        tables[model._meta.db_table] = _table(model)
    return {
        "format_version": FORMAT_VERSION,
        "app": app,
        # Prefixed, because detect-secrets takes a bare 40-character hex
        # string for a secret and a JSON file has no line to allowlist it on.
        "commit": f"git:{commit}",
        "django": django.get_version(),
        "tables": dict(sorted(tables.items())),
    }


def main() -> None:
    """Parse the arguments and write the dump to stdout."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--settings",
        required=True,
        help="The app's settings module, e.g. main.settings.",
    )
    parser.add_argument(
        "--app-dir",
        type=Path,
        default=Path.cwd(),
        help="The app's checkout. Default: the current directory.",
    )
    parser.add_argument(
        "--app", help="A name for the app. Default: the checkout's directory name."
    )
    parser.add_argument(
        "--commit",
        required=True,
        help="The app's commit, e.g. $(git rev-parse HEAD).",
    )
    arguments = parser.parse_args()
    app_dir = arguments.app_dir.resolve()
    os.environ["DJANGO_SETTINGS_MODULE"] = arguments.settings
    sys.path.insert(0, str(app_dir))
    # Settings read files relative to the checkout (.env, templates).
    os.chdir(app_dir)
    document = dump(arguments.app or app_dir.name, arguments.commit)
    sys.stdout.write(json.dumps(document, indent=2, sort_keys=False) + "\n")


if __name__ == "__main__":
    main()
