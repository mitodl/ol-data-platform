"""Factories that generate raw-table rows for the local lake.

A unit with a fixture file (``ingestion/inventory/fixtures/<unit>.yml``) can
also have a package here of the same name, e.g. ``xpro__app_postgres/``. The
fixture file still declares every table's columns and types and holds the
hand-written rows. The package adds generated rows to those tables.

A unit package has two modules:

``_generated.py``
    Base factories scaffolded from the source application's Django models.
    Regenerated wholesale, so never edited by hand.

``__init__.py``
    Written by hand. Subclasses the base factories for what the models cannot
    say (JSON shapes, readable id formats, date ordering) and wires the tables
    together in ``generate``.

and exposes what :class:`ol_dbt_cli.lib.raw_fixtures.UnitFactories` describes:

``SEED``
    The committed seed. The caller seeds with it before calling ``generate``,
    so the same commit generates the same rows.

``generate(hand_written, scale)``
    Returns the generated rows keyed by raw table name. ``hand_written`` is the
    fixture file's rows under the same keys, there so a generated child can
    reference a hand-written parent. Which parents a child draws from,
    one-to-one pairing and tree placement are ordinary Python in this function.
    A column a row leaves out is NULL, as in a hand-written row.

    The hand-written rows are copies, and what ``generate`` leaves in them is
    what loads: placing a generated page under a hand-written one adds to the
    parent's ``numchild``. A hand-written row keeps its primary key, and
    generated rows are returned, not added to ``hand_written``.

Tables that reference themselves use :mod:`ol_dbt_cli.lib.fixture_trees`:
``Tree`` for a django-treebeard ``MP_Node`` (``path``, ``depth``,
``numchild``) and ``attach`` for a ``parent_id`` column.

Each factory is a :class:`ol_dbt_cli.lib.fixture_generation.RowFactory` that
names the table it fills and how many rows it makes at scale 1 with
``@table(raw_table, default_count=...)``, which sets them as the class
attributes ``raw_table`` and ``default_count``. They cannot be written in the
class body: factory_boy would read them as columns. Default counts are tens of
rows per table, enough for the dbt tests to have something to fail on. Volume
comes from ``scale`` and nowhere else.

:mod:`ol_dbt_cli.lib.fixture_generation` holds what the packages share (seeded
values, parent columns, per-table sequences, the draws ``generate`` wires
tables with) and :func:`~ol_dbt_cli.lib.fixture_generation.generate_rows`,
which runs a package. It brings every value to its column's fixture type, fills
the loader's columns, and has
:func:`ol_dbt_cli.lib.raw_fixtures.validate_generated` check the rows against
the fixture file's columns and the inventory unit's primary keys. Foreign keys
are declared in neither, so a unit checks its own references in its tests.
"""
