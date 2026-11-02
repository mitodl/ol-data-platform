"""Factories that generate raw-table rows for the local lake.

A unit with a fixture file (``ingestion/inventory/fixtures/<unit>.yml``) can
also have a package here of the same name, e.g. ``xpro__app_postgres/``. The
fixture file still declares every table's columns and types and holds the
hand-written rows. The package adds generated rows to those tables.

A unit package has two modules:

``_generated.py``
    Base factories scaffolded from the source application's Django models by
    ``ol-dbt fixtures scaffold`` (:mod:`ol_dbt_cli.lib.fixture_scaffold`),
    from the ``models.json`` beside it, or for a unit with no Django app
    drafted from its staging models' tests. Regenerated wholesale, so never
    edited by hand.

``__init__.py``
    Written by hand. Subclasses the base factories for what the models cannot
    say (JSON shapes, readable id formats, date ordering) and wires the tables
    together in ``generate``.

and exposes what :class:`ol_dbt_cli.lib.raw_fixtures.UnitFactories` describes:

``SEED``
    The committed seed. The caller seeds with it before calling ``generate``,
    so the same commit generates the same rows. ``--seed`` on ``ol-dbt
    fixtures load`` and ``generate`` replaces it for one run.

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
``numchild``) and ``attach`` for a ``parent_id`` column. A unit with a
treebeard table also exposes:

``TREE_ALPHABETS``
    Imported from ``_generated.py``, where the scaffold writes one for each
    table whose model the dump records as an ``MP_Node``. ``generate_rows``
    checks each of these tables whole with ``check_tree``: one row to a path,
    and every row under a row of the table or a stand-in.

``TREE_STAND_INS``
    The parents a tree's rows sit under that the fixture leaves out (wagtail's
    root, home and index pages), by raw table, each a stand-in mapping with its
    ``path``. A stand-in ``generate`` places rows under is one of these, so a
    mistyped path fails the check. A tree with nothing left out has no entry.

Each factory is a :class:`ol_dbt_cli.lib.fixture_generation.RowFactory` that
names the table it fills and how many rows it makes at scale 1 with
``@table(raw_table, default_count=...)``, which sets them as the class
attributes ``raw_table`` and ``default_count``. They cannot be written in the
class body: factory_boy would read them as columns. Default counts are tens of
rows per table, enough for the dbt tests to have something to fail on. Volume
comes from ``scale`` and nowhere else.

A table has one factory and its subclasses: ``@table`` refuses a second
factory of a table that does not subclass the first, whose keys would count
from the same start. Values come from ``value`` and ``draw``, which refuse a
mimesis provider that can make up a real hostname, IP address or phone number
or whose rows change with the date, and ``@table`` refuses a ``factory.Faker``
column, which neither sees. A column that needs an address takes one from a
reserved range by hand (e.g. ``192.0.2.0/24``, ``555-0100``). Not checked: a
subclass not passed through ``@table``, the factory a ``factory.SubFactory``
builds, and what a ``LazyFunction`` calls.

:mod:`ol_dbt_cli.lib.fixture_generation` holds what the packages share (seeded
values, parent columns, per-table sequences, the draws ``generate`` wires
tables with) and :func:`~ol_dbt_cli.lib.fixture_generation.generate_rows`,
which runs a package. It brings every value to its column's fixture type, fills
the loader's columns, and has
:func:`ol_dbt_cli.lib.raw_fixtures.validate_generated` check the rows against
the fixture file's columns and the inventory unit's primary keys. Foreign keys
are declared in neither. ``tests/test_fixture_factories.py`` checks every
package's rows against its ``models.json`` and pins them to a digest, so a
change to the rows a package generates fails CI until the digest is updated
with it.
"""
