"""Generated rows for tables that reference their own table.

factory_boy builds one row at a time and has no notion of a parent in the same
table. Two shapes need one:

- A django-treebeard materialized-path tree (``MP_Node``: wagtail's
  ``wagtailcore_page``, MITx Online's ``courses_programrequirement``). A row's
  place is its ``path``, ``steplen`` characters per level, and ``depth`` and the
  parent's ``numchild`` follow from it. :class:`Tree` places generated rows.
- A ``parent_id`` foreign key to the same table (the course topic tables).
  :func:`attach` draws each row's parent from rows built before it.

A unit's ``generate`` builds the rows with its factories and passes them here::

    pages = Tree(hand_written[Page.raw_table])
    courses_index = {"path": "000100010001", "url_path": "/home/courses/"}
    course_pages = pages.add(Page.build_batch(scaled_count(Page, scale)), under=courses_index)

A :class:`Tree` sees only the rows it was built from and placed, so
:func:`~ol_dbt_cli.lib.fixture_generation.generate_rows` checks each tree whole
with :func:`check_tree`, against the stand-ins the package declares.
"""

from __future__ import annotations

from collections import Counter
from typing import TYPE_CHECKING

from ol_dbt_cli.lib.fixture_generation import pick

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from ol_dbt_cli.lib.fixture_generation import Row

# django-treebeard's `MP_Node.alphabet`, which wagtail's Page keeps. A model can
# set its own (MITx Online's ProgramRequirement adds the lowercase letters).
TREEBEARD_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
# `MP_Node.steplen`. No model in the fixture units overrides it.
TREEBEARD_STEPLEN = 4


class Tree:
    """One table's treebeard tree, for placing generated rows in it.

    Built from the rows the table already holds, so a placed row never takes a
    path one of them has. Placing a row under a parent adds one to the parent's
    ``numchild``. A parent that is a hand-written row is changed in the
    ``hand_written`` rows ``generate`` was given, which is what loads.

    A parent the fixture does not hold (wagtail's root and index pages are
    left out of the committed fixtures) is given as a stand-in mapping with its
    ``path`` and, for pages, its ``url_path``. A stand-in for a path the tree
    holds is that row. The package declares its stand-ins in
    ``TREE_STAND_INS``, which :func:`check_tree` holds the placed rows to.

    :param rows: Every row the table holds so far, hand-written and generated.
    :param alphabet: The model's ``MP_Node.alphabet``.
    :raises ValueError: If a row's ``path`` is not made of ``alphabet`` in
        whole steps, or two rows have one path.
    """

    def __init__(self, rows: Iterable[Row], *, alphabet: str = TREEBEARD_ALPHABET) -> None:
        self._alphabet = alphabet
        self._rows: dict[str, Row] = {}
        self._url_paths: set[str] = set()
        # The step of the last child under each parent path, "" for the roots.
        self._last_step: dict[str, int] = {}
        for row in rows:
            self._hold(row)

    def add(self, children: Sequence[Row], under: Row | None = None) -> list[Row]:
        """Place ``children`` under ``under``, after its existing children, as ``add_child`` does.

        Sets each child's ``path``, ``depth`` and ``numchild`` (0). A child
        with a ``slug`` under a parent with a ``url_path`` gets wagtail's
        ``url_path``: the parent's, the slug and a slash. A root with a
        ``slug`` gets ``/``, as wagtail's root page has.

        :param children: Rows built by the table's factory.
        :param under: The parent row, a stand-in for one the fixture does not
            hold, or ``None`` to place the children as roots.
        :returns: ``children``, placed.
        :rtype: list[Row]
        :raises ValueError: If the parent has no room for another child at
            this step length, or a child's ``url_path`` is another row's.
        """
        parent = None if under is None else self._rows.get(under["path"], under)
        parent_path = "" if parent is None else parent["path"]
        for child in children:
            child["path"] = parent_path + self._step(self._last_step.get(parent_path, 0) + 1, parent_path)
            child["depth"] = len(child["path"]) // TREEBEARD_STEPLEN
            child["numchild"] = 0
            if child.get("slug") is not None:
                if parent is None:
                    child["url_path"] = "/"
                elif parent.get("url_path") is not None:
                    child["url_path"] = f"{parent['url_path']}{child['slug']}/"
            self._hold(child)
        if parent is not None:
            # A hand-written count can be a quoted "0", which a long column accepts.
            parent["numchild"] = int(parent.get("numchild") or 0) + len(children)
        return list(children)

    def _step(self, number: int, parent_path: str) -> str:
        base = len(self._alphabet)
        if number >= base**TREEBEARD_STEPLEN:
            msg = f"no room for another child under path {parent_path!r}: {number - 1} is the most a step holds"
            raise ValueError(msg)
        digits = ""
        while number:
            number, digit = divmod(number, base)
            digits = self._alphabet[digit] + digits
        return digits.rjust(TREEBEARD_STEPLEN, self._alphabet[0])

    def _hold(self, row: Row) -> None:
        path = row["path"]
        _check_path(path, self._alphabet)
        if path in self._rows:
            msg = f"two rows have path {path!r}"
            raise ValueError(msg)
        url_path = row.get("url_path")
        if url_path is not None and url_path in self._url_paths:
            msg = f"two rows have url_path {url_path!r}; a slug repeats under one parent"
            raise ValueError(msg)
        self._rows[path] = row
        if url_path is not None:
            self._url_paths.add(url_path)
        parent_path, step = path[:-TREEBEARD_STEPLEN], path[-TREEBEARD_STEPLEN:]
        number = 0
        for character in step:
            number = number * len(self._alphabet) + self._alphabet.index(character)
        self._last_step[parent_path] = max(self._last_step.get(parent_path, 0), number)


def check_tree(
    raw_table: str,
    hand_written: Sequence[Row],
    generated: Sequence[Row],
    *,
    key_columns: Sequence[str],
    alphabet: str = TREEBEARD_ALPHABET,
    stand_ins: Sequence[Row] = (),
) -> None:
    """Check that every row a treebeard table loads, with the parents it leaves out, makes one tree.

    Two trees built over one table, or rows placed without one, can give two
    rows one path, and a mistyped stand-in places rows under a parent nothing
    has. ``validate_generated`` checks only primary keys, so it sees neither.
    A hand-written parent's ``numchild`` counts children the fixture leaves
    out, so only a generated row's is checked.

    :param raw_table: The table, for the messages.
    :param hand_written: The fixture's rows, as ``generate`` left them.
    :param generated: The rows the package generated.
    :param key_columns: The table's primary key, which names a row in the messages.
    :param alphabet: The model's ``MP_Node.alphabet``.
    :param stand_ins: The parents the fixture leaves out, the package's
        ``TREE_STAND_INS`` for the table.
    :raises ValueError: If a path is not whole steps of ``alphabet``, two rows
        have one path, a stand-in has a row's path, a row's or stand-in's
        parent is neither a row nor a stand-in, a row's ``url_path`` is not
        under its stand-in parent's, a row's ``depth`` is not its path's, or a
        generated row's ``numchild`` is not its children's count.
    """
    held: dict[str, Row] = {}
    for row in [*hand_written, *generated]:
        path = row["path"]
        _check_path(path, alphabet)
        if path in held:
            msg = (
                f"{raw_table}: {_named(held[path], key_columns)} and {_named(row, key_columns)} both have path {path!r}"
            )
            raise ValueError(msg)
        held[path] = row
        # A long column accepts a quoted depth, and the fixture keeps it as written.
        if int(row["depth"]) != len(path) // TREEBEARD_STEPLEN:
            msg = f"{raw_table}: {_named(row, key_columns)} has depth {row['depth']!r} at path {path!r}"
            raise ValueError(msg)
    left_out: dict[str, Row] = {}
    for stand_in in stand_ins:
        path = stand_in["path"]
        _check_path(path, alphabet)
        if path in held:
            msg = f"{raw_table}: stand-in {path!r} is {_named(held[path], key_columns)}, which the fixture holds"
            raise ValueError(msg)
        left_out[path] = stand_in
    for path in [*held, *sorted(left_out)]:
        parent_path = path[:-TREEBEARD_STEPLEN]
        if parent_path and parent_path not in held and parent_path not in left_out:
            what = _named(held[path], key_columns) if path in held else "stand-in"
            msg = f"{raw_table}: {what} at path {path!r} is under {parent_path!r}, which no row or stand-in has"
            raise ValueError(msg)
        # A page under a stand-in takes its url_path from it, so a mistyped one shows here.
        parent_url_path = left_out[parent_path].get("url_path") if parent_path in left_out else None
        url_path = held[path].get("url_path") if path in held else None
        if parent_url_path is not None and url_path is not None and not url_path.startswith(parent_url_path):
            msg = (
                f"{raw_table}: {_named(held[path], key_columns)} has url_path {url_path!r}, "
                f"not under its stand-in parent's {parent_url_path!r}"
            )
            raise ValueError(msg)
    children = Counter(path[:-TREEBEARD_STEPLEN] for path in held)
    for row in generated:
        if row["numchild"] != children[row["path"]]:
            msg = (
                f"{raw_table}: {_named(row, key_columns)} has numchild {row['numchild']!r} "
                f"and {children[row['path']]} children"
            )
            raise ValueError(msg)


def _check_path(path: str, alphabet: str) -> None:
    if not path or len(path) % TREEBEARD_STEPLEN or set(path) - set(alphabet):
        msg = f"path {path!r} is not whole steps of {TREEBEARD_STEPLEN} characters from {alphabet!r}"
        raise ValueError(msg)


def _named(row: Row, key_columns: Sequence[str]) -> str:
    return "the row with " + ", ".join(f"{column}={row[column]!r}" for column in key_columns)


def attach(children: Sequence[Row], parents: Sequence[Row], *, column: str = "parent_id", key: str = "id") -> list[Row]:
    """Point each of ``children`` at a parent row of the same table, drawn with the seeded RNG.

    Build a hierarchy a level at a time: the top level with no parent, then
    each level attached to rows already built (hand-written or generated), so
    no row points at a row after it or at none.

    :param children: Rows built by the table's factory.
    :param parents: Rows to draw from, repeats allowed.
    :param column: The foreign key column.
    :param key: The column it references.
    :returns: ``children``, attached.
    :rtype: list[Row]
    :raises ValueError: If there are children and no parents.
    """
    if children and not parents:
        msg = f"cannot attach {len(children)} rows to no parents"
        raise ValueError(msg)
    for child, chosen in zip(children, pick(parents, len(children)), strict=True):
        child[column] = chosen[key]
    return list(children)
