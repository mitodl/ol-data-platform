"""Tests for the helpers that place generated rows in a table that references itself."""

from __future__ import annotations

import copy
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from ol_dbt_cli.lib.fixture_generation import pick, seed_generation
from ol_dbt_cli.lib.fixture_trees import TREEBEARD_STEPLEN, Tree, attach, check_tree
from ol_dbt_cli.lib.inventory import DEFAULT_INVENTORY_DIR, load_units
from ol_dbt_cli.lib.raw_fixtures import load_fixtures

if TYPE_CHECKING:
    from collections.abc import Collection

REPO_ROOT = Path(__file__).resolve().parents[3]
XPRO_PAGES = "raw__xpro__app__postgres__wagtailcore_page"
# The pages the committed xPRO fixture leaves out: the root, home and the two index pages.
XPRO_STAND_INS = [
    {"path": path, "url_path": url_path}
    for path, url_path in [
        ("0001", "/"),
        ("00010001", "/home/"),
        ("000100010001", "/home/courses/"),
        ("000100010002", "/home/programs/"),
    ]
]
COURSES_INDEX = {"path": "000100010001", "url_path": "/home/courses/"}
# MITx Online's ProgramRequirement.alphabet.
BASE62 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

type Row = dict[str, Any]


@pytest.fixture(autouse=True)
def _seeded() -> None:
    seed_generation(20261015)


def _committed_rows(raw_table: str) -> list[Row]:
    inventory_dir = REPO_ROOT / DEFAULT_INVENTORY_DIR
    for fixture in load_fixtures(inventory_dir, load_units(inventory_dir)):
        for table in fixture.tables:
            if table.raw_table == raw_table:
                return copy.deepcopy(table.rows)
    raise AssertionError(raw_table)


def _by_id(rows: list[Row], row_id: int) -> Row:
    (row,) = (row for row in rows if row["id"] == row_id)
    return row


def _assert_a_tree(rows: list[Row], absent: Collection[str] = ()) -> None:
    """Hold ``rows`` to what treebeard's ``find_problems`` checks, ``absent`` being paths the rows leave out."""
    paths = [row["path"] for row in rows]
    assert len(paths) == len(set(paths))
    for row in rows:
        parent_path = row["path"][:-TREEBEARD_STEPLEN]
        assert not parent_path or parent_path in paths or parent_path in absent
        assert row["depth"] == len(row["path"]) // TREEBEARD_STEPLEN
        children = [path for path in paths if path[:-TREEBEARD_STEPLEN] == row["path"]]
        assert row["numchild"] == len(children)


def test_children_of_a_hand_written_page_follow_its_last_child() -> None:
    pages = _committed_rows(XPRO_PAGES)
    course = _by_id(pages, 10)
    tree = Tree(pages)

    placed = tree.add([{"id": 100, "slug": "about"}, {"id": 101, "slug": "outcomes"}], under=course)

    assert [row["path"] for row in placed] == ["00010001000100010003", "00010001000100010004"]
    assert [row["depth"] for row in placed] == [5, 5]
    assert [row["url_path"] for row in placed] == [
        "/home/courses/local-fixtures-i/about/",
        "/home/courses/local-fixtures-i/outcomes/",
    ]
    assert course["numchild"] == 4
    _assert_a_tree(pages + placed, absent={"0001", "00010001", "000100010001", "000100010002"})


def test_pages_under_an_index_page_the_fixture_leaves_out() -> None:
    pages = _committed_rows(XPRO_PAGES)
    tree = Tree(pages)
    courses_index = {"path": "000100010001", "url_path": "/home/courses/"}

    placed = tree.add([{"id": 100, "slug": "generated-course"}], under=courses_index)

    # After the four hand-written course pages, 0001 to 0004.
    assert placed[0]["path"] == "0001000100010005"
    assert placed[0]["url_path"] == "/home/courses/generated-course/"
    assert placed[0]["depth"] == 4


def test_a_stand_in_for_a_held_page_is_that_page() -> None:
    pages = _committed_rows(XPRO_PAGES)
    course = _by_id(pages, 10)

    (placed,) = Tree(pages).add([{"id": 100, "slug": "about"}], under={"path": course["path"]})

    assert course["numchild"] == 3
    assert placed["url_path"] == "/home/courses/local-fixtures-i/about/"


def test_a_quoted_child_count_is_counted() -> None:
    # A long column accepts a quoted "1", and the fixture keeps it as written.
    root = {"path": "0001", "depth": 1, "numchild": "1"}
    child = {"path": "00010001", "depth": 2, "numchild": 0}

    Tree([root, child]).add([{}], under=root)

    assert root["numchild"] == 2


def test_a_root_page_has_the_root_url_path() -> None:
    (root,) = Tree([]).add([{"id": 1, "slug": "root"}])

    assert (root["path"], root["url_path"]) == ("0001", "/")


def test_a_generated_tree_holds_together_at_every_level() -> None:
    pages = _committed_rows(XPRO_PAGES)
    tree = Tree(pages)
    absent = {"0001", "00010001", "000100010001", "000100010002"}
    levels: list[list[Row]] = [
        tree.add([{"id": 100 + n, "slug": f"course-{n}"} for n in range(12)], under={"path": "000100010001"})
    ]
    for depth in range(3):
        parents = levels[-1]
        level: list[Row] = []
        for chosen in pick(parents, 15):
            level += tree.add([{"id": 1000 * (depth + 1) + len(level), "slug": f"p{depth}-{len(level)}"}], under=chosen)
        levels.append(level)

    _assert_a_tree(pages + [row for level in levels for row in level], absent=absent)


def test_a_requirement_tree_per_generated_program() -> None:
    requirements = _committed_rows("raw__mitxonline__app__postgres__courses_programrequirement")
    tree = Tree(requirements, alphabet=BASE62)
    programs, courses = [{"id": 4}, {"id": 5}], [{"id": 10 + n} for n in range(6)]

    generated: list[Row] = []
    for program in programs:
        (root,) = tree.add([{"program_id": program["id"], "node_type": "program_root"}])
        (required,) = tree.add(
            [{"program_id": program["id"], "node_type": "operator", "operator": "all_of"}], under=root
        )
        nodes = tree.add(
            [{"program_id": program["id"], "node_type": "course", "course_id": course["id"]} for course in courses[:3]],
            under=required,
        )
        generated += [root, required, *nodes]

    # One root per program, after the two hand-written programs' trees.
    assert [row["path"] for row in generated if row["node_type"] == "program_root"] == ["0003", "0004"]
    assert [row["depth"] for row in generated[:5]] == [1, 2, 3, 3, 3]
    _assert_a_tree(requirements + generated)


def test_roots_follow_the_existing_roots() -> None:
    requirements = [
        {"path": "0001", "depth": 1, "numchild": 0},
        {"path": "0002", "depth": 1, "numchild": 0},
    ]

    (root,) = Tree(requirements, alphabet=BASE62).add([{"node_type": "program_root"}])

    assert root["path"] == "0003"
    assert root["depth"] == 1


@pytest.mark.parametrize(
    ("alphabet", "tenth", "thirty_seventh"),
    [
        # django-treebeard's default, which wagtail's Page keeps.
        ("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ", "000A", "0011"),
        (BASE62, "000A", "000b"),
    ],
)
def test_steps_count_in_the_model_alphabet(alphabet: str, tenth: str, thirty_seventh: str) -> None:
    root = {"path": "0001", "depth": 1, "numchild": 0}

    children = Tree([root], alphabet=alphabet).add([{} for _ in range(37)], under=root)

    assert children[9]["path"] == f"0001{tenth}"
    assert children[36]["path"] == f"0001{thirty_seventh}"
    assert root["numchild"] == 37


def test_a_full_step_has_no_room_for_another_child() -> None:
    root = {"path": "0001", "depth": 1, "numchild": 0}
    tree = Tree([root], alphabet="01")
    # Four binary digits hold 1 to 15.
    tree.add([{} for _ in range(15)], under=root)

    with pytest.raises(ValueError, match="no room for another child under path '0001'"):
        tree.add([{}], under=root)


@pytest.mark.parametrize(
    ("rows", "message"),
    [
        ([{"path": "00010"}], "a row has path '00010', which is not whole steps of 4 characters"),
        ([{"path": "000a"}], "path '000a', which is not whole steps"),
        ([{"path": ""}], "path '', which is not whole steps"),
        ([{"path": "0001"}, {"path": "0001"}], "two rows have path '0001'"),
        (
            [{"path": "0001", "url_path": "/home/"}, {"path": "0002", "url_path": "/home/"}],
            "two rows have url_path '/home/'",
        ),
    ],
)
def test_rows_that_are_not_a_tree_are_refused(rows: list[Row], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        Tree(rows)


def test_a_slug_repeated_under_one_parent_is_refused() -> None:
    pages = _committed_rows(XPRO_PAGES)

    with pytest.raises(ValueError, match="two rows have url_path '/home/courses/local-fixtures-i/faculty/'"):
        Tree(pages).add([{"slug": "faculty"}], under=_by_id(pages, 10))


def test_placement_is_seeded() -> None:
    def placed() -> list[Row]:
        seed_generation(7)
        tree = Tree([])
        roots = tree.add([{"id": n} for n in range(3)])
        children = [row for n, chosen in enumerate(pick(roots, 10)) for row in tree.add([{"id": 10 + n}], under=chosen)]
        return roots + children

    assert placed() == placed()


def test_a_tree_and_its_stand_ins_pass_the_check() -> None:
    pages = _committed_rows(XPRO_PAGES)
    tree = Tree(pages)
    (course,) = tree.add([{"id": 100, "slug": "generated-course"}], under=dict(COURSES_INDEX))
    placed = [course, *tree.add([{"id": 101, "slug": "about"}], under=course)]

    check_tree(XPRO_PAGES, pages, placed, key_columns=["id"], stand_ins=XPRO_STAND_INS)


def test_two_trees_over_one_table_are_refused() -> None:
    pages = _committed_rows(XPRO_PAGES)
    # Each tree knows only the hand-written pages, so both place after them.
    first = Tree(pages).add([{"id": 100, "slug": "first"}], under=dict(COURSES_INDEX))
    second = Tree(pages).add([{"id": 101, "slug": "second"}], under=dict(COURSES_INDEX))

    with pytest.raises(
        ValueError,
        match=f"{XPRO_PAGES}: the row with id=100 and the row with id=101 both have path '0001000100010005'",
    ):
        check_tree(XPRO_PAGES, pages, first + second, key_columns=["id"], stand_ins=XPRO_STAND_INS)


def _page(path: str, **values: Any) -> Row:
    return {"id": 100, "path": path, "depth": len(path) // TREEBEARD_STEPLEN, "numchild": 0, **values}


@pytest.mark.parametrize(
    ("generated", "stand_ins", "message"),
    [
        # Placed without a tree, on a hand-written page's path.
        ([_page("0001000100010001")], XPRO_STAND_INS, "the row with id=10 and the row with id=100 both have path"),
        # A mistyped index page.
        (
            [_page("0001000100090001")],
            XPRO_STAND_INS,
            "the row with id=100 at path '0001000100090001' is under '000100010009', which no row or stand-in has",
        ),
        # The index pages declared, the home page above them not.
        (
            [],
            XPRO_STAND_INS[2:],
            "stand-in at path '000100010001' is under '00010001', which no row or stand-in has",
        ),
        (
            [],
            [*XPRO_STAND_INS, {"path": "0001000100010001"}],
            "stand-in '0001000100010001' is the row with id=10, which the fixture holds",
        ),
        # The courses index's url_path mistyped: its hand-written pages are not directly under it.
        *(
            (
                [],
                [*XPRO_STAND_INS[:2], {"path": "000100010001", "url_path": mistyped}, XPRO_STAND_INS[3]],
                "has url_path '/home/courses/local-fixtures-i/', "
                f"not directly under its stand-in parent's '{mistyped}'",
            )
            # The last two are prefixes of the real one's pages: an ancestor, and the slash left off.
            for mistyped in ["/home/course/", "/home/", "/home/courses"]
        ),
        ([_page("0001000100010005", depth=3)], XPRO_STAND_INS, "the row with id=100 has depth 3 at path"),
        (
            [_page("0001000100010005"), _page("00010001000100050001", id=101)],
            XPRO_STAND_INS,
            "the row with id=100 has numchild 0 and 1 children",
        ),
        (
            [_page("00010001000100010")],
            XPRO_STAND_INS,
            f"{XPRO_PAGES}: the row with id=100 has path '00010001000100010', which is not whole steps",
        ),
        (
            [],
            [*XPRO_STAND_INS, {"path": "00010001000"}],
            f"{XPRO_PAGES}: a stand-in has path '00010001000', which is not whole steps",
        ),
        # Declared, and nothing placed under it.
        (
            [],
            [*XPRO_STAND_INS, {"path": "000100010003", "url_path": "/home/faculty/"}],
            f"{XPRO_PAGES}: stand-in '000100010003' has no row or stand-in under it",
        ),
        *(
            (
                [{key: value for key, value in _page("0001000100010005").items() if key != column}],
                XPRO_STAND_INS,
                message,
            )
            for column, message in [
                ("path", f"{XPRO_PAGES}: the row with id=100 has no path"),
                ("depth", f"{XPRO_PAGES}: the row with id=100 has no depth"),
                ("numchild", f"{XPRO_PAGES}: the row with id=100 has no numchild"),
            ]
        ),
        ([{"id": 100, "path": None, "numchild": 0}], XPRO_STAND_INS, "id=100 has no path or depth"),
    ],
)
def test_rows_that_do_not_make_one_tree_with_their_stand_ins_are_refused(
    generated: list[Row], stand_ins: list[Row], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        check_tree(XPRO_PAGES, _committed_rows(XPRO_PAGES), generated, key_columns=["id"], stand_ins=stand_ins)


def test_a_hand_written_row_without_its_key_is_named_by_null() -> None:
    pages = _committed_rows(XPRO_PAGES)
    keyless, pathless = _by_id(pages, 10), _by_id(pages, 13)
    del keyless["id"]
    del pathless["path"]

    # The row with no key comes first and is a valid one, so the check reaches the row with no path.
    with pytest.raises(ValueError, match=f"{XPRO_PAGES}: the row with id=13 has no path"):
        check_tree(XPRO_PAGES, pages, [], key_columns=["id"], stand_ins=XPRO_STAND_INS)


def test_a_hand_written_parent_may_count_children_the_fixture_leaves_out() -> None:
    pages = _committed_rows(XPRO_PAGES)
    _by_id(pages, 10)["numchild"] = 7

    check_tree(XPRO_PAGES, pages, [], key_columns=["id"], stand_ins=XPRO_STAND_INS)


def test_each_level_attaches_to_rows_built_before_it() -> None:
    topics = _committed_rows("raw__xpro__app__postgres__courses_coursetopic")
    top = [{"id": 10 + n, "parent_id": None} for n in range(4)]
    middle = attach([{"id": 20 + n} for n in range(8)], top + topics)
    bottom = attach([{"id": 30 + n} for n in range(16)], middle)

    rows = topics + top + middle + bottom
    for position, row in enumerate(rows):
        if row.get("parent_id") is not None:
            assert row["parent_id"] in {earlier["id"] for earlier in rows[:position]}
    assert {row["parent_id"] for row in bottom} <= {row["id"] for row in middle}
    # Hand-written rows keep their own parents.
    assert [row.get("parent_id") for row in topics] == [None, 1, None]


def test_attach_names_its_columns() -> None:
    (child,) = attach([{"topic_id": 5}], [{"topic_id": 1}], column="parent_topic_id", key="topic_id")

    assert child == {"topic_id": 5, "parent_topic_id": 1}


def test_attach_needs_a_parent() -> None:
    assert attach([], []) == []
    with pytest.raises(ValueError, match="cannot attach 2 rows to no parents"):
        attach([{}, {}], [])
