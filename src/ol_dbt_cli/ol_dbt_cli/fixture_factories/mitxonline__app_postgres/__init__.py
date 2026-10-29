"""The factory package of the `mitxonline/app_postgres` unit.

`_generated.py` holds the base factories `ol-dbt fixtures scaffold` wrote from
mitxonline's models. This module subclasses them for what the models cannot
say and wires the tables together in `generate`, around the hand-written rows,
which keep the branches the Learn models take (a course with no page, a
MicroMasters program with no page, a run with no end, a contract page's
variant that shares a course's object id).

What the generated rows add is volume on the main path: live programs, each
with a requirement tree over a few courses, each course with an open run and
an ended one, enrollment modes, a live page per course and program with
topics, instructors, a price and FAQs, and the departments Learn knows. A share of
rows, one in six to one in thirteen depending on the branch, is left off that
path so the models' filters have rows to drop: a course with no page, a draft
page, a page left out of the Learn catalog, a proctored exam, a run that is
not live, a B2B-only run or program, a run in a language the course has a
non-default variant for, and a program that is not live. Each cycle's first
off-path row falls within the default counts, so every branch has a row at
scale 1. Every fifth course also has a run that starts in 2099, which a
program's next start reads, and every other one of those is a source run,
which it does not. One in four pages has no FAQs, and some FAQs have no sort
order, which the pages API lists last.

Content types and enrollment modes are not generated: the hand-written rows
are the two content types a variant points through and the audit and
verified modes, and the staging tests hold their names unique.

Every date is either before the extract (`fg.LOADED_AT`) or in 2099, so what
the models list does not change with the day they build: whether a run is
enrollable is decided against the clock.
"""

from __future__ import annotations

import datetime
import re
import types
from typing import TYPE_CHECKING, Any

import factory

from ol_dbt_cli.lib import fixture_generation as fg
from ol_dbt_cli.lib.fixture_trees import Tree

from ._generated import (
    TREE_ALPHABETS,
    ContentTypeBase,
    CourseBase,
    CourseDepartmentsBase,
    CoursePageBase,
    CoursePageTopicsBase,
    CourseRunBase,
    CourseRunEnrollmentModesBase,
    CoursesTopicBase,
    DepartmentBase,
    EnrollmentModeBase,
    ImageBase,
    InstructorPageBase,
    InstructorPageLinkBase,
    PageBase,
    ProductPageFAQBase,
    ProgramBase,
    ProgramDepartmentsBase,
    ProgramEnrollmentModesBase,
    ProgramPageBase,
    ProgramRequirementBase,
    SupportedVariantBase,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from ol_dbt_cli.lib.fixture_generation import Row

SEED = 3955118133

# The wagtail pages the fixture leaves out: the root and home pages, and the
# index pages the product and instructor pages sit under.
ROOT_PAGE = {"path": "0001", "url_path": "/"}
HOME_PAGE = {"path": "00010001", "url_path": "/home/"}
COURSES_INDEX = {"path": "000100010001", "url_path": "/home/courses/"}
PROGRAMS_INDEX = {"path": "000100010002", "url_path": "/home/programs/"}
INSTRUCTORS_INDEX = {"path": "000100010003", "url_path": "/home/instructors/"}
# Every page sits under a page of the table or one of these.
TREE_STAND_INS = {PageBase.raw_table: [ROOT_PAGE, HOME_PAGE, COURSES_INDEX, PROGRAMS_INDEX, INSTRUCTORS_INDEX]}
# The content type of each page type, as the hand-written pages have them.
# Nothing reads them, and django_content_type holds only the two a variant
# points through.
COURSE_PAGE_TYPE = {"id": 50}
PROGRAM_PAGE_TYPE = {"id": 51}
INSTRUCTOR_PAGE_TYPE = {"id": 52}
# Every page's locale and every image's collection. Neither table is in the
# fixture, and nothing reads them.
LOCALE = {"id": 1}
COLLECTION = {"id": 1}
OPEN_RUN_END = datetime.datetime(2099, 12, 31)  # noqa: DTZ001
OPEN_ENROLLMENT_END = datetime.datetime(2099, 6, 30)  # noqa: DTZ001
# An upcoming run starts here, enrollable from 2025: the earliest future start
# of a program's runs is its start date.
UPCOMING_RUN_START = datetime.datetime(2099, 3, 1)  # noqa: DTZ001
# The last moment a date may fall on that is not in 2099: the extract's.
EXTRACTED = fg.LOADED_AT.replace(tzinfo=None)
TOPICS_PER_COURSE_PAGE = 2
# The language of a course's variant runs, and the one every other run has.
VARIANT_LANGUAGE = "es_419"
LANGUAGE = "en"
# Topic names MIT Learn keeps (mit-learn learning_resources/data/topics.yaml):
# its ETL drops a MITx Online topic whose name is not a Learn topic. Each
# top-level topic with some of its subtopics, as MITx Online nests them.
LEARN_TOPICS = {
    "Engineering": [
        "Aerospace Engineering",
        "Civil Engineering",
        "Electrical Engineering",
        "Mechanical Engineering",
        "Systems Engineering",
    ],
    "Science & Math": ["Biology", "Chemistry", "Physics", "Mathematics", "Earth Science"],
    "Data Science, Analytics & Computer Technology": [
        "AI",
        "Machine Learning",
        "Data Science",
        "Computer Science",
        "Programming & Coding",
    ],
    "Business & Management": ["Entrepreneurship", "Marketing", "Supply Chain", "Operations", "Finance & Accounting"],
}
# Department names MIT Learn maps to its departments (mit-learn
# learning_resources/constants.py DEPARTMENTS). It drops one it does not know.
LEARN_DEPARTMENTS = (
    "Civil and Environmental Engineering",
    "Mechanical Engineering",
    "Materials Science and Engineering",
    "Architecture",
    "Chemistry",
    "Electrical Engineering and Computer Science",
    "Biology",
    "Physics",
)


def _slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")


def _before_extract(moment: datetime.datetime) -> datetime.datetime:
    return min(moment, EXTRACTED)


def _block_id() -> str:
    return str(fg.draw("cryptographic.uuid"))


def _price(row: Any) -> list[dict[str, Any]]:
    """Return the page's price StreamField, one price_details block, as the hand-written pages have it."""
    text = f"${row.max_price:.0f}" if row.max_price else "Free"
    return [{"type": "price_details", "value": {"text": text, "link": ""}, "id": _block_id()}]


@fg.table(CoursesTopicBase.raw_table, default_count=12)
class CoursesTopic(CoursesTopicBase):
    # Named and nested in generate(), from LEARN_TOPICS.
    name = None
    parent_id = None
    created_on = fg.value("datetime.datetime", start=2024, end=2024)
    updated_on = factory.LazyAttribute(lambda row: row.created_on)


@fg.table(DepartmentBase.raw_table, default_count=4)
class Department(DepartmentBase):
    # Named in generate(), from LEARN_DEPARTMENTS.
    name = None
    slug = factory.LazyAttribute(lambda row: _slug(row.name))
    created_on = fg.value("datetime.datetime", start=2024, end=2024)
    updated_on = factory.LazyAttribute(lambda row: row.created_on)


@fg.table(ImageBase.raw_table, default_count=8)
class Image(ImageBase):
    class Params:
        collection = COLLECTION

    title = factory.Sequence(lambda n: f"Generated image {n}")
    file = factory.Sequence(lambda n: f"original_images/generated-{n}.jpg")
    file_hash = factory.Sequence(lambda n: f"{n:040d}")
    width = 1200
    height = 675
    file_size = fg.value("numeric.integer_number", start=10000, end=90000)
    created_at = fg.value("datetime.datetime", start=2024, end=2025)
    focal_point_x = None
    focal_point_y = None
    focal_point_width = None
    focal_point_height = None
    description = ""


@fg.table(ProgramBase.raw_table, default_count=8)
class Program(ProgramBase):
    class Params:
        # A MicroMasters program: staging splits its title into name and track
        # and gives it the MicroMasters credential.
        micromasters = False

    title = factory.LazyAttributeSequence(
        lambda row, n: f"Generated Program {n}: Track {n}" if row.micromasters else f"Generated Program {n}"
    )
    readable_id = factory.Sequence(lambda n: f"program-v1:MITxT+GEN{n}")
    program_type = factory.LazyAttribute(lambda row: "MicroMasters®" if row.micromasters else "Series")
    # One in eight not live and one in nine B2B-only, both of which the program
    # models drop. Placed so the eight programs at scale 1 have one of each.
    live = factory.Iterator([*[True] * 6, False, True])
    b2b_only = factory.Iterator([*[False] * 5, True, *[False] * 3])
    availability = factory.Iterator(["anytime", "dated"])
    # Set in generate() on a program another requires, so it is listed as a course.
    display_mode = None
    certificates_disabled = False
    start_date = factory.LazyAttribute(
        lambda row: fg.draw("datetime.datetime", start=2025, end=2025) if row.availability == "dated" else None
    )
    enrollment_start = factory.LazyAttribute(
        lambda row: row.start_date - datetime.timedelta(days=30) if row.start_date else None
    )
    end_date = None
    enrollment_end = None
    created_on = fg.value("datetime.datetime", start=2024, end=2024)
    updated_on = factory.LazyAttribute(lambda row: row.created_on)


@fg.table(CourseBase.raw_table, default_count=20)
class Course(CourseBase):
    class Params:
        # A proctored exam, which MIT Learn's ETL drops by its title.
        proctored = False

    title = factory.LazyAttributeSequence(
        lambda row, n: f"Generated Course {n} Proctored Exam" if row.proctored else f"Generated Course {n}"
    )
    readable_id = factory.Sequence(lambda n: f"course-v1:MITxT+GEN{n}x")
    # One in ten not live: the program models leave it out of a program's topics.
    live = factory.Iterator([*[True] * 9, False])
    certificates_disabled = False
    created_on = fg.value("datetime.datetime", start=2024, end=2024)
    updated_on = factory.LazyAttribute(lambda row: row.created_on)


class CourseRun(CourseRunBase):
    class Params:
        # "open" ends in 2099, "closed" started in 2024 and ended 30 to 90 days
        # later, and "upcoming" starts in 2099 with enrollment open since 2025.
        state = "open"

    title = factory.LazyAttribute(lambda row: row.course["title"])
    run_tag = factory.Sequence(lambda n: f"R{n}")
    courseware_id = factory.LazyAttribute(lambda row: f"{row.course['readable_id']}+{row.run_tag}")
    courseware_url_path = factory.LazyAttribute(lambda row: f"/learn/course/{row.courseware_id}/home")
    has_courseware_url = True
    # One in nine not live, which makes a run not enrollable. Odd: with two runs
    # to most courses, an even cycle would only catch closed runs.
    live = factory.Iterator([*[True] * 8, False])
    is_self_paced = factory.Iterator([False, False, True])
    # One in thirteen B2B-only, which makes it a variant and unpublished.
    b2b_only = factory.Iterator([*[False] * 12, True])
    is_source_run = False
    language = LANGUAGE
    is_primary_language = True
    variant_length = ""
    variant_industry = ""
    start_date = factory.LazyAttribute(
        lambda row: {
            "open": fg.draw("datetime.datetime", start=2025, end=2025),
            "closed": fg.draw("datetime.datetime", start=2024, end=2024),
            "upcoming": UPCOMING_RUN_START,
        }[row.state]
    )
    # Thirty days before the start, or in 2025 for an upcoming run: before the
    # extract either way, so an open or upcoming run's window is open whenever
    # the model builds.
    enrollment_start = factory.LazyAttribute(
        lambda row: (
            fg.draw("datetime.datetime", start=2025, end=2025)
            if row.state == "upcoming"
            else row.start_date - datetime.timedelta(days=30)
        )
    )
    end_date = factory.LazyAttribute(
        lambda row: (
            row.start_date + datetime.timedelta(days=fg.draw("numeric.integer_number", start=30, end=90))
            if row.state == "closed"
            else OPEN_RUN_END
        )
    )
    enrollment_end = factory.LazyAttribute(
        lambda row: row.start_date + datetime.timedelta(days=7) if row.state == "closed" else OPEN_ENROLLMENT_END
    )
    upgrade_deadline = None
    expiration_date = None
    certificate_available_date = None
    created_on = factory.LazyAttribute(lambda row: row.course["created_on"])
    updated_on = factory.LazyAttribute(lambda row: row.course["created_on"])


class ProgramRequirement(ProgramRequirementBase):
    # Set per node in _requirements(): which columns a node has depends on its type.
    title = None
    operator = None
    operator_value = None
    node_type = None
    elective_flag = False


class Page(PageBase):
    class Params:
        locale = LOCALE

    draft_title = factory.LazyAttribute(lambda row: row.title)
    seo_title = ""
    search_description = ""
    live = True
    has_unpublished_changes = factory.LazyAttribute(lambda row: not row.live)
    locked = False
    locked_at = None
    expired = False
    expire_at = None
    go_live_at = None
    show_in_menus = False
    # The hand-written pages leave it NULL, and nothing reads it.
    translation_key = None
    first_published_at = factory.LazyAttribute(
        lambda row: fg.draw("datetime.datetime", start=2025, end=2025) if row.live else None
    )
    last_published_at = factory.LazyAttribute(
        lambda row: (
            _before_extract(
                row.first_published_at + datetime.timedelta(days=fg.draw("numeric.integer_number", start=0, end=300))
            )
            if row.live
            else None
        )
    )
    latest_revision_created_at = factory.LazyAttribute(
        lambda row: row.last_published_at or _before_extract(fg.draw("datetime.datetime", start=2026, end=2026))
    )


def _product_page(**declarations: Any) -> dict[str, Any]:
    """Return the declarations a course page and a program page share, with ``declarations`` added.

    factory_boy reads declarations only from factories, so they are not on a
    mixin. Fresh for each factory: an iterator keeps its place in its factory.
    """
    return {
        "about": factory.LazyFunction(lambda: f"<p>{fg.draw('text.sentence')}</p>"),
        "description": factory.LazyFunction(lambda: f"<p>{fg.draw('text.sentence')}</p>"),
        "what_you_learn": factory.LazyFunction(lambda: f"<ul><li>{fg.draw('text.sentence')}</li></ul>"),
        "prerequisites": "<p>None.</p>",
        "min_weeks": fg.value("numeric.integer_number", start=4, end=12),
        "max_weeks": factory.LazyAttribute(lambda row: row.min_weeks),
        "length": factory.LazyAttribute(lambda row: f"{row.min_weeks} weeks"),
        "min_weekly_hours": fg.value("numeric.integer_number", start=2, end=6),
        "max_weekly_hours": factory.LazyAttribute(lambda row: row.min_weekly_hours + 2),
        "effort": factory.LazyAttribute(lambda row: f"{row.min_weekly_hours}-{row.max_weekly_hours} hours per week"),
        # One in five free: a page with no price makes every run and program free.
        "min_price": factory.Sequence(
            lambda n: 0.0 if n % 5 == 0 else float(fg.draw("numeric.integer_number", start=49, end=499))
        ),
        "max_price": factory.LazyAttribute(lambda row: row.min_price * 2),
        "price": factory.LazyAttribute(_price),
        "faq_url": None,
        "video_url": None,
        "faculty_section_title": None,
        "hubspot_form_id": "",
        "show_stay_updated": False,
        # One in nine left out of the Learn catalog, which unpublishes it there.
        # The eighth, so the eight programs at scale 1 have one.
        "include_in_learn_catalog": factory.Iterator([*[True] * 7, False, True]),
        "hyl_choice_learn_by_doing": None,
        "hyl_choice_learn_on_demand": None,
        "hyl_choice_learn_from_others": None,
        "hyl_choice_ai_enabled_support": None,
        "hyl_choice_realworld_learning": None,
        "hyl_choice_stackable_credentials": None,
        "__module__": __name__,
        **declarations,
    }


CoursePage = types.new_class(
    "CoursePage",
    (CoursePageBase,),
    exec_body=lambda namespace: namespace.update(
        _product_page(ingest_content_files_for_ai=False, show_course_outline=False)
    ),
)
ProgramPage = types.new_class(
    "ProgramPage", (ProgramPageBase,), exec_body=lambda namespace: namespace.update(_product_page(list_price=None))
)


@fg.table(InstructorPageBase.raw_table, default_count=6)
class InstructorPage(InstructorPageBase):
    instructor_name = fg.value("person.full_name")
    instructor_title = factory.Iterator(["Professor", "Lecturer", "Research Scientist"])
    instructor_bio_short = fg.value("text.sentence")
    instructor_bio_long = None


class ProductPageFAQ(ProductPageFAQBase):
    question = factory.LazyFunction(lambda: f"{fg.draw('text.sentence').removesuffix('.')}?")
    answer = factory.LazyFunction(lambda: f"<p>{fg.draw('text.sentence')}</p>")


class SupportedVariant(SupportedVariantBase):
    class Params:
        # The course the variant set belongs to.
        course = None

    object_id = factory.LazyAttribute(lambda row: row.course["id"])
    language = VARIANT_LANGUAGE
    variant_length = ""
    variant_industry = ""
    active = True
    b2b_only = False
    # Not the default: the course's runs in this language are variants.
    default_variant = False
    created_on = factory.LazyAttribute(lambda row: row.course["created_on"])
    updated_on = factory.LazyAttribute(lambda row: row.course["created_on"])


def _by(hand_written: Mapping[str, list[Row]], raw_table: str, column: str, value: Any) -> Row:
    (row,) = (row for row in hand_written[raw_table] if row[column] == value)
    return row


def _topics(hand_written: Mapping[str, list[Row]], scale: float) -> list[Row]:
    """Learn's topics, each top-level one before its subtopics, as many as the scale asks for.

    A top-level topic the hand-written rows have (Engineering) is theirs, and
    its subtopics are placed under it. No more than LEARN_TOPICS holds: a name
    is unique under its parent, so past that scale the courses share them.
    """
    existing = {(row["name"], row.get("parent_id")) for row in hand_written[CoursesTopicBase.raw_table]}
    roots = {row["name"]: row for row in hand_written[CoursesTopicBase.raw_table] if row.get("parent_id") is None}
    # The parents first, then a subtopic of each parent in turn, so a small count spans every parent.
    wanted: list[tuple[str, str | None]] = [(parent, None) for parent in LEARN_TOPICS]
    wanted += [
        (children[rank], parent)
        for rank in range(max(len(children) for children in LEARN_TOPICS.values()))
        for parent, children in LEARN_TOPICS.items()
        if rank < len(children)
    ]
    topics: list[Row] = []
    for name, parent in wanted:
        if len(topics) == fg.scaled_count(CoursesTopic, scale):
            break
        if parent is None:
            if name in roots:
                continue
            roots[name] = CoursesTopic.build(name=name)
            topics.append(roots[name])
        elif (name, roots[parent]["id"]) not in existing:
            topics.append(CoursesTopic.build(name=name, parent_id=roots[parent]["id"]))
    return topics


def _departments(hand_written: Mapping[str, list[Row]], scale: float) -> list[Row]:
    taken = {row["name"] for row in hand_written[DepartmentBase.raw_table]}
    names = [name for name in LEARN_DEPARTMENTS if name not in taken]
    return [Department.build(name=name) for name in names[: min(fg.scaled_count(Department, scale), len(names))]]


def _runs(courses: list[Row], course_type: Row) -> tuple[list[Row], list[Row]]:
    """Build an open run and a closed one per course, an upcoming one for every fifth, and variant runs.

    :param course_type: The courses.course content type a variant set points through.
    :returns: The runs, and a supported variant for each course that has variant runs.
    """
    runs: list[Row] = []
    variants: list[Row] = []
    for number, course in enumerate(courses):
        runs += [CourseRun.build(course=course, state="open"), CourseRun.build(course=course, state="closed")]
        if number % 5 == 4:
            # Every other one a source run, which a program's next start skips.
            runs.append(CourseRun.build(course=course, state="upcoming", is_source_run=number % 10 == 4))
        # One in six has a run in a language it has a non-default variant set
        # for: that run is a variant, and unpublished.
        if number % 6 == 5:
            runs.append(CourseRun.build(course=course, state="open", language=VARIANT_LANGUAGE))
            variants.append(SupportedVariant.build(course=course, content_type=course_type))
    return runs, variants


def _modes(runs: list[Row], audit: Row, verified: Row) -> list[Row]:
    """Audit and verified, verified only, audit only, both, or neither, in turn: a run with neither is free."""
    cycle = [(audit, verified), (verified,), (audit,), (audit, verified), ()]
    return [
        CourseRunEnrollmentModesBase.build(courserun=run, enrollmentmode=mode)
        for number, run in enumerate(runs)
        for mode in cycle[number % len(cycle)]
    ]


def _requirements(tree: Tree, program: Row, courses: list[Row], required_program: Row | None) -> list[Row]:
    """Build a program's requirement tree: a root, required courses under all_of, and its last course elective.

    A program with three courses or more has one elective, under min_number_of
    1. A required program is a node under the required operator.
    """
    (root,) = tree.add([ProgramRequirement.build(program=program, node_type="program_root")])
    elective = courses[-1:] if len(courses) >= 3 else []
    required = courses[: len(courses) - len(elective)]
    operators = [
        ProgramRequirement.build(program=program, node_type="operator", operator="all_of", title="Required Courses")
    ]
    if elective:
        operators.append(
            ProgramRequirement.build(
                program=program,
                node_type="operator",
                operator="min_number_of",
                operator_value="1",
                title="Elective Courses",
                elective_flag=True,
            )
        )
    tree.add(operators, under=root)
    nodes = [root, *operators]
    nodes += tree.add(
        [ProgramRequirement.build(program=program, course=course, node_type="course") for course in required],
        under=operators[0],
    )
    if required_program is not None:
        nodes += tree.add(
            [ProgramRequirement.build(program=program, required_program=required_program, node_type="program")],
            under=operators[0],
        )
    if elective:
        nodes += tree.add(
            [ProgramRequirement.build(program=program, course=course, node_type="course") for course in elective],
            under=operators[1],
        )
    return nodes


def _product_pages(
    tree: Tree,
    page_factory: type[fg.RowFactory],
    products: list[Row],
    *,
    index: Row,
    page_type: Row,
    images: list[Row],
    column: str,
) -> tuple[list[Row], list[Row]]:
    """Give each of ``products`` a wagtail page under ``index`` and its product page.

    :returns: The wagtail pages and the product pages.
    """
    # One in seven a draft, which the models drop. Seven shares no factor with
    # the course (10) and program (8) live cycles, and the third product is the
    # first draft so the eight programs at scale 1 have one.
    lives = [number % 7 != 2 for number in range(len(products))]
    wagtail = tree.add(
        [
            Page.build(title=product["title"], slug=_slug(product["readable_id"]), content_type=page_type, live=live)
            for product, live in zip(products, lives, strict=True)
        ],
        # A copy: Tree.add counts the children on the parent it is given.
        under=dict(index),
    )
    features = fg.pick([*images, None], len(products))
    pages = [
        page_factory.build(page_ptr=page, feature_image=feature, search_image=None, **{column: product})
        for page, product, feature in zip(wagtail, products, features, strict=True)
    ]
    return wagtail, pages


def _faqs(pages: list[Row]) -> list[Row]:
    """Give three in four of ``pages`` one to three FAQs.

    The sort order runs against the id, so the models order by it and not by
    the id. Every eighth page leaves its first FAQ unordered, which then comes
    last.
    """
    faqs = []
    for number, page in enumerate(pages):
        count = number % 4
        for position in range(count):
            unordered = number % 8 == 3 and position == 0
            faqs.append(ProductPageFAQ.build(page=page, sort_order=None if unordered else count - 1 - position))
    return faqs


def generate(hand_written: Mapping[str, list[Row]], scale: float) -> dict[str, list[Row]]:
    """Build programs, courses, runs, requirement trees and pages around the hand-written rows."""
    audit = _by(hand_written, EnrollmentModeBase.raw_table, "mode_slug", "audit")
    verified = _by(hand_written, EnrollmentModeBase.raw_table, "mode_slug", "verified")
    course_type = next(
        row
        for row in hand_written[ContentTypeBase.raw_table]
        if (row["app_label"], row["model"]) == ("courses", "course")
    )
    topics = _topics(hand_written, scale)
    departments = _departments(hand_written, scale)
    images = Image.build_batch(fg.scaled_count(Image, scale))

    programs = [Program.build(micromasters=number % 3 == 2) for number in range(fg.scaled_count(Program, scale))]
    # One in eleven a proctored exam. One in four outside any program, the
    # rest drawn among the programs: about two courses to a program.
    course_count = fg.scaled_count(Course, scale)
    courses = [Course.build(proctored=number % 11 == 10) for number in range(course_count)]
    homes = [None if number % 4 == 3 else program for number, program in enumerate(fg.pick(programs, course_count))]
    runs, variants = _runs(courses, course_type)

    # The second program and every third after it require the program before
    # them. Every other required program is shown as a course, and the rest
    # stand for their own courses in the requiring program's list.
    required = {program["id"]: programs[number - 1] for number, program in enumerate(programs) if number % 3 == 1}
    for number, required_program in enumerate(required.values()):
        if number % 2 == 0:
            required_program["display_mode"] = "course"
    requirement_tree = Tree(
        hand_written[ProgramRequirementBase.raw_table], alphabet=TREE_ALPHABETS[ProgramRequirementBase.raw_table]
    )
    requirements = [
        node
        for program in programs
        for node in _requirements(
            requirement_tree,
            program,
            [course for course, home in zip(courses, homes, strict=True) if home is program],
            required.get(program["id"]),
        )
    ]

    page_tree = Tree(hand_written[PageBase.raw_table], alphabet=TREE_ALPHABETS[PageBase.raw_table])
    # One in eight courses has no page, which leaves it unpublished.
    paged = [course for number, course in enumerate(courses) if number % 8 != 7]
    course_wagtail, course_pages = _product_pages(
        page_tree, CoursePage, paged, index=COURSES_INDEX, page_type=COURSE_PAGE_TYPE, images=images, column="course"
    )
    program_wagtail, program_pages = _product_pages(
        page_tree,
        ProgramPage,
        programs,
        index=PROGRAMS_INDEX,
        page_type=PROGRAM_PAGE_TYPE,
        images=images,
        column="program",
    )
    instructor_count = fg.scaled_count(InstructorPage, scale)
    instructor_wagtail = page_tree.add(
        [
            Page.build(title=f"Instructor {number}", slug=f"instructor-{number}", content_type=INSTRUCTOR_PAGE_TYPE)
            for number in range(instructor_count)
        ],
        under=dict(INSTRUCTORS_INDEX),
    )
    instructors = [InstructorPage.build(page_ptr=page) for page in instructor_wagtail]
    for page, instructor in zip(instructor_wagtail, instructors, strict=True):
        page["title"] = page["draft_title"] = instructor["instructor_name"]
    all_instructors = [*hand_written[InstructorPageBase.raw_table], *instructors]
    # One or two instructors on each product page.
    links = [
        InstructorPageLinkBase.build(page=page, linked_instructor_page=instructor, order=order)
        for number, page in enumerate(course_wagtail + program_wagtail)
        for order, instructor in enumerate(fg.pair(all_instructors, 1 + number % 2))
    ]

    # Linked from Learn's names only: the hand-written Data Engineering topic
    # and Local Fixture Studies department are ones Learn drops.
    learn_topics = {name for parent, children in LEARN_TOPICS.items() for name in (parent, *children)}
    all_topics = [row for row in [*hand_written[CoursesTopicBase.raw_table], *topics] if row["name"] in learn_topics]
    all_departments = [
        row for row in [*hand_written[DepartmentBase.raw_table], *departments] if row["name"] in LEARN_DEPARTMENTS
    ]
    return {
        CoursesTopicBase.raw_table: topics,
        DepartmentBase.raw_table: departments,
        ImageBase.raw_table: images,
        ProgramBase.raw_table: programs,
        CourseBase.raw_table: courses,
        CourseRunBase.raw_table: runs,
        SupportedVariantBase.raw_table: variants,
        CourseRunEnrollmentModesBase.raw_table: _modes(runs, audit, verified),
        # Four in five programs offer the verified mode, which certifies them,
        # and one in five of those the audit mode too, which adds a free price.
        ProgramEnrollmentModesBase.raw_table: [
            ProgramEnrollmentModesBase.build(program=program, enrollmentmode=mode)
            for number, program in enumerate(programs)
            if number % 5 != 3
            for mode in ((audit, verified) if number % 5 == 0 else (verified,))
        ],
        ProgramRequirementBase.raw_table: requirements,
        PageBase.raw_table: course_wagtail + program_wagtail + instructor_wagtail,
        CoursePageBase.raw_table: course_pages,
        ProgramPageBase.raw_table: program_pages,
        InstructorPageBase.raw_table: instructors,
        InstructorPageLinkBase.raw_table: links,
        ProductPageFAQBase.raw_table: _faqs(course_wagtail + program_wagtail),
        CoursePageTopicsBase.raw_table: [
            CoursePageTopicsBase.build(coursepage=page, coursestopic=topic)
            for page in course_pages
            for topic in fg.pair(all_topics, min(TOPICS_PER_COURSE_PAGE, len(all_topics)))
        ],
        CourseDepartmentsBase.raw_table: [
            CourseDepartmentsBase.build(course=course, department=department)
            for course, department in zip(courses, fg.pick(all_departments, len(courses)), strict=True)
        ],
        ProgramDepartmentsBase.raw_table: [
            ProgramDepartmentsBase.build(program=program, department=department)
            for program, department in zip(programs, fg.pick(all_departments, len(programs)), strict=True)
        ],
    }
