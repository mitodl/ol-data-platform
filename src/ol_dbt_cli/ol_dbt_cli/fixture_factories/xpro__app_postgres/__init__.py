"""The factory package of the `xpro/app_postgres` unit.

`_generated.py` holds the base factories `ol-dbt fixtures scaffold` wrote from
mitxpro's models. This module subclasses them for what the models cannot say
and wires the tables together in `generate`, around the hand-written rows,
which keep the branches the Learn models take (a draft page, a course or
program that is not live, an external course and program, a retired price, a
run that ended).

What the generated rows add is volume on the main path: live programs, each
with a few live courses, each course with an open run and an ended one, their
products and prices, and a live wagtail page for every course and program with
certificate and faculty child pages. A share of each is left off that path
(not live, no product, a draft page) so the models' filters have rows to drop.

Content types and platforms are not generated: the hand-written rows are the
two content types a product points through and the two platforms, and their
names are unique in the staging tests.
"""

from __future__ import annotations

import datetime
import re
from typing import TYPE_CHECKING, Any

import factory

from ol_dbt_cli.lib import fixture_generation as fg
from ol_dbt_cli.lib.fixture_trees import Tree, attach

from ._generated import (
    TREE_ALPHABETS,
    CertificatePageBase,
    ContentTypeBase,
    CourseBase,
    CoursePageBase,
    CoursePageTopicsBase,
    CourseRunBase,
    CourseTopicBase,
    FacultyMembersPageBase,
    ImageBase,
    PageBase,
    PlatformBase,
    ProductBase,
    ProductVersionBase,
    ProgramBase,
    ProgramPageBase,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from ol_dbt_cli.lib.fixture_generation import Row

SEED = 311373020

# The wagtail index pages the course and program pages sit under. The fixture
# leaves them out, as it does the root and home pages above them.
COURSES_INDEX = {"path": "000100010001", "url_path": "/home/courses/"}
PROGRAMS_INDEX = {"path": "000100010002", "url_path": "/home/programs/"}
# The content type of each page type, as the hand-written pages have them.
# Nothing reads them, and django_content_type holds only the two a product
# points through.
COURSE_PAGE_TYPE = {"id": 50}
PROGRAM_PAGE_TYPE = {"id": 51}
CERTIFICATE_PAGE_TYPE = {"id": 52}
FACULTY_PAGE_TYPE = {"id": 53}
# Every page's locale and every image's collection. Neither table is in the
# fixture, and nothing reads them.
LOCALE = {"id": 1}
COLLECTION = {"id": 1}
# An open run ends here, as the hand-written ones do: whether a run is expired
# is decided against the clock when the model builds, so a date near today
# would change which rows the models list.
OPEN_RUN_END = datetime.datetime(2099, 12, 31)  # noqa: DTZ001
OPEN_ENROLLMENT_END = datetime.datetime(2099, 6, 30)  # noqa: DTZ001
RUNS_PER_COURSE = 2
TOPICS_PER_COURSE_PAGE = 2


def _slug(readable_id: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", readable_id.lower()).strip("-")


def _weeks() -> Any:
    return fg.value("numeric.integer_number", start=4, end=12)


def _weekly_hours() -> Any:
    return fg.value("numeric.integer_number", start=2, end=6)


def _block_id() -> str:
    return str(fg.draw("cryptographic.uuid"))


@fg.table(CourseTopicBase.raw_table, default_count=6)
class CourseTopic(CourseTopicBase):
    name = factory.Sequence(lambda n: f"Generated Topic {n}")


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
    focal_point_x = None
    focal_point_y = None
    focal_point_width = None
    focal_point_height = None
    description = ""


@fg.table(ProgramBase.raw_table, default_count=4)
class Program(ProgramBase):
    title = factory.Sequence(lambda n: f"Generated Program {n}")
    readable_id = factory.Sequence(lambda n: f"program-v1:xPRO+GEN{n}x")
    # One in four not live, which the program model drops.
    live = factory.Iterator([True, True, True, False])
    is_external = False


@fg.table(CourseBase.raw_table, default_count=12)
class Course(CourseBase):
    title = factory.Sequence(lambda n: f"Generated Course {n}")
    readable_id = factory.Sequence(lambda n: f"course-v1:xPRO+GEN{n}x")
    # One in five not live, which the course model and a program's course list drop.
    live = factory.Iterator([True, True, True, True, False])
    is_external = False
    external_course_id = None
    position_in_program = None


class CourseRun(CourseRunBase):
    class Params:
        # An open run ends in 2099. A closed one started and ended in 2024.
        is_open = True

    title = factory.LazyAttribute(lambda row: row.course["title"])
    run_tag = factory.Sequence(lambda n: f"R{n}")
    courseware_id = factory.LazyAttribute(lambda row: f"{row.course['readable_id']}+{row.run_tag}")
    courseware_url_path = factory.LazyAttribute(lambda row: f"/courses/{row.courseware_id}/course/")
    external_course_run_id = None
    # One in five not live, which the catalog leaves out. Five, not an even
    # number: with two runs to a course, an even cycle would only ever catch
    # the closed runs, which the models drop anyway.
    live = factory.Iterator([True, True, True, True, False])
    start_date = factory.LazyAttribute(
        lambda row: (
            fg.draw("datetime.datetime", start=2025, end=2025)
            if row.is_open
            else fg.draw("datetime.datetime", start=2024, end=2024)
        )
    )
    # Enrollment opens a month before the start, which is before today for
    # every run, so an open run's window is open whenever the model builds.
    enrollment_start = factory.LazyAttribute(lambda row: row.start_date - datetime.timedelta(days=30))
    end_date = factory.LazyAttribute(
        lambda row: (
            OPEN_RUN_END
            if row.is_open
            else row.start_date + datetime.timedelta(days=fg.draw("numeric.integer_number", start=30, end=90))
        )
    )
    enrollment_end = factory.LazyAttribute(
        lambda row: OPEN_ENROLLMENT_END if row.is_open else row.start_date + datetime.timedelta(days=7)
    )
    expiration_date = None


class Product(ProductBase):
    class Params:
        # The course run or program row the product sells.
        target = None

    object_id = factory.LazyAttribute(lambda row: row.target["id"])
    # One in eight inactive: a run with one has no price, and a program keeps
    # its listing without a price.
    is_active = factory.Iterator([True, True, True, True, True, True, True, False])
    is_private = False


class ProductVersion(ProductVersionBase):
    class Params:
        # The readable id and title of what the product sells.
        readable_id = None
        name = None

    price = fg.value("numeric.integer_number", start=200, end=5000)
    text_id = factory.LazyAttribute(lambda row: row.readable_id)
    description = factory.LazyAttribute(lambda row: row.name)
    requires_enrollment_code = False


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
    first_published_at = factory.LazyAttribute(
        lambda row: fg.draw("datetime.datetime", start=2025, end=2025) if row.live else None
    )
    last_published_at = factory.LazyAttribute(
        lambda row: (
            row.first_published_at + datetime.timedelta(days=fg.draw("numeric.integer_number", start=0, end=300))
            if row.live
            else None
        )
    )
    latest_revision_created_at = factory.LazyAttribute(
        lambda row: row.last_published_at or fg.draw("datetime.datetime", start=2026, end=2026)
    )


class CoursePage(CoursePageBase):
    # The hand-written pages leave the language and the JSON body NULL, and
    # nothing reads either.
    language_id = None
    content = None
    description = factory.LazyFunction(lambda: f"<p>{fg.draw('text.sentence')}</p>")
    subhead = fg.value("text.sentence")
    format = factory.Iterator(["Online", "Online", "Hybrid", "Other"])
    min_weeks = _weeks()
    max_weeks = factory.LazyAttribute(lambda row: row.min_weeks)
    duration = factory.LazyAttribute(lambda row: f"{row.min_weeks} weeks")
    min_weekly_hours = _weekly_hours()
    max_weekly_hours = factory.LazyAttribute(lambda row: row.min_weekly_hours + 2)
    time_commitment = factory.LazyAttribute(lambda row: f"{row.min_weekly_hours}-{row.max_weekly_hours} hours per week")
    catalog_details = ""
    external_marketing_url = None


class ProgramPage(ProgramPageBase):
    language_id = None
    content = None
    description = factory.LazyFunction(lambda: f"<p>{fg.draw('text.sentence')}</p>")
    subhead = fg.value("text.sentence")
    # The staging test accepts Online and Other for a program page.
    format = factory.Iterator(["Online", "Online", "Other"])
    min_weeks = _weeks()
    max_weeks = factory.LazyAttribute(lambda row: row.min_weeks * 2)
    duration = factory.LazyAttribute(lambda row: f"{row.max_weeks} weeks")
    min_weekly_hours = _weekly_hours()
    max_weekly_hours = factory.LazyAttribute(lambda row: row.min_weekly_hours + 2)
    time_commitment = factory.LazyAttribute(lambda row: f"{row.min_weekly_hours}-{row.max_weekly_hours} hours per week")
    catalog_details = ""
    external_marketing_url = None


class CertificatePage(CertificatePageBase):
    class Params:
        # The title of the course or program page the certificate is a child of.
        product_title = None

    product_name = factory.LazyAttribute(lambda row: row.product_title)
    ceus = fg.value("numeric.integer_number", start=1, end=20)
    overrides = None
    institute_text = None
    partner_logo_placement = None
    # A StreamField of signatory blocks, as the hand-written rows have it.
    signatories = factory.LazyFunction(
        lambda: [
            {"type": "signatory", "value": fg.draw("numeric.integer_number", start=1, end=9), "id": _block_id()}
            for _ in range(fg.draw("numeric.integer_number", start=1, end=2))
        ]
    )


class FacultyMembersPage(FacultyMembersPageBase):
    heading = "Your instructors"
    subhead = None
    # A StreamField of member blocks: the models read each block's value.name.
    members = factory.LazyFunction(
        lambda: [
            {
                "type": "member",
                "value": {"name": fg.draw("person.full_name"), "image": None, "description": ""},
                "id": _block_id(),
            }
            for _ in range(fg.draw("numeric.integer_number", start=1, end=3))
        ]
    )


def _content_type(hand_written: Mapping[str, list[Row]], model: str) -> Row:
    (content_type,) = (
        row for row in hand_written[ContentTypeBase.raw_table] if (row["app_label"], row["model"]) == ("courses", model)
    )
    return content_type


def _topics(hand_written: Mapping[str, list[Row]], scale: float) -> list[Row]:
    """Half the generated topics at the top level, the other half under a top-level topic."""
    topics = CourseTopic.build_batch(fg.scaled_count(CourseTopic, scale))
    top = topics[: len(topics) // 2]
    top_level = [row for row in hand_written[CourseTopicBase.raw_table] if row.get("parent_id") is None] + top
    return [*top, *attach(topics[len(topics) // 2 :], top_level)]


def _product_pages(
    tree: Tree,
    page_factory: type[CoursePage | ProgramPage],
    products: list[Row],
    *,
    index: Row,
    page_type: Row,
    images: list[Row],
    column: str,
) -> tuple[list[Row], list[Row], list[Row], list[Row]]:
    """Give each of ``products`` a wagtail page under ``index``, its product page, and certificate and faculty pages.

    :returns: The wagtail pages, the product pages, the certificate pages and the faculty pages.
    """
    # One in seven a draft, which the models drop. Seven shares no factor with
    # the course (5) and program (4) live cycles, so some drafts are live courses
    # and programs and the draft filter has rows of its own to drop.
    lives = [number % 7 != 6 for number in range(len(products))]
    wagtail = tree.add(
        [
            Page.build(title=product["title"], slug=_slug(product["readable_id"]), content_type=page_type, live=live)
            for product, live in zip(products, lives, strict=True)
        ],
        under=index,
    )
    thumbnails = fg.pick([*images, None], len(products))
    pages = [
        page_factory.build(page_ptr=page, thumbnail_image=thumbnail, **{column: product})
        for page, product, thumbnail in zip(wagtail, products, thumbnails, strict=True)
    ]
    certificates: list[Row] = []
    faculty: list[Row] = []
    # Two in three get a certificate page and a faculty page as children.
    for number, (page, product) in enumerate(zip(list(wagtail), products, strict=True)):
        if number % 3 == 2:
            continue
        certificate, members = tree.add(
            [
                Page.build(
                    title=f"Certificate for {product['title']}", slug="certificate", content_type=CERTIFICATE_PAGE_TYPE
                ),
                Page.build(title="Faculty", slug="faculty", content_type=FACULTY_PAGE_TYPE),
            ],
            under=page,
        )
        wagtail += [certificate, members]
        certificates.append(CertificatePage.build(page_ptr=certificate, product_title=product["title"]))
        faculty.append(FacultyMembersPage.build(page_ptr=members))
    return wagtail, pages, certificates, faculty


def generate(hand_written: Mapping[str, list[Row]], scale: float) -> dict[str, list[Row]]:
    """Build programs, courses, runs, products and pages around the hand-written rows."""
    platforms = hand_written[PlatformBase.raw_table]
    topics = _topics(hand_written, scale)
    images = Image.build_batch(fg.scaled_count(Image, scale))

    programs = [Program.build(platform=platform) for platform in fg.pick(platforms, fg.scaled_count(Program, scale))]
    # About three courses to a program, and one in four outside any program.
    homes = fg.pick([*programs, *programs, *programs, None], fg.scaled_count(Course, scale))
    positions: dict[int, int] = {}
    courses = []
    for program in homes:
        if program is None:
            courses.append(Course.build(platform=fg.pick(platforms, 1)[0]))
            continue
        positions[program["id"]] = positions.get(program["id"], 0) + 1
        courses.append(
            Course.build(
                program=program, platform={"id": program["platform_id"]}, position_in_program=positions[program["id"]]
            )
        )

    runs = [
        CourseRun.build(course=course, is_open=number == 0) for course in courses for number in range(RUNS_PER_COURSE)
    ]

    # Most runs and programs have a product: one without is not listed (a run)
    # or not published (a program).
    run_type, program_type = _content_type(hand_written, "courserun"), _content_type(hand_written, "program")
    sold_runs = fg.pair(runs, round(len(runs) * 0.8))
    sold_programs = fg.pair(programs, len(programs) - max(1, len(programs) // 10))
    products = [Product.build(content_type=run_type, target=run) for run in sold_runs] + [
        Product.build(content_type=program_type, target=program) for program in sold_programs
    ]
    sold = [*sold_runs, *sold_programs]
    versions = [
        ProductVersion.build(
            product=product,
            # A run's readable id is its courseware id.
            readable_id=target["courseware_id"] if "courseware_id" in target else target["readable_id"],
            name=target["title"],
        )
        for product, target in zip(products, sold, strict=True)
        # A repriced product has two versions; its newer one is the price.
        for _ in range(1 + (product["id"] % 4 == 0))
    ]

    tree = Tree(hand_written[PageBase.raw_table], alphabet=TREE_ALPHABETS[PageBase.raw_table])
    course_wagtail, course_pages, course_certificates, course_faculty = _product_pages(
        tree, CoursePage, courses, index=COURSES_INDEX, page_type=COURSE_PAGE_TYPE, images=images, column="course"
    )
    program_wagtail, program_pages, program_certificates, program_faculty = _product_pages(
        tree, ProgramPage, programs, index=PROGRAMS_INDEX, page_type=PROGRAM_PAGE_TYPE, images=images, column="program"
    )
    page_topics = fg.link(
        course_pages,
        [*hand_written[CourseTopicBase.raw_table], *topics],
        min(len(course_pages) * TOPICS_PER_COURSE_PAGE, len(course_pages) * (len(topics) + 3)),
    )

    return {
        CourseTopicBase.raw_table: topics,
        ImageBase.raw_table: images,
        ProgramBase.raw_table: programs,
        CourseBase.raw_table: courses,
        CourseRunBase.raw_table: runs,
        ProductBase.raw_table: products,
        ProductVersionBase.raw_table: versions,
        PageBase.raw_table: course_wagtail + program_wagtail,
        CoursePageBase.raw_table: course_pages,
        ProgramPageBase.raw_table: program_pages,
        CertificatePageBase.raw_table: course_certificates + program_certificates,
        FacultyMembersPageBase.raw_table: course_faculty + program_faculty,
        CoursePageTopicsBase.raw_table: [
            CoursePageTopicsBase.build(coursepage=page, coursetopic=topic) for page, topic in page_topics
        ],
    }
