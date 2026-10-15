"""Base factories of the `mitxonline/app_postgres` unit, scaffolded from its models.

From mitxonline at git:47ad6f2e9b2e7f3bd3c9cf60102754d8de3410d7. Written by `ol-dbt fixtures scaffold`
from models.json and rewritten wholesale on every run: change a factory by
subclassing it in __init__.py.
"""

from __future__ import annotations

import factory

from ol_dbt_cli.lib.fixture_generation import RowFactory, or_null, parent, table, url, value

SCAFFOLD_DEFAULT_COUNT = 10

COURSE_RUN_LANGUAGE_CHOICES = [
    "af_ZA",
    "ar",
    "az",
    "bo",
    "da",
    "de",
    "de_DE",
    "el",
    "es_419",
    "es_ES",
    "en",
    "fa",
    "fr",
    "fr_CA",
    "he",
    "hi",
    "hu",
    "id",
    "it_IT",
    "ja",
    "ka",
    "kk",
    "ko",
    "lv",
    "nl",
    "pl",
    "pt_BR",
    "pt_PT",
    "ro",
    "ru",
    "sq",
    "sv",
    "sw",
    "te",
    "th",
    "tr_TR",
    "uk",
    "uz",
    "vi",
    "zh_CN",
    "zh_HANS",
    "zh_HK",
]

SUPPORTED_VARIANT_LANGUAGE_CHOICES = [
    "af_ZA",
    "ar",
    "az",
    "bo",
    "da",
    "de",
    "de_DE",
    "el",
    "es_419",
    "es_ES",
    "en",
    "fa",
    "fr",
    "fr_CA",
    "he",
    "hi",
    "hu",
    "id",
    "it_IT",
    "ja",
    "ka",
    "kk",
    "ko",
    "lv",
    "nl",
    "pl",
    "pt_BR",
    "pt_PT",
    "ro",
    "ru",
    "sq",
    "sv",
    "sw",
    "te",
    "th",
    "tr_TR",
    "uk",
    "uz",
    "vi",
    "zh_CN",
    "zh_HANS",
    "zh_HK",
]


@table("raw__mitxonline__app__postgres__courses_course", default_count=SCAFFOLD_DEFAULT_COUNT)
class CourseBase(RowFactory):
    """Table courses_course, from model courses.Course."""

    id = factory.Sequence(lambda n: n)
    live = value("development.boolean")
    title = value("text.title")
    created_on = value("datetime.datetime", start=2020, end=2025)
    updated_on = value("datetime.datetime", start=2020, end=2025)
    readable_id = factory.Sequence(lambda n: f"readable-{n}")
    certificates_disabled = value("development.boolean")


@table("raw__mitxonline__app__postgres__courses_courserun", default_count=SCAFFOLD_DEFAULT_COUNT)
class CourseRunBase(RowFactory):
    """Table courses_courserun, from model courses.CourseRun."""

    # Unique together: course_id, courseware_id, run_tag. generate() keeps them distinct.
    # b2b_contract_id: references b2b_contractpage, which the fixture has no table for; NULL.

    class Params:
        course = None

    id = factory.Sequence(lambda n: n)
    live = value("development.boolean")
    title = value("text.title")
    run_tag = value("text.sentence")
    end_date = or_null(value("datetime.datetime", start=2020, end=2025))
    course_id = parent("course", "id")
    created_on = value("datetime.datetime", start=2020, end=2025)
    start_date = or_null(value("datetime.datetime", start=2020, end=2025))
    updated_on = value("datetime.datetime", start=2020, end=2025)
    courseware_id = factory.Sequence(lambda n: f"courseware-{n}")
    is_self_paced = value("development.boolean")
    enrollment_end = or_null(value("datetime.datetime", start=2020, end=2025))
    expiration_date = or_null(value("datetime.datetime", start=2020, end=2025))
    enrollment_start = or_null(value("datetime.datetime", start=2020, end=2025))
    upgrade_deadline = or_null(value("datetime.datetime", start=2020, end=2025))
    certificate_available_date = or_null(value("datetime.datetime", start=2020, end=2025))
    is_source_run = value("development.boolean")
    has_courseware_url = value("development.boolean")
    language = factory.Iterator(COURSE_RUN_LANGUAGE_CHOICES)
    is_primary_language = value("development.boolean")
    variant_length = factory.Iterator(["", "S"])
    variant_industry = factory.Iterator(["", "E", "F", "HC", "M"])
    b2b_only = value("development.boolean")
    courseware_url_path = value("text.word")


@table("raw__mitxonline__app__postgres__courses_coursestopic", default_count=SCAFFOLD_DEFAULT_COUNT)
class CoursesTopicBase(RowFactory):
    """Table courses_coursestopic, from model courses.CoursesTopic."""

    # Unique together: name, parent_id. generate() keeps them distinct.
    # parent_id: references this table; NULL unless generate() sets it with attach().

    id = factory.Sequence(lambda n: n)
    name = value("text.title")
    created_on = value("datetime.datetime", start=2020, end=2025)
    updated_on = value("datetime.datetime", start=2020, end=2025)


@table("raw__mitxonline__app__postgres__courses_program", default_count=SCAFFOLD_DEFAULT_COUNT)
class ProgramBase(RowFactory):
    """Table courses_program, from model courses.Program."""

    id = factory.Sequence(lambda n: n)
    live = value("development.boolean")
    title = value("text.title")
    end_date = or_null(value("datetime.datetime", start=2020, end=2025))
    created_on = value("datetime.datetime", start=2020, end=2025)
    start_date = or_null(value("datetime.datetime", start=2020, end=2025))
    updated_on = value("datetime.datetime", start=2020, end=2025)
    readable_id = factory.Sequence(lambda n: f"readable-{n}")
    availability = factory.Iterator(["anytime", "dated"])
    program_type = or_null(value("text.word"))
    enrollment_end = or_null(value("datetime.datetime", start=2020, end=2025))
    enrollment_start = or_null(value("datetime.datetime", start=2020, end=2025))
    b2b_only = value("development.boolean")
    display_mode = or_null(factory.Iterator(["course"]))
    certificates_disabled = value("development.boolean")


@table("raw__mitxonline__app__postgres__courses_programrequirement", default_count=SCAFFOLD_DEFAULT_COUNT)
class ProgramRequirementBase(RowFactory):
    """Table courses_programrequirement, from model courses.ProgramRequirement."""

    # A treebeard tree: place rows with Tree(rows, alphabet=TREE_ALPHABETS[ProgramRequirementBase.raw_table]).
    # path: set by Tree.add.
    # depth: set by Tree.add.
    # numchild: set by Tree.add.

    class Params:
        course = None
        program = None
        required_program = None

    id = factory.Sequence(lambda n: n)
    title = or_null(value("text.title"))
    operator = or_null(factory.Iterator(["all_of", "min_number_of"]))
    course_id = parent("course", "id")
    node_type = or_null(factory.Iterator(["program_root", "operator", "course", "program", "track"]))
    program_id = parent("program", "id")
    elective_flag = or_null(value("development.boolean"))
    operator_value = or_null(value("text.word"))
    required_program_id = parent("required_program", "id")


@table("raw__mitxonline__app__postgres__wagtailimages_image", default_count=SCAFFOLD_DEFAULT_COUNT)
class ImageBase(RowFactory):
    """Table wagtailimages_image, from model wagtailimages.Image."""

    # collection_id: references wagtailcore_collection, which the fixture has no table for.
    # uploaded_by_user_id: references users_user, which the fixture has no table for; NULL.

    class Params:
        collection = None

    id = factory.Sequence(lambda n: n)
    file = value("file.file_name")
    title = value("text.title")
    width = value("numeric.integer_number", start=0, end=1000)
    height = value("numeric.integer_number", start=0, end=1000)
    file_hash = value("text.word")
    file_size = or_null(value("numeric.integer_number", start=0, end=1000))
    created_at = value("datetime.datetime", start=2020, end=2025)
    description = value("text.sentence")
    collection_id = parent("collection", "id")
    focal_point_x = or_null(value("numeric.integer_number", start=0, end=1000))
    focal_point_y = or_null(value("numeric.integer_number", start=0, end=1000))
    focal_point_width = or_null(value("numeric.integer_number", start=0, end=1000))
    focal_point_height = or_null(value("numeric.integer_number", start=0, end=1000))


@table("raw__mitxonline__app__postgres__courses_department", default_count=SCAFFOLD_DEFAULT_COUNT)
class DepartmentBase(RowFactory):
    """Table courses_department, from model courses.Department."""

    id = factory.Sequence(lambda n: n)
    name = factory.Sequence(lambda n: f"name-{n}")
    slug = factory.Sequence(lambda n: f"slug-{n}")
    created_on = value("datetime.datetime", start=2020, end=2025)
    updated_on = value("datetime.datetime", start=2020, end=2025)


@table("raw__mitxonline__app__postgres__courses_course_departments", default_count=SCAFFOLD_DEFAULT_COUNT)
class CourseDepartmentsBase(RowFactory):
    """Table courses_course_departments, from model courses.Course_departments."""

    # Unique together: course_id, department_id. generate() keeps them distinct.

    class Params:
        course = None
        department = None

    id = factory.Sequence(lambda n: n)
    course_id = parent("course", "id")
    department_id = parent("department", "id")


@table("raw__mitxonline__app__postgres__courses_enrollmentmode", default_count=SCAFFOLD_DEFAULT_COUNT)
class EnrollmentModeBase(RowFactory):
    """Table courses_enrollmentmode, from model courses.EnrollmentMode."""

    id = factory.Sequence(lambda n: n)
    mode_slug = factory.Sequence(lambda n: f"mode-slug-{n}")
    requires_payment = value("development.boolean")
    mode_display_name = value("text.title")


@table("raw__mitxonline__app__postgres__courses_courserun_enrollment_modes", default_count=SCAFFOLD_DEFAULT_COUNT)
class CourseRunEnrollmentModesBase(RowFactory):
    """Table courses_courserun_enrollment_modes, from model courses.CourseRun_enrollment_modes."""

    # Unique together: courserun_id, enrollmentmode_id. generate() keeps them distinct.

    class Params:
        courserun = None
        enrollmentmode = None

    id = factory.Sequence(lambda n: n)
    courserun_id = parent("courserun", "id")
    enrollmentmode_id = parent("enrollmentmode", "id")


@table("raw__mitxonline__app__postgres__courses_program_departments", default_count=SCAFFOLD_DEFAULT_COUNT)
class ProgramDepartmentsBase(RowFactory):
    """Table courses_program_departments, from model courses.Program_departments."""

    # Unique together: program_id, department_id. generate() keeps them distinct.

    class Params:
        program = None
        department = None

    id = factory.Sequence(lambda n: n)
    program_id = parent("program", "id")
    department_id = parent("department", "id")


@table("raw__mitxonline__app__postgres__courses_program_enrollment_modes", default_count=SCAFFOLD_DEFAULT_COUNT)
class ProgramEnrollmentModesBase(RowFactory):
    """Table courses_program_enrollment_modes, from model courses.Program_enrollment_modes."""

    # Unique together: program_id, enrollmentmode_id. generate() keeps them distinct.

    class Params:
        program = None
        enrollmentmode = None

    id = factory.Sequence(lambda n: n)
    program_id = parent("program", "id")
    enrollmentmode_id = parent("enrollmentmode", "id")


@table("raw__mitxonline__app__postgres__django_content_type", default_count=SCAFFOLD_DEFAULT_COUNT)
class ContentTypeBase(RowFactory):
    """Table django_content_type, from model contenttypes.ContentType."""

    # Unique together: app_label, model. generate() keeps them distinct.

    id = factory.Sequence(lambda n: n)
    model = value("text.word")
    app_label = value("text.word")


@table("raw__mitxonline__app__postgres__wagtailcore_page", default_count=SCAFFOLD_DEFAULT_COUNT)
class PageBase(RowFactory):
    """Table wagtailcore_page, from model wagtailcore.Page."""

    # Unique together: translation_key, locale_id. generate() keeps them distinct.
    # A treebeard tree: place rows with Tree(rows, alphabet=TREE_ALPHABETS[PageBase.raw_table]).
    # path: set by Tree.add.
    # depth: set by Tree.add.
    # numchild: set by Tree.add.
    # owner_id: references users_user, which the fixture has no table for; NULL.
    # url_path: set by Tree.add.
    # locale_id: references wagtailcore_locale, which the fixture has no table for.
    # alias_of_id: references this table; NULL unless generate() sets it with attach().
    # locked_by_id: references users_user, which the fixture has no table for; NULL.
    # live_revision_id: references wagtailcore_revision, which the fixture has no table for; NULL.
    # latest_revision_id: references wagtailcore_revision, which the fixture has no table for; NULL.

    class Params:
        locale = None
        content_type = None

    id = factory.Sequence(lambda n: n)
    live = value("development.boolean")
    slug = value("internet.slug")
    title = value("text.title")
    locked = value("development.boolean")
    expired = value("development.boolean")
    expire_at = or_null(value("datetime.datetime", start=2020, end=2025))
    locale_id = parent("locale", "id")
    locked_at = or_null(value("datetime.datetime", start=2020, end=2025))
    seo_title = value("text.title")
    go_live_at = or_null(value("datetime.datetime", start=2020, end=2025))
    draft_title = value("text.title")
    show_in_menus = value("development.boolean")
    content_type_id = parent("content_type", "id")
    translation_key = value("cryptographic.uuid")
    last_published_at = or_null(value("datetime.datetime", start=2020, end=2025))
    first_published_at = or_null(value("datetime.datetime", start=2020, end=2025))
    search_description = value("text.sentence")
    has_unpublished_changes = value("development.boolean")
    latest_revision_created_at = or_null(value("datetime.datetime", start=2020, end=2025))


@table("raw__mitxonline__app__postgres__cms_coursepage", default_count=SCAFFOLD_DEFAULT_COUNT)
class CoursePageBase(RowFactory):
    """Table cms_coursepage, from model cms.CoursePage."""

    # price: JSON whose shape the model does not say; set it in __init__.py.
    # course_id: one-to-one with courses_course; draw its parents with pair().
    # page_ptr_id: one-to-one with wagtailcore_page; draw its parents with pair().

    class Params:
        course = None
        page_ptr = None
        search_image = None
        feature_image = None

    about = or_null(value("text.sentence"))
    effort = or_null(value("text.word"))
    length = value("text.word")
    faq_url = or_null(url())
    course_id = parent("course", "id")
    max_weeks = or_null(value("numeric.integer_number", start=0, end=1000))
    min_weeks = or_null(value("numeric.integer_number", start=0, end=1000))
    video_url = or_null(url())
    description = value("text.sentence")
    page_ptr_id = parent("page_ptr", "id")
    prerequisites = or_null(value("text.sentence"))
    what_you_learn = or_null(value("text.sentence"))
    search_image_id = parent("search_image", "id")
    feature_image_id = parent("feature_image", "id")
    max_weekly_hours = value("text.word")
    min_weekly_hours = value("text.word")
    faculty_section_title = or_null(value("text.title"))
    include_in_learn_catalog = or_null(value("development.boolean"))
    ingest_content_files_for_ai = or_null(value("development.boolean"))
    hyl_choice_learn_by_doing = or_null(value("development.boolean"))
    hyl_choice_learn_on_demand = or_null(value("development.boolean"))
    hyl_choice_learn_from_others = or_null(value("development.boolean"))
    hyl_choice_ai_enabled_support = or_null(value("development.boolean"))
    hyl_choice_realworld_learning = or_null(value("development.boolean"))
    hyl_choice_stackable_credentials = or_null(value("development.boolean"))
    max_price = value("numeric.decimal_number", start=0, end=999999)
    min_price = value("numeric.decimal_number", start=0, end=999999)
    hubspot_form_id = value("text.word")
    show_course_outline = value("development.boolean")
    show_stay_updated = value("development.boolean")


@table("raw__mitxonline__app__postgres__cms_coursepage_topics", default_count=SCAFFOLD_DEFAULT_COUNT)
class CoursePageTopicsBase(RowFactory):
    """Table cms_coursepage_topics, from model cms.CoursePage_topics."""

    # Unique together: coursepage_id, coursestopic_id. generate() keeps them distinct.

    class Params:
        coursepage = None
        coursestopic = None

    id = factory.Sequence(lambda n: n)
    coursepage_id = parent("coursepage", "page_ptr_id")
    coursestopic_id = parent("coursestopic", "id")


@table("raw__mitxonline__app__postgres__cms_instructorpage", default_count=SCAFFOLD_DEFAULT_COUNT)
class InstructorPageBase(RowFactory):
    """Table cms_instructorpage, from model cms.InstructorPage."""

    # page_ptr_id: one-to-one with wagtailcore_page; draw its parents with pair().

    class Params:
        page_ptr = None
        feature_image = None

    page_ptr_id = parent("page_ptr", "id")
    instructor_name = value("text.title")
    feature_image_id = parent("feature_image", "id")
    instructor_title = or_null(value("text.title"))
    instructor_bio_long = or_null(value("text.sentence"))
    instructor_bio_short = or_null(value("text.sentence"))


@table("raw__mitxonline__app__postgres__cms_instructorpagelink", default_count=SCAFFOLD_DEFAULT_COUNT)
class InstructorPageLinkBase(RowFactory):
    """Table cms_instructorpagelink, from model cms.InstructorPageLink."""

    class Params:
        page = None
        linked_instructor_page = None

    id = factory.Sequence(lambda n: n)
    order = or_null(value("numeric.integer_number", start=0, end=1000))
    page_id = parent("page", "id")
    linked_instructor_page_id = parent("linked_instructor_page", "page_ptr_id")


@table("raw__mitxonline__app__postgres__cms_programpage", default_count=SCAFFOLD_DEFAULT_COUNT)
class ProgramPageBase(RowFactory):
    """Table cms_programpage, from model cms.ProgramPage."""

    # price: JSON whose shape the model does not say; set it in __init__.py.
    # program_id: one-to-one with courses_program; draw its parents with pair().
    # page_ptr_id: one-to-one with wagtailcore_page; draw its parents with pair().

    class Params:
        program = None
        page_ptr = None
        search_image = None
        feature_image = None

    about = or_null(value("text.sentence"))
    effort = or_null(value("text.word"))
    length = value("text.word")
    faq_url = or_null(url())
    max_weeks = or_null(value("numeric.integer_number", start=0, end=1000))
    min_weeks = or_null(value("numeric.integer_number", start=0, end=1000))
    video_url = or_null(url())
    program_id = parent("program", "id")
    description = or_null(value("text.sentence"))
    page_ptr_id = parent("page_ptr", "id")
    prerequisites = or_null(value("text.sentence"))
    what_you_learn = or_null(value("text.sentence"))
    search_image_id = parent("search_image", "id")
    feature_image_id = parent("feature_image", "id")
    max_weekly_hours = value("text.word")
    min_weekly_hours = value("text.word")
    faculty_section_title = or_null(value("text.title"))
    include_in_learn_catalog = or_null(value("development.boolean"))
    hyl_choice_learn_by_doing = or_null(value("development.boolean"))
    hyl_choice_learn_on_demand = or_null(value("development.boolean"))
    hyl_choice_learn_from_others = or_null(value("development.boolean"))
    hyl_choice_ai_enabled_support = or_null(value("development.boolean"))
    hyl_choice_realworld_learning = or_null(value("development.boolean"))
    hyl_choice_stackable_credentials = or_null(value("development.boolean"))
    list_price = or_null(value("numeric.decimal_number", start=0, end=999999))
    max_price = value("numeric.decimal_number", start=0, end=999999)
    min_price = value("numeric.decimal_number", start=0, end=999999)
    hubspot_form_id = value("text.word")
    show_stay_updated = value("development.boolean")


@table("raw__mitxonline__app__postgres__variants_supportedvariant", default_count=SCAFFOLD_DEFAULT_COUNT)
class SupportedVariantBase(RowFactory):
    """Table variants_supportedvariant, from model variants.SupportedVariant."""

    # object_id: the object id of generic foreign key variant_object, set in generate() with content_type_id.

    class Params:
        content_type = None

    id = factory.Sequence(lambda n: n)
    active = value("development.boolean")
    b2b_only = value("development.boolean")
    language = factory.Iterator(SUPPORTED_VARIANT_LANGUAGE_CHOICES)
    created_on = value("datetime.datetime", start=2020, end=2025)
    updated_on = value("datetime.datetime", start=2020, end=2025)
    variant_length = factory.Iterator(["", "S"])
    content_type_id = parent("content_type", "id")
    default_variant = value("development.boolean")
    variant_industry = factory.Iterator(["", "E", "F", "HC", "M"])


# Each tree's MP_Node.alphabet, for fixture_trees.Tree.
TREE_ALPHABETS = {
    ProgramRequirementBase.raw_table: "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
    PageBase.raw_table: "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ",
}
