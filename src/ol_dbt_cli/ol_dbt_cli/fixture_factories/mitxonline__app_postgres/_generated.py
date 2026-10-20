"""Base factories of the `mitxonline/app_postgres` unit, scaffolded from its models.

From mitxonline at git:47ad6f2e9b2e7f3bd3c9cf60102754d8de3410d7. Written by `ol-dbt fixtures scaffold` from models.json
and rewritten wholesale on every run: change a factory by subclassing it in __init__.py.
"""

from __future__ import annotations

import factory

from ol_dbt_cli.lib import fixture_generation as fg

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


@fg.table("raw__mitxonline__app__postgres__courses_course", default_count=SCAFFOLD_DEFAULT_COUNT)
class CourseBase(fg.RowFactory):
    """Table courses_course, from model courses.Course."""

    id = factory.Sequence(lambda n: n)
    live = fg.value("development.boolean")
    title = fg.value("text.title")
    created_on = fg.value("datetime.datetime", start=2020, end=2025)
    updated_on = fg.value("datetime.datetime", start=2020, end=2025)
    readable_id = factory.Sequence(lambda n: f"readable-{n}")
    certificates_disabled = fg.value("development.boolean")


@fg.table("raw__mitxonline__app__postgres__courses_courserun", default_count=SCAFFOLD_DEFAULT_COUNT)
class CourseRunBase(fg.RowFactory):
    """Table courses_courserun, from model courses.CourseRun."""

    # Unique together: course_id, courseware_id, run_tag. generate() must keep them distinct.
    # b2b_contract_id: references b2b_contractpage, which the fixture has no table for; NULL.

    class Params:
        course = None

    id = factory.Sequence(lambda n: n)
    live = fg.value("development.boolean")
    title = fg.value("text.title")
    run_tag = fg.value("text.sentence")
    end_date = fg.or_null(fg.value("datetime.datetime", start=2020, end=2025))
    course_id = fg.parent("course", "id")
    created_on = fg.value("datetime.datetime", start=2020, end=2025)
    start_date = fg.or_null(fg.value("datetime.datetime", start=2020, end=2025))
    updated_on = fg.value("datetime.datetime", start=2020, end=2025)
    courseware_id = factory.Sequence(lambda n: f"courseware-{n}")
    is_self_paced = fg.value("development.boolean")
    enrollment_end = fg.or_null(fg.value("datetime.datetime", start=2020, end=2025))
    expiration_date = fg.or_null(fg.value("datetime.datetime", start=2020, end=2025))
    enrollment_start = fg.or_null(fg.value("datetime.datetime", start=2020, end=2025))
    upgrade_deadline = fg.or_null(fg.value("datetime.datetime", start=2020, end=2025))
    certificate_available_date = fg.or_null(fg.value("datetime.datetime", start=2020, end=2025))
    is_source_run = fg.value("development.boolean")
    has_courseware_url = fg.value("development.boolean")
    language = factory.Iterator(COURSE_RUN_LANGUAGE_CHOICES)
    is_primary_language = fg.value("development.boolean")
    variant_length = factory.Iterator(["", "S"])
    variant_industry = factory.Iterator(["", "E", "F", "HC", "M"])
    b2b_only = fg.value("development.boolean")
    courseware_url_path = fg.value("text.word")


@fg.table("raw__mitxonline__app__postgres__courses_coursestopic", default_count=SCAFFOLD_DEFAULT_COUNT)
class CoursesTopicBase(fg.RowFactory):
    """Table courses_coursestopic, from model courses.CoursesTopic."""

    # Unique together: name, parent_id. generate() must keep them distinct.
    # parent_id: references this table; NULL unless generate() sets it with attach().

    id = factory.Sequence(lambda n: n)
    name = fg.value("text.title")
    created_on = fg.value("datetime.datetime", start=2020, end=2025)
    updated_on = fg.value("datetime.datetime", start=2020, end=2025)


@fg.table("raw__mitxonline__app__postgres__courses_program", default_count=SCAFFOLD_DEFAULT_COUNT)
class ProgramBase(fg.RowFactory):
    """Table courses_program, from model courses.Program."""

    id = factory.Sequence(lambda n: n)
    live = fg.value("development.boolean")
    title = fg.value("text.title")
    end_date = fg.or_null(fg.value("datetime.datetime", start=2020, end=2025))
    created_on = fg.value("datetime.datetime", start=2020, end=2025)
    start_date = fg.or_null(fg.value("datetime.datetime", start=2020, end=2025))
    updated_on = fg.value("datetime.datetime", start=2020, end=2025)
    readable_id = factory.Sequence(lambda n: f"readable-{n}")
    availability = factory.Iterator(["anytime", "dated"])
    program_type = fg.or_null(fg.value("text.word"))
    enrollment_end = fg.or_null(fg.value("datetime.datetime", start=2020, end=2025))
    enrollment_start = fg.or_null(fg.value("datetime.datetime", start=2020, end=2025))
    b2b_only = fg.value("development.boolean")
    display_mode = fg.or_null(factory.Iterator(["course"]))
    certificates_disabled = fg.value("development.boolean")


@fg.table("raw__mitxonline__app__postgres__courses_programrequirement", default_count=SCAFFOLD_DEFAULT_COUNT)
class ProgramRequirementBase(fg.RowFactory):
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
    title = fg.or_null(fg.value("text.title"))
    operator = fg.or_null(factory.Iterator(["all_of", "min_number_of"]))
    course_id = fg.parent("course", "id", nullable=True)
    node_type = fg.or_null(factory.Iterator(["program_root", "operator", "course", "program", "track"]))
    program_id = fg.parent("program", "id")
    elective_flag = fg.or_null(fg.value("development.boolean"))
    operator_value = fg.or_null(fg.value("text.word"))
    required_program_id = fg.parent("required_program", "id", nullable=True)


@fg.table("raw__mitxonline__app__postgres__wagtailimages_image", default_count=SCAFFOLD_DEFAULT_COUNT)
class ImageBase(fg.RowFactory):
    """Table wagtailimages_image, from model wagtailimages.Image."""

    # collection_id: references wagtailcore_collection, which the fixture has no table for.
    # uploaded_by_user_id: references users_user, which the fixture has no table for; NULL.

    class Params:
        collection = None

    id = factory.Sequence(lambda n: n)
    file = fg.value("file.file_name")
    title = fg.value("text.title")
    width = fg.value("numeric.integer_number", start=0, end=1000)
    height = fg.value("numeric.integer_number", start=0, end=1000)
    file_hash = fg.value("text.word")
    file_size = fg.or_null(fg.value("numeric.integer_number", start=0, end=1000))
    created_at = fg.value("datetime.datetime", start=2020, end=2025)
    description = fg.value("text.sentence")
    collection_id = fg.parent("collection", "id")
    focal_point_x = fg.or_null(fg.value("numeric.integer_number", start=0, end=1000))
    focal_point_y = fg.or_null(fg.value("numeric.integer_number", start=0, end=1000))
    focal_point_width = fg.or_null(fg.value("numeric.integer_number", start=0, end=1000))
    focal_point_height = fg.or_null(fg.value("numeric.integer_number", start=0, end=1000))


@fg.table("raw__mitxonline__app__postgres__courses_department", default_count=SCAFFOLD_DEFAULT_COUNT)
class DepartmentBase(fg.RowFactory):
    """Table courses_department, from model courses.Department."""

    id = factory.Sequence(lambda n: n)
    name = factory.Sequence(lambda n: f"name-{n}")
    slug = factory.Sequence(lambda n: f"slug-{n}")
    created_on = fg.value("datetime.datetime", start=2020, end=2025)
    updated_on = fg.value("datetime.datetime", start=2020, end=2025)


@fg.table("raw__mitxonline__app__postgres__courses_course_departments", default_count=SCAFFOLD_DEFAULT_COUNT)
class CourseDepartmentsBase(fg.RowFactory):
    """Table courses_course_departments, from model courses.Course_departments."""

    # Unique together: course_id, department_id. generate() must keep them distinct.

    class Params:
        course = None
        department = None

    id = factory.Sequence(lambda n: n)
    course_id = fg.parent("course", "id")
    department_id = fg.parent("department", "id")


@fg.table("raw__mitxonline__app__postgres__courses_enrollmentmode", default_count=SCAFFOLD_DEFAULT_COUNT)
class EnrollmentModeBase(fg.RowFactory):
    """Table courses_enrollmentmode, from model courses.EnrollmentMode."""

    id = factory.Sequence(lambda n: n)
    mode_slug = factory.Sequence(lambda n: f"mode-slug-{n}")
    requires_payment = fg.value("development.boolean")
    mode_display_name = fg.value("text.title")


@fg.table("raw__mitxonline__app__postgres__courses_courserun_enrollment_modes", default_count=SCAFFOLD_DEFAULT_COUNT)
class CourseRunEnrollmentModesBase(fg.RowFactory):
    """Table courses_courserun_enrollment_modes, from model courses.CourseRun_enrollment_modes."""

    # Unique together: courserun_id, enrollmentmode_id. generate() must keep them distinct.

    class Params:
        courserun = None
        enrollmentmode = None

    id = factory.Sequence(lambda n: n)
    courserun_id = fg.parent("courserun", "id")
    enrollmentmode_id = fg.parent("enrollmentmode", "id")


@fg.table("raw__mitxonline__app__postgres__courses_program_departments", default_count=SCAFFOLD_DEFAULT_COUNT)
class ProgramDepartmentsBase(fg.RowFactory):
    """Table courses_program_departments, from model courses.Program_departments."""

    # Unique together: program_id, department_id. generate() must keep them distinct.

    class Params:
        program = None
        department = None

    id = factory.Sequence(lambda n: n)
    program_id = fg.parent("program", "id")
    department_id = fg.parent("department", "id")


@fg.table("raw__mitxonline__app__postgres__courses_program_enrollment_modes", default_count=SCAFFOLD_DEFAULT_COUNT)
class ProgramEnrollmentModesBase(fg.RowFactory):
    """Table courses_program_enrollment_modes, from model courses.Program_enrollment_modes."""

    # Unique together: program_id, enrollmentmode_id. generate() must keep them distinct.

    class Params:
        program = None
        enrollmentmode = None

    id = factory.Sequence(lambda n: n)
    program_id = fg.parent("program", "id")
    enrollmentmode_id = fg.parent("enrollmentmode", "id")


@fg.table("raw__mitxonline__app__postgres__django_content_type", default_count=SCAFFOLD_DEFAULT_COUNT)
class ContentTypeBase(fg.RowFactory):
    """Table django_content_type, from model contenttypes.ContentType."""

    # Unique together: app_label, model. generate() must keep them distinct.

    id = factory.Sequence(lambda n: n)
    model = fg.value("text.word")
    app_label = fg.value("text.word")


@fg.table("raw__mitxonline__app__postgres__wagtailcore_page", default_count=SCAFFOLD_DEFAULT_COUNT)
class PageBase(fg.RowFactory):
    """Table wagtailcore_page, from model wagtailcore.Page."""

    # Unique together: translation_key, locale_id. generate() must keep them distinct.
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
    live = fg.value("development.boolean")
    slug = fg.value("internet.slug")
    title = fg.value("text.title")
    locked = fg.value("development.boolean")
    expired = fg.value("development.boolean")
    expire_at = fg.or_null(fg.value("datetime.datetime", start=2020, end=2025))
    locale_id = fg.parent("locale", "id")
    locked_at = fg.or_null(fg.value("datetime.datetime", start=2020, end=2025))
    seo_title = fg.value("text.title")
    go_live_at = fg.or_null(fg.value("datetime.datetime", start=2020, end=2025))
    draft_title = fg.value("text.title")
    show_in_menus = fg.value("development.boolean")
    content_type_id = fg.parent("content_type", "id")
    translation_key = fg.value("cryptographic.uuid")
    last_published_at = fg.or_null(fg.value("datetime.datetime", start=2020, end=2025))
    first_published_at = fg.or_null(fg.value("datetime.datetime", start=2020, end=2025))
    search_description = fg.value("text.sentence")
    has_unpublished_changes = fg.value("development.boolean")
    latest_revision_created_at = fg.or_null(fg.value("datetime.datetime", start=2020, end=2025))


@fg.table("raw__mitxonline__app__postgres__cms_coursepage", default_count=SCAFFOLD_DEFAULT_COUNT)
class CoursePageBase(fg.RowFactory):
    """Table cms_coursepage, from model cms.CoursePage."""

    # price: JSON whose shape the model does not say; set it in __init__.py.
    # course_id: one-to-one with courses_course; draw its parents with pair().
    # page_ptr_id: one-to-one with wagtailcore_page; draw its parents with pair().

    class Params:
        course = None
        page_ptr = None
        search_image = None
        feature_image = None

    about = fg.or_null(fg.value("text.sentence"))
    effort = fg.or_null(fg.value("text.word"))
    length = fg.value("text.word")
    faq_url = fg.or_null(fg.url())
    course_id = fg.parent("course", "id", nullable=True)
    max_weeks = fg.or_null(fg.value("numeric.integer_number", start=0, end=1000))
    min_weeks = fg.or_null(fg.value("numeric.integer_number", start=0, end=1000))
    video_url = fg.or_null(fg.url())
    description = fg.value("text.sentence")
    page_ptr_id = fg.parent("page_ptr", "id")
    prerequisites = fg.or_null(fg.value("text.sentence"))
    what_you_learn = fg.or_null(fg.value("text.sentence"))
    search_image_id = fg.parent("search_image", "id", nullable=True)
    feature_image_id = fg.parent("feature_image", "id", nullable=True)
    max_weekly_hours = fg.value("numeric.integer_number", start=0, end=9999)
    min_weekly_hours = fg.value("numeric.integer_number", start=0, end=9999)
    faculty_section_title = fg.or_null(fg.value("text.title"))
    include_in_learn_catalog = fg.or_null(fg.value("development.boolean"))
    ingest_content_files_for_ai = fg.or_null(fg.value("development.boolean"))
    hyl_choice_learn_by_doing = fg.or_null(fg.value("development.boolean"))
    hyl_choice_learn_on_demand = fg.or_null(fg.value("development.boolean"))
    hyl_choice_learn_from_others = fg.or_null(fg.value("development.boolean"))
    hyl_choice_ai_enabled_support = fg.or_null(fg.value("development.boolean"))
    hyl_choice_realworld_learning = fg.or_null(fg.value("development.boolean"))
    hyl_choice_stackable_credentials = fg.or_null(fg.value("development.boolean"))
    max_price = fg.value("numeric.decimal_number", start=0, end=999999)
    min_price = fg.value("numeric.decimal_number", start=0, end=999999)
    hubspot_form_id = fg.value("text.word")
    show_course_outline = fg.value("development.boolean")
    show_stay_updated = fg.value("development.boolean")


@fg.table("raw__mitxonline__app__postgres__cms_coursepage_topics", default_count=SCAFFOLD_DEFAULT_COUNT)
class CoursePageTopicsBase(fg.RowFactory):
    """Table cms_coursepage_topics, from model cms.CoursePage_topics."""

    # Unique together: coursepage_id, coursestopic_id. generate() must keep them distinct.

    class Params:
        coursepage = None
        coursestopic = None

    id = factory.Sequence(lambda n: n)
    coursepage_id = fg.parent("coursepage", "page_ptr_id")
    coursestopic_id = fg.parent("coursestopic", "id")


@fg.table("raw__mitxonline__app__postgres__cms_instructorpage", default_count=SCAFFOLD_DEFAULT_COUNT)
class InstructorPageBase(fg.RowFactory):
    """Table cms_instructorpage, from model cms.InstructorPage."""

    # page_ptr_id: one-to-one with wagtailcore_page; draw its parents with pair().

    class Params:
        page_ptr = None
        feature_image = None

    page_ptr_id = fg.parent("page_ptr", "id")
    instructor_name = fg.value("person.full_name")
    feature_image_id = fg.parent("feature_image", "id", nullable=True)
    instructor_title = fg.or_null(fg.value("text.title"))
    instructor_bio_long = fg.or_null(fg.value("text.sentence"))
    instructor_bio_short = fg.or_null(fg.value("text.sentence"))


@fg.table("raw__mitxonline__app__postgres__cms_instructorpagelink", default_count=SCAFFOLD_DEFAULT_COUNT)
class InstructorPageLinkBase(fg.RowFactory):
    """Table cms_instructorpagelink, from model cms.InstructorPageLink."""

    class Params:
        page = None
        linked_instructor_page = None

    id = factory.Sequence(lambda n: n)
    order = fg.or_null(fg.value("numeric.integer_number", start=0, end=1000))
    page_id = fg.parent("page", "id")
    linked_instructor_page_id = fg.parent("linked_instructor_page", "page_ptr_id", nullable=True)


@fg.table("raw__mitxonline__app__postgres__cms_productpagefaq", default_count=SCAFFOLD_DEFAULT_COUNT)
class ProductPageFAQBase(fg.RowFactory):
    """Table cms_productpagefaq, from model cms.ProductPageFAQ."""

    class Params:
        page = None

    id = factory.Sequence(lambda n: n)
    page_id = fg.parent("page", "id")
    question = fg.value("text.sentence")
    answer = fg.value("text.sentence")
    sort_order = fg.or_null(fg.value("numeric.integer_number", start=0, end=1000))


@fg.table("raw__mitxonline__app__postgres__cms_programpage", default_count=SCAFFOLD_DEFAULT_COUNT)
class ProgramPageBase(fg.RowFactory):
    """Table cms_programpage, from model cms.ProgramPage."""

    # price: JSON whose shape the model does not say; set it in __init__.py.
    # program_id: one-to-one with courses_program; draw its parents with pair().
    # page_ptr_id: one-to-one with wagtailcore_page; draw its parents with pair().

    class Params:
        program = None
        page_ptr = None
        search_image = None
        feature_image = None

    about = fg.or_null(fg.value("text.sentence"))
    effort = fg.or_null(fg.value("text.word"))
    length = fg.value("text.word")
    faq_url = fg.or_null(fg.url())
    max_weeks = fg.or_null(fg.value("numeric.integer_number", start=0, end=1000))
    min_weeks = fg.or_null(fg.value("numeric.integer_number", start=0, end=1000))
    video_url = fg.or_null(fg.url())
    program_id = fg.parent("program", "id", nullable=True)
    description = fg.or_null(fg.value("text.sentence"))
    page_ptr_id = fg.parent("page_ptr", "id")
    prerequisites = fg.or_null(fg.value("text.sentence"))
    what_you_learn = fg.or_null(fg.value("text.sentence"))
    search_image_id = fg.parent("search_image", "id", nullable=True)
    feature_image_id = fg.parent("feature_image", "id", nullable=True)
    max_weekly_hours = fg.value("numeric.integer_number", start=0, end=9999)
    min_weekly_hours = fg.value("numeric.integer_number", start=0, end=9999)
    faculty_section_title = fg.or_null(fg.value("text.title"))
    include_in_learn_catalog = fg.or_null(fg.value("development.boolean"))
    hyl_choice_learn_by_doing = fg.or_null(fg.value("development.boolean"))
    hyl_choice_learn_on_demand = fg.or_null(fg.value("development.boolean"))
    hyl_choice_learn_from_others = fg.or_null(fg.value("development.boolean"))
    hyl_choice_ai_enabled_support = fg.or_null(fg.value("development.boolean"))
    hyl_choice_realworld_learning = fg.or_null(fg.value("development.boolean"))
    hyl_choice_stackable_credentials = fg.or_null(fg.value("development.boolean"))
    list_price = fg.or_null(fg.value("numeric.decimal_number", start=0, end=999999))
    max_price = fg.value("numeric.decimal_number", start=0, end=999999)
    min_price = fg.value("numeric.decimal_number", start=0, end=999999)
    hubspot_form_id = fg.value("text.word")
    show_stay_updated = fg.value("development.boolean")


@fg.table("raw__mitxonline__app__postgres__variants_supportedvariant", default_count=SCAFFOLD_DEFAULT_COUNT)
class SupportedVariantBase(fg.RowFactory):
    """Table variants_supportedvariant, from model variants.SupportedVariant."""

    # object_id: the object id of generic foreign key variant_object, set in generate() with content_type_id.

    class Params:
        content_type = None

    id = factory.Sequence(lambda n: n)
    active = fg.value("development.boolean")
    b2b_only = fg.value("development.boolean")
    language = factory.Iterator(SUPPORTED_VARIANT_LANGUAGE_CHOICES)
    created_on = fg.value("datetime.datetime", start=2020, end=2025)
    updated_on = fg.value("datetime.datetime", start=2020, end=2025)
    variant_length = factory.Iterator(["", "S"])
    content_type_id = fg.parent("content_type", "id")
    default_variant = fg.value("development.boolean")
    variant_industry = factory.Iterator(["", "E", "F", "HC", "M"])


# Each tree's MP_Node.alphabet, for fixture_trees.Tree.
TREE_ALPHABETS = {
    ProgramRequirementBase.raw_table: "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
    PageBase.raw_table: "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ",
}
