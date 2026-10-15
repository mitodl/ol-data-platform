"""Base factories of the `xpro/app_postgres` unit, scaffolded from its models.

From mitxpro at git:5196f767ba8db0160f3e67a8118b78c43638d5ef. Written by `ol-dbt fixtures scaffold` from models.json and
rewritten wholesale on every run: change a factory by subclassing it in __init__.py.
"""

from __future__ import annotations

import factory

from ol_dbt_cli.lib.fixture_generation import RowFactory, or_null, parent, table, url, value

SCAFFOLD_DEFAULT_COUNT = 10


@table("raw__xpro__app__postgres__courses_coursetopic", default_count=SCAFFOLD_DEFAULT_COUNT)
class CourseTopicBase(RowFactory):
    """Table courses_coursetopic, from model courses.CourseTopic."""

    # parent_id: references this table; NULL unless generate() sets it with attach().

    id = factory.Sequence(lambda n: n)
    name = factory.Sequence(lambda n: f"name-{n}")
    created_on = value("datetime.datetime", start=2020, end=2025)
    updated_on = value("datetime.datetime", start=2020, end=2025)


@table("raw__xpro__app__postgres__courses_platform", default_count=SCAFFOLD_DEFAULT_COUNT)
class PlatformBase(RowFactory):
    """Table courses_platform, from model courses.Platform."""

    id = factory.Sequence(lambda n: n)
    name = factory.Sequence(lambda n: f"name-{n}")
    created_on = value("datetime.datetime", start=2020, end=2025)
    updated_on = value("datetime.datetime", start=2020, end=2025)
    enable_sync = value("development.boolean")


@table("raw__xpro__app__postgres__courses_program", default_count=SCAFFOLD_DEFAULT_COUNT)
class ProgramBase(RowFactory):
    """Table courses_program, from model courses.Program."""

    class Params:
        platform = None

    id = factory.Sequence(lambda n: n)
    live = value("development.boolean")
    title = value("text.title")
    created_on = value("datetime.datetime", start=2020, end=2025)
    updated_on = value("datetime.datetime", start=2020, end=2025)
    is_external = value("development.boolean")
    platform_id = parent("platform", "id")
    readable_id = factory.Sequence(lambda n: f"readable-{n}")


@table("raw__xpro__app__postgres__courses_course", default_count=SCAFFOLD_DEFAULT_COUNT)
class CourseBase(RowFactory):
    """Table courses_course, from model courses.Course."""

    class Params:
        program = None
        platform = None

    id = factory.Sequence(lambda n: n)
    live = value("development.boolean")
    title = value("text.title")
    created_on = value("datetime.datetime", start=2020, end=2025)
    program_id = parent("program", "id", nullable=True)
    updated_on = value("datetime.datetime", start=2020, end=2025)
    is_external = value("development.boolean")
    platform_id = parent("platform", "id")
    readable_id = factory.Sequence(lambda n: f"readable-{n}")
    external_course_id = value("text.word")
    position_in_program = or_null(value("numeric.integer_number", start=0, end=1000))


@table("raw__xpro__app__postgres__courses_courserun", default_count=SCAFFOLD_DEFAULT_COUNT)
class CourseRunBase(RowFactory):
    """Table courses_courserun, from model courses.CourseRun."""

    # Unique together: course_id, run_tag. generate() must keep them distinct.

    class Params:
        course = None

    id = factory.Sequence(lambda n: n)
    live = value("development.boolean")
    title = value("text.title")
    run_tag = value("numeric.integer_number", start=0, end=9999)
    end_date = or_null(value("datetime.datetime", start=2020, end=2025))
    course_id = parent("course", "id")
    created_on = value("datetime.datetime", start=2020, end=2025)
    start_date = or_null(value("datetime.datetime", start=2020, end=2025))
    updated_on = value("datetime.datetime", start=2020, end=2025)
    courseware_id = factory.Sequence(lambda n: f"courseware-{n}")
    enrollment_end = or_null(value("datetime.datetime", start=2020, end=2025))
    expiration_date = or_null(value("datetime.datetime", start=2020, end=2025))
    enrollment_start = or_null(value("datetime.datetime", start=2020, end=2025))
    courseware_url_path = or_null(value("text.word"))
    external_course_run_id = value("text.word")


@table("raw__xpro__app__postgres__django_content_type", default_count=SCAFFOLD_DEFAULT_COUNT)
class ContentTypeBase(RowFactory):
    """Table django_content_type, from model contenttypes.ContentType."""

    # Unique together: app_label, model. generate() must keep them distinct.

    id = factory.Sequence(lambda n: n)
    model = value("text.word")
    app_label = value("text.word")


@table("raw__xpro__app__postgres__ecommerce_product", default_count=SCAFFOLD_DEFAULT_COUNT)
class ProductBase(RowFactory):
    """Table ecommerce_product, from model ecommerce.Product."""

    # Unique together: content_type_id, object_id. generate() must keep them distinct.
    # object_id: the object id of generic foreign key content_object, set in generate() with content_type_id.

    class Params:
        content_type = None

    id = factory.Sequence(lambda n: n)
    is_active = value("development.boolean")
    created_on = value("datetime.datetime", start=2020, end=2025)
    is_private = value("development.boolean")
    updated_on = value("datetime.datetime", start=2020, end=2025)
    content_type_id = parent("content_type", "id")


@table("raw__xpro__app__postgres__ecommerce_productversion", default_count=SCAFFOLD_DEFAULT_COUNT)
class ProductVersionBase(RowFactory):
    """Table ecommerce_productversion, from model ecommerce.ProductVersion."""

    class Params:
        product = None

    id = factory.Sequence(lambda n: n)
    price = value("numeric.decimal_number", start=0, end=999999)
    text_id = or_null(value("text.sentence"))
    created_on = value("datetime.datetime", start=2020, end=2025)
    product_id = parent("product", "id")
    updated_on = value("datetime.datetime", start=2020, end=2025)
    description = value("text.sentence")
    requires_enrollment_code = value("development.boolean")


@table("raw__xpro__app__postgres__wagtailcore_page", default_count=SCAFFOLD_DEFAULT_COUNT)
class PageBase(RowFactory):
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


@table("raw__xpro__app__postgres__cms_facultymemberspage", default_count=SCAFFOLD_DEFAULT_COUNT)
class FacultyMembersPageBase(RowFactory):
    """Table cms_facultymemberspage, from model cms.FacultyMembersPage."""

    # members: JSON whose shape the model does not say; set it in __init__.py.
    # page_ptr_id: one-to-one with wagtailcore_page; draw its parents with pair().

    class Params:
        page_ptr = None

    heading = value("text.word")
    subhead = or_null(value("text.word"))
    page_ptr_id = parent("page_ptr", "id")


@table("raw__xpro__app__postgres__wagtailimages_image", default_count=SCAFFOLD_DEFAULT_COUNT)
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
    collection_id = parent("collection", "id")
    focal_point_x = or_null(value("numeric.integer_number", start=0, end=1000))
    focal_point_y = or_null(value("numeric.integer_number", start=0, end=1000))
    focal_point_width = or_null(value("numeric.integer_number", start=0, end=1000))
    focal_point_height = or_null(value("numeric.integer_number", start=0, end=1000))
    description = value("text.sentence")


@table("raw__xpro__app__postgres__cms_certificatepage", default_count=SCAFFOLD_DEFAULT_COUNT)
class CertificatePageBase(RowFactory):
    """Table cms_certificatepage, from model cms.CertificatePage."""

    # overrides: JSON whose shape the model does not say; set it in __init__.py.
    # page_ptr_id: one-to-one with wagtailcore_page; draw its parents with pair().
    # signatories: JSON whose shape the model does not say; set it in __init__.py.

    class Params:
        page_ptr = None
        partner_logo = None

    ceus = or_null(value("numeric.decimal_number", start=0, end=999))
    page_ptr_id = parent("page_ptr", "id")
    product_name = value("text.title")
    institute_text = or_null(value("text.sentence"))
    partner_logo_id = parent("partner_logo", "id", nullable=True)
    partner_logo_placement = or_null(factory.Iterator([1, 2]))
    display_mit_seal = value("development.boolean")


@table("raw__xpro__app__postgres__cms_coursepage", default_count=SCAFFOLD_DEFAULT_COUNT)
class CoursePageBase(RowFactory):
    """Table cms_coursepage, from model cms.CoursePage."""

    # content: JSON whose shape the model does not say; set it in __init__.py.
    # course_id: one-to-one with courses_course; draw its parents with pair().
    # language_id: references courses_courselanguage, which the fixture has no table for.
    # page_ptr_id: one-to-one with wagtailcore_page; draw its parents with pair().

    class Params:
        course = None
        language = None
        page_ptr = None
        search_image = None
        thumbnail_image = None
        background_image = None

    format = factory.Iterator(["Online", "Hybrid"])
    subhead = value("text.word")
    duration = or_null(value("text.word"))
    featured = value("development.boolean")
    course_id = parent("course", "id", nullable=True)
    max_weeks = or_null(value("numeric.integer_number", start=0, end=1000))
    min_weeks = or_null(value("numeric.integer_number", start=0, end=1000))
    video_url = or_null(url())
    description = value("text.sentence")
    language_id = parent("language", "id")
    page_ptr_id = parent("page_ptr", "id")
    video_title = value("text.title")
    catalog_details = value("text.sentence")
    search_image_id = parent("search_image", "id", nullable=True)
    time_commitment = or_null(value("text.word"))
    max_weekly_hours = or_null(value("numeric.integer_number", start=0, end=1000))
    min_weekly_hours = or_null(value("numeric.integer_number", start=0, end=1000))
    thumbnail_image_id = parent("thumbnail_image", "id", nullable=True)
    background_image_id = parent("background_image", "id", nullable=True)
    background_video_url = or_null(url())
    external_marketing_url = or_null(url())
    marketing_hubspot_form_id = value("text.word")


@table("raw__xpro__app__postgres__cms_coursepage_topics", default_count=SCAFFOLD_DEFAULT_COUNT)
class CoursePageTopicsBase(RowFactory):
    """Table cms_coursepage_topics, from model cms.CoursePage_topics."""

    # Unique together: coursepage_id, coursetopic_id. generate() must keep them distinct.

    class Params:
        coursepage = None
        coursetopic = None

    id = factory.Sequence(lambda n: n)
    coursepage_id = parent("coursepage", "page_ptr_id")
    coursetopic_id = parent("coursetopic", "id")


@table("raw__xpro__app__postgres__cms_externalcoursepage", default_count=SCAFFOLD_DEFAULT_COUNT)
class ExternalCoursePageBase(RowFactory):
    """Table cms_externalcoursepage, from model cms.ExternalCoursePage."""

    # content: JSON whose shape the model does not say; set it in __init__.py.
    # course_id: one-to-one with courses_course; draw its parents with pair().
    # language_id: references courses_courselanguage, which the fixture has no table for.
    # page_ptr_id: one-to-one with wagtailcore_page; draw its parents with pair().

    class Params:
        course = None
        language = None
        page_ptr = None
        search_image = None
        thumbnail_image = None
        background_image = None

    format = factory.Iterator(["Online", "Hybrid"])
    subhead = value("text.word")
    duration = or_null(value("text.word"))
    featured = value("development.boolean")
    course_id = parent("course", "id", nullable=True)
    max_weeks = or_null(value("numeric.integer_number", start=0, end=1000))
    min_weeks = or_null(value("numeric.integer_number", start=0, end=1000))
    video_url = or_null(url())
    description = value("text.sentence")
    language_id = parent("language", "id")
    page_ptr_id = parent("page_ptr", "id")
    video_title = value("text.title")
    catalog_details = value("text.sentence")
    search_image_id = parent("search_image", "id", nullable=True)
    time_commitment = or_null(value("text.word"))
    max_weekly_hours = or_null(value("numeric.integer_number", start=0, end=1000))
    min_weekly_hours = or_null(value("numeric.integer_number", start=0, end=1000))
    thumbnail_image_id = parent("thumbnail_image", "id", nullable=True)
    background_image_id = parent("background_image", "id", nullable=True)
    background_video_url = or_null(url())
    external_marketing_url = or_null(url())
    marketing_hubspot_form_id = value("text.word")


@table("raw__xpro__app__postgres__cms_externalcoursepage_topics", default_count=SCAFFOLD_DEFAULT_COUNT)
class ExternalCoursePageTopicsBase(RowFactory):
    """Table cms_externalcoursepage_topics, from model cms.ExternalCoursePage_topics."""

    # Unique together: externalcoursepage_id, coursetopic_id. generate() must keep them distinct.

    class Params:
        coursetopic = None
        externalcoursepage = None

    id = factory.Sequence(lambda n: n)
    coursetopic_id = parent("coursetopic", "id")
    externalcoursepage_id = parent("externalcoursepage", "page_ptr_id")


@table("raw__xpro__app__postgres__cms_externalprogrampage", default_count=SCAFFOLD_DEFAULT_COUNT)
class ExternalProgramPageBase(RowFactory):
    """Table cms_externalprogrampage, from model cms.ExternalProgramPage."""

    # content: JSON whose shape the model does not say; set it in __init__.py.
    # program_id: one-to-one with courses_program; draw its parents with pair().
    # language_id: references courses_courselanguage, which the fixture has no table for.
    # page_ptr_id: one-to-one with wagtailcore_page; draw its parents with pair().

    class Params:
        program = None
        language = None
        page_ptr = None
        search_image = None
        thumbnail_image = None
        background_image = None

    format = factory.Iterator(["Online", "Hybrid"])
    subhead = value("text.word")
    duration = or_null(value("text.word"))
    featured = value("development.boolean")
    max_weeks = or_null(value("numeric.integer_number", start=0, end=1000))
    min_weeks = or_null(value("numeric.integer_number", start=0, end=1000))
    video_url = or_null(url())
    program_id = parent("program", "id", nullable=True)
    description = value("text.sentence")
    language_id = parent("language", "id")
    page_ptr_id = parent("page_ptr", "id")
    video_title = value("text.title")
    catalog_details = value("text.sentence")
    search_image_id = parent("search_image", "id", nullable=True)
    time_commitment = or_null(value("text.word"))
    max_weekly_hours = or_null(value("numeric.integer_number", start=0, end=1000))
    min_weekly_hours = or_null(value("numeric.integer_number", start=0, end=1000))
    thumbnail_image_id = parent("thumbnail_image", "id", nullable=True)
    background_image_id = parent("background_image", "id", nullable=True)
    background_video_url = or_null(url())
    external_marketing_url = or_null(url())
    marketing_hubspot_form_id = value("text.word")


@table("raw__xpro__app__postgres__cms_programpage", default_count=SCAFFOLD_DEFAULT_COUNT)
class ProgramPageBase(RowFactory):
    """Table cms_programpage, from model cms.ProgramPage."""

    # content: JSON whose shape the model does not say; set it in __init__.py.
    # program_id: one-to-one with courses_program; draw its parents with pair().
    # language_id: references courses_courselanguage, which the fixture has no table for.
    # page_ptr_id: one-to-one with wagtailcore_page; draw its parents with pair().

    class Params:
        program = None
        language = None
        page_ptr = None
        search_image = None
        thumbnail_image = None
        background_image = None

    format = factory.Iterator(["Online", "Hybrid"])
    subhead = value("text.word")
    duration = or_null(value("text.word"))
    featured = value("development.boolean")
    max_weeks = or_null(value("numeric.integer_number", start=0, end=1000))
    min_weeks = or_null(value("numeric.integer_number", start=0, end=1000))
    video_url = or_null(url())
    program_id = parent("program", "id", nullable=True)
    description = value("text.sentence")
    language_id = parent("language", "id")
    page_ptr_id = parent("page_ptr", "id")
    video_title = value("text.title")
    catalog_details = value("text.sentence")
    search_image_id = parent("search_image", "id", nullable=True)
    time_commitment = or_null(value("text.word"))
    max_weekly_hours = or_null(value("numeric.integer_number", start=0, end=1000))
    min_weekly_hours = or_null(value("numeric.integer_number", start=0, end=1000))
    thumbnail_image_id = parent("thumbnail_image", "id", nullable=True)
    background_image_id = parent("background_image", "id", nullable=True)
    background_video_url = or_null(url())
    external_marketing_url = or_null(url())
    marketing_hubspot_form_id = value("text.word")


# Each tree's MP_Node.alphabet, for fixture_trees.Tree.
TREE_ALPHABETS = {
    PageBase.raw_table: "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ",
}
