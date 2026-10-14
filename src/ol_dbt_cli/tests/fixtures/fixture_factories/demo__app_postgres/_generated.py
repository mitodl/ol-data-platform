"""Base factories of the `demo/app_postgres` unit, as a scaffold from Django models would leave them."""

from __future__ import annotations

import factory

from ol_dbt_cli.lib.fixture_generation import RESERVED_DOMAIN, RowFactory, parent, table, url, value


@table("raw__demo__app__postgres__courses_course", default_count=4)
class CourseBase(RowFactory):
    id = factory.Sequence(lambda n: n)
    title = value("text.title")
    live = value("development.boolean")
    price = value("numeric.float_number", start=0, end=999, precision=4)
    created_on = value("datetime.datetime", start=2020, end=2024)


@table("raw__demo__app__postgres__cms_coursepage", default_count=4)
class CoursePageBase(RowFactory):
    class Params:
        course = None

    page_ptr_id = factory.Sequence(lambda n: n)
    course_id = parent("course", "id")
    description = value("text.sentence")
    thumbnail_url = url()


@table("raw__demo__app__postgres__courses_courserun", default_count=8)
class CourseRunBase(RowFactory):
    class Params:
        course = None

    id = factory.Sequence(lambda n: n)
    course_id = parent("course", "id")
    run_tag = factory.Iterator(["R1", "R2", "R3"])
    start_date = value("datetime.datetime", start=2022, end=2024)
    enrollment_start = value("datetime.date", start=2021, end=2021)


@table("raw__demo__app__postgres__users_user", default_count=6)
class UserBase(RowFactory):
    id = factory.Sequence(lambda n: n)
    username = factory.Sequence(lambda n: f"user{n}")
    email = factory.Sequence(lambda n: f"user{n}@{RESERVED_DOMAIN}")
    contact_email = value("person.email")
    name = value("person.full_name")


@table("raw__demo__app__postgres__courses_courserunenrollment", default_count=12)
class EnrollmentBase(RowFactory):
    class Params:
        user = None
        run = None

    id = factory.Sequence(lambda n: n)
    user_id = parent("user", "id")
    run_id = parent("run", "id")
