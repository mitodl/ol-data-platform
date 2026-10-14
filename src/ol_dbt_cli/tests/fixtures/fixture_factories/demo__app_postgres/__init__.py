"""The factory package of the `demo/app_postgres` unit the generation tests build."""

from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import Any

import factory

from ol_dbt_cli.lib.fixture_generation import draw, link, pair, pick, scaled_count

from ._generated import CourseBase, CoursePageBase, CourseRunBase, EnrollmentBase, UserBase

SEED = 20261013


class Course(CourseBase):
    readable_id = factory.Sequence(lambda n: f"course-v1:Demo+C{n}")


class CourseRun(CourseRunBase):
    courseware_id = factory.LazyAttribute(lambda row: f"{row.course['readable_id']}+{row.run_tag}")
    end_date = factory.LazyAttribute(
        lambda row: row.start_date + datetime.timedelta(days=draw("numeric.integer_number", start=7, end=120))
    )


class User(UserBase):
    profile = factory.LazyAttribute(lambda row: {"name": row.name, "tags": ["generated"]})


def generate(hand_written: Mapping[str, list[dict[str, Any]]], scale: float) -> dict[str, list[dict[str, Any]]]:
    """Give every generated course a page, and draw runs and enrollments from all the rows there are."""
    courses = Course.build_batch(scaled_count(Course, scale))
    pages = CoursePageBase.build_batch(len(courses), course=factory.Iterator(pair(courses, len(courses))))
    run_courses = pick(hand_written[Course.raw_table] + courses, scaled_count(CourseRun, scale))
    runs = CourseRun.build_batch(len(run_courses), course=factory.Iterator(run_courses))
    users = User.build_batch(scaled_count(User, scale))
    enrolled = link(users, runs, scaled_count(EnrollmentBase, scale))
    return {
        Course.raw_table: courses,
        CoursePageBase.raw_table: pages,
        CourseRun.raw_table: runs,
        User.raw_table: users,
        EnrollmentBase.raw_table: [EnrollmentBase.build(user=user, run=run) for user, run in enrolled],
    }
