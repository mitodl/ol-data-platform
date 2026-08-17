"""Tests for ol_orchestrate.resources.openedx.

Focused on pagination, which is where the subtle bug was: the ``next`` URL was
parsed as though it were a bare query string, so the whole URL became a query
parameter *name* and each page nested it one level deeper.
"""

from ol_orchestrate.resources.openedx import next_page_params

COURSES = "https://courses.learn.mit.edu/api/courses/v1/courses/"


def test_only_the_query_component_becomes_parameters() -> None:
    assert next_page_params(f"{COURSES}?page=2") == {"page": ["2"]}


def test_the_url_itself_never_becomes_a_parameter_name() -> None:
    """The DAGSTER-E mechanism.

    ``parse_qs`` on a full URL takes everything before the first ``=`` as the
    key, so this used to return
    ``{"https://courses.learn.mit.edu/api/courses/v1/courses/?page": ["2"]}``
    and that key was sent as a query parameter name. The server echoed the
    mangled parameters into the next ``next``, so each page nested the base URL
    one level deeper until the request 429'd.
    """
    params = next_page_params(f"{COURSES}?page=2")

    assert not any(key.startswith("http") for key in params), (
        f"a URL leaked into the parameter names: {list(params)}"
    )


def test_several_parameters_all_survive() -> None:
    params = next_page_params(f"{COURSES}?page=3&page_size=100&username=svc")

    assert params == {"page": ["3"], "page_size": ["100"], "username": ["svc"]}


def test_a_next_url_with_no_query_yields_nothing() -> None:
    """The last page can hand back a bare URL; that must not invent a filter."""
    assert next_page_params(COURSES) == {}


def test_a_relative_next_url_is_handled() -> None:
    """Some DRF configurations return a path rather than an absolute URL."""
    assert next_page_params("/api/courses/v1/courses/?page=4") == {"page": ["4"]}
