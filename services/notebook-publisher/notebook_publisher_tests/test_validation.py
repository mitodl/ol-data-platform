import pytest
from notebook_publisher.settings import Settings
from notebook_publisher.validation import (
    NotebookPublish,
    NotebookRejectedError,
    check_notebook,
)
from pydantic import ValidationError

from notebook_publisher_tests.conftest import NOTEBOOK

TOKEN_REMOTE = "https://x-access-token:abc@github.com/mitodl/r.git"  # noqa: S105  # pragma: allowlist secret


@pytest.mark.parametrize("name", ["sales", "a", "q3-report", "x" * 40])
def test_accepts_dns_label_names(name: str) -> None:
    assert NotebookPublish(name=name, content="").name == name


@pytest.mark.parametrize(
    "name",
    ["", "Sales", "3d", "-a", "a-", "a_b", "a.b", "x" * 41, "logout", "healthz"],
)
def test_rejects_other_names(name: str) -> None:
    with pytest.raises(ValidationError):
        NotebookPublish(name=name, content="")


def test_rejects_public_access_in_pass_one() -> None:
    with pytest.raises(ValidationError):
        NotebookPublish.model_validate({"name": "a", "content": "", "access": "public"})


@pytest.mark.parametrize(
    "remote",
    [
        "https://github.com/mitodl/ol-data-notebooks.git",
        "git@github.com:mitodl/ol-data-notebooks.git",
    ],
)
def test_accepts_git_remotes(remote: str) -> None:
    assert NotebookPublish(name="a", content="", git_remote=remote).git_remote == remote


def test_rejects_git_remote_with_credentials() -> None:
    with pytest.raises(ValidationError, match="credentials"):
        NotebookPublish(name="a", content="", git_remote=TOKEN_REMOTE)


def test_accepts_a_marimo_notebook(settings: Settings) -> None:
    check_notebook(NOTEBOOK, settings)


def test_requires_a_script_header(settings: Settings) -> None:
    content = NOTEBOOK.split("# ///\n", 2)[-1]
    with pytest.raises(NotebookRejectedError, match="script"):
        check_notebook(content, settings)


def test_rejects_an_invalid_script_header(settings: Settings) -> None:
    content = NOTEBOOK.replace('["marimo", "ol-notebook"]', '["marimo"')
    with pytest.raises(NotebookRejectedError, match="TOML"):
        check_notebook(content, settings)


def test_rejects_oversized_content(settings: Settings) -> None:
    content = NOTEBOOK + "#" * settings.max_content_bytes
    with pytest.raises(NotebookRejectedError, match="limit"):
        check_notebook(content, settings)


def test_rejects_syntax_errors(settings: Settings) -> None:
    with pytest.raises(NotebookRejectedError, match="syntax"):
        check_notebook(NOTEBOOK + "\ndef broken(:\n", settings)


def test_rejects_what_marimo_check_rejects(settings: Settings) -> None:
    content = NOTEBOOK.replace(
        "\n\nif __name__",
        "\n\n@app.cell\ndef _():\n    mo = 1\n    return (mo,)\n\n\nif __name__",
    )
    with pytest.raises(NotebookRejectedError, match="multiple-definitions"):
        check_notebook(content, settings)


def test_rejects_plain_scripts(settings: Settings) -> None:
    content = '# /// script\n# dependencies = []\n# ///\nprint("hi")\n'
    with pytest.raises(NotebookRejectedError, match="marimo check"):
        check_notebook(content, settings)
