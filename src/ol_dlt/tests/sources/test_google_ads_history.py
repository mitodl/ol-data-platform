import base64
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from ol_dlt.sources.google_ads import history

REPORT = (
    "Change history report\n"
    "October 7, 2024 - October 6, 2026\n"
    "Date & time\tUser\tCampaign\tAd group\tChanges\n"
    "Oct 6, 2026, 3:04:05 PM\tperson@example.com\tCourse - Search\t\t"
    '"Budget changed\n'
    'Course - Search: Budget amount changed from $50.00 to $60.00"\n'
    "Oct 5, 2026, 9:00:00 AM\tperson@example.com\t\t\t"
    'Granted access to "new.person@example.org"\n'
)
API_LINES = [
    {
        "timestamp": "2026-10-20T10:00:00",
        "campaign": "Course - Search",
        "ad_group": "",
        "header": "Campaign changed",
        "details": ["Status changed from paused to active"],
    },
    {
        "timestamp": "2026-10-21T11:00:00",
        "campaign": "",
        "ad_group": "",
        "header": "Budget changed",
        "details": ["Course - Search: Budget amount changed from $60.00 to $70.00"],
    },
]
API_JSONL = "".join(json.dumps(line) + "\n" for line in API_LINES)


def _checkout(root: Path, report: str = REPORT) -> Path:
    data = root / "sys_think" / "data"
    data.mkdir(parents=True)
    (data / history.REPORT_FILE).write_bytes(report.encode("utf-16"))
    (data / history.API_FILE).write_text(API_JSONL, encoding="utf-8")
    (data / "keyword_windows.csv").write_text("not change history\n")
    return root


def _rows(root: Path, table: str) -> list[dict[str, Any]]:
    source = history.google_ads_history_source(files=lambda: history.local_files(root))
    return list(source.resources[table])


def test_report_rows_drop_the_user_and_mask_emails(tmp_path: Path) -> None:
    rows = _rows(_checkout(tmp_path), history.REPORT_TABLE)

    assert [r["row_number"] for r in rows] == [1, 2]
    assert {r["course"] for r in rows} == {"sys_think"}
    assert all("user" not in r for r in rows)
    assert rows[0]["changes"].splitlines() == [
        "Budget changed",
        "Course - Search: Budget amount changed from $50.00 to $60.00",
    ]
    assert rows[1]["changes"] == 'Granted access to "<email>"'
    assert "example" not in json.dumps(rows)


def test_api_rows_keep_the_report_terms(tmp_path: Path) -> None:
    rows = _rows(_checkout(tmp_path), history.API_TABLE)

    assert [(r["line_number"], r["header"]) for r in rows] == [
        (1, "Campaign changed"),
        (2, "Budget changed"),
    ]
    assert json.loads(rows[1]["details"]) == API_LINES[1]["details"]


def test_a_changed_report_header_fails_the_load(tmp_path: Path) -> None:
    report = REPORT.replace("Ad group\t", "")
    with pytest.raises(Exception, match="has the header"):
        _rows(_checkout(tmp_path, report), history.REPORT_TABLE)


def test_github_files_reads_only_the_change_history_blobs() -> None:
    blobs = {"r": REPORT.encode("utf-16"), "a": API_JSONL.encode(), "x": b"x"}
    tree = [
        SimpleNamespace(
            path="sys_think/data/Change history report.csv", type="blob", sha="r"
        ),
        SimpleNamespace(
            path="sys_think/data/change-history-api.jsonl", type="blob", sha="a"
        ),
        SimpleNamespace(
            path="sys_think/data/keyword_windows.csv", type="blob", sha="x"
        ),
        SimpleNamespace(path="sys_think/data", type="tree", sha="t"),
    ]
    repo = SimpleNamespace(
        get_git_tree=lambda ref, recursive: SimpleNamespace(tree=tree),
        get_git_blob=lambda sha: SimpleNamespace(
            content=base64.b64encode(blobs[sha]).decode()
        ),
    )

    files = dict(history.github_files(repo))

    assert sorted(files) == [
        "sys_think/data/Change history report.csv",
        "sys_think/data/change-history-api.jsonl",
    ]
    assert files["sys_think/data/change-history-api.jsonl"] == API_JSONL.encode()
