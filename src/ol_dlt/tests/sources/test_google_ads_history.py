import json
from pathlib import Path
from typing import Any

import fsspec
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


def _landing_zone(root: Path, report: str = REPORT) -> Path:
    for course, api_lines in (("sys_think", API_LINES), ("mm_dedp", [])):
        folder = root / course
        folder.mkdir(parents=True)
        (folder / history.REPORT_FILE).write_bytes(report.encode("utf-16"))
        (folder / history.API_FILE).write_text(
            "".join(json.dumps(line) + "\n" for line in api_lines), encoding="utf-8"
        )
    return root


def _rows(root: Path, table: str) -> list[dict[str, Any]]:
    source = history.google_ads_history_source(
        files=lambda: history.landing_zone_files(fsspec.filesystem("file"), str(root))
    )
    return list(source.resources[table])


def test_report_rows_drop_the_user_and_mask_emails(tmp_path: Path) -> None:
    rows = _rows(_landing_zone(tmp_path), history.REPORT_TABLE)

    sys_think = [r for r in rows if r["course"] == "sys_think"]
    assert [r["row_number"] for r in sys_think] == [1, 2]
    assert {r["course"] for r in rows} == {"sys_think", "mm_dedp"}
    assert all("user" not in r for r in rows)
    assert sys_think[0]["changes"].splitlines() == [
        "Budget changed",
        "Course - Search: Budget amount changed from $50.00 to $60.00",
    ]
    assert sys_think[1]["changes"] == 'Granted access to "<email>"'
    assert "example" not in json.dumps(rows)


def test_api_rows_keep_the_report_terms_and_skip_empty_files(tmp_path: Path) -> None:
    rows = _rows(_landing_zone(tmp_path), history.API_TABLE)

    assert [(r["course"], r["line_number"], r["header"]) for r in rows] == [
        ("sys_think", 1, "Campaign changed"),
        ("sys_think", 2, "Budget changed"),
    ]
    assert json.loads(rows[1]["details"]) == API_LINES[1]["details"]


def test_a_changed_report_header_fails_the_load(tmp_path: Path) -> None:
    report = REPORT.replace("Ad group\t", "")
    with pytest.raises(Exception, match="has the header"):
        _rows(_landing_zone(tmp_path, report), history.REPORT_TABLE)
