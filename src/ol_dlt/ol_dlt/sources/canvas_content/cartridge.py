"""What MIT Learn reads from a Canvas Common Cartridge export.

A port of ``learning_resources/etl/canvas_utils.py`` in mit-learn, working on
an open ``zipfile.ZipFile`` so a course can be read over S3 byte ranges rather
than downloaded: the largest export is 24 GB, nearly all of it video no rule
here reads. A published item is an entry in the map ``published_items`` returns,
keyed by its path in the archive. The rules follow Learn's, odd ones included,
because a Sync task replaces what Learn's own parser wrote:

- Every file of a module item's resource counts, so a quiz in a module brings
  in ``assessment_meta.xml`` and an assignment its ``assignment_settings.xml``.
- A file is read for its folder's visibility, its own, and the Files tab, but a
  file a published page links to needs only not to be locked.
- Whether something is locked depends on the time of the read, so the version
  (``course_checksum``) digests the published set.
"""

import hashlib
import json
import logging
import posixpath
import zipfile
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any
from urllib.parse import unquote, unquote_plus
from xml.etree.ElementTree import Element, ParseError
from zoneinfo import ZoneInfo

import dateutil.parser
from bs4 import BeautifulSoup
from defusedxml import ElementTree

logger = logging.getLogger(__name__)

NAMESPACES = {
    "cccv1p0": "http://canvas.instructure.com/xsd/cccv1p0",
    "imscp": "http://www.imsglobal.org/xsd/imsccv1p1/imscp_v1p1",
}

MANIFEST = "imsmanifest.xml"
SETTINGS = "course_settings/course_settings.xml"
CONTEXT = "course_settings/context.xml"
FILES_META = "course_settings/files_meta.xml"
MODULE_META = "course_settings/module_meta.xml"
WEB_RESOURCES = "web_resources"

# CANVAS_TUTORBOT_FOLDER in mit-learn. Learn loads these as tutor problem files,
# not ContentFiles.
TUTOR_FOLDER = "web_resources/ai/tutor/"

# The Files entry of a course's tab_configuration. See
# https://developerdocs.instructure.com/services/dap/dataset/dataset-additional-notes
FILES_TAB_ID = 11

# Canvas rewrites these on every export without changing the course: the
# manifest embeds the export time and reshuffles its resources. What a read takes
# from them is digested instead: the published set, and the course's own fields.
CHECKSUM_EXCLUDED = (MANIFEST, "course_settings/")

# Part of every course version, so that changing it reads every course again.
# Raise it when a rule here or in the source picks different files or text for
# an archive that has not changed.
PARSE_RULE = 1

LOCK_TIMEZONE = ZoneInfo("US/Eastern")


def _parse_datetime(value: str) -> datetime:
    return dateutil.parser.parse(value).replace(tzinfo=LOCK_TIMEZONE).astimezone(UTC)


def is_date_locked(lock_at: str | None, unlock_at: str | None, now: datetime) -> bool:
    """Tell whether a resource is outside its availability dates at ``now``.

    Canvas writes both as UTC, and Learn reads them as US/Eastern. Kept, because
    the choice decides which side of a boundary a file is on for four hours.
    """
    for value, is_locked in (
        (unlock_at, lambda at: now < at),
        (lock_at, lambda at: now > at),
    ):
        if not value or value.lower() == "nil":
            continue
        try:
            if is_locked(_parse_datetime(value)):
                return True
        except (ValueError, OverflowError):
            logger.warning("Could not parse the date %s", value)
    return False


def is_file_published(file_meta: dict[str, Any], now: datetime) -> bool:
    """Tell whether a ``files_meta.xml`` entry is visible to students."""
    if str(file_meta.get("hidden", "false")).lower() == "true":
        return False
    if str(file_meta.get("locked", "false")).lower() == "true":
        return False
    if is_date_locked(file_meta.get("lock_at"), file_meta.get("unlock_at"), now):
        return False
    return file_meta.get("visibility", "inherit") in {
        "course",
        "inherit",
        "institution",
        "public",
    }


def _compact_element(element: Element) -> dict[str, Any] | str | None:
    """Turn an element into nested dictionaries keyed by unqualified tag."""
    if len(element) == 0:
        return element.text.strip() if element.text else None
    return {
        child.tag.split("}")[-1]: _compact_element(child)
        for child in element  # noqa: C416
    }


def resources_by_identifier(manifest_xml: bytes) -> dict[str, dict[str, Any]]:
    """Return the manifest's resources, keyed by identifier."""
    root = ElementTree.fromstring(manifest_xml)
    resources = {}
    for resource in root.findall(".//imscp:resource[@identifier]", NAMESPACES):
        metadata = resource.find("imscp:metadata", NAMESPACES)
        compacted = _compact_element(metadata) if metadata is not None else None
        resources[resource.get("identifier")] = {
            "type": resource.get("type"),
            "href": resource.get("href"),
            "files": [
                file.get("href") for file in resource.findall("imscp:file", NAMESPACES)
            ],
            "metadata": compacted if isinstance(compacted, dict) else {},
            "intendeduse": resource.get("intendeduse"),
        }
    return resources


def resources_by_identifierref(manifest_xml: bytes) -> dict[str, list[list[str]]]:
    """Return the files of each manifest item's resource, by identifierref."""
    root = ElementTree.fromstring(manifest_xml)
    resources: dict[str, list[list[str]]] = defaultdict(list)
    for item in root.findall(".//imscp:item[@identifierref]", NAMESPACES):
        identifierref = item.get("identifierref")
        resource = root.find(
            f'.//imscp:resource[@identifier="{identifierref}"]', NAMESPACES
        )
        if resource is not None:
            resources[identifierref].append(
                [
                    file.get("href")
                    for file in resource.findall("imscp:file", NAMESPACES)
                ]
            )
    return dict(resources)


def parse_settings(archive: zipfile.ZipFile) -> dict[str, Any]:
    """Return ``course_settings.xml`` as a flat map of unqualified tags."""
    if SETTINGS not in archive.namelist():
        return {}
    attributes: dict[str, Any] = {}
    for node in ElementTree.fromstring(archive.read(SETTINGS)).iter():
        tag = node.tag.split("}")[-1]
        value: Any = node.text
        if tag == "tab_configuration":
            value = {tab["id"]: tab for tab in json.loads(node.text)}
        attributes[tag] = value
    return attributes


def parse_context(archive: zipfile.ZipFile) -> dict[str, str | None]:
    """Return the course id and Canvas domain from ``context.xml``."""
    if CONTEXT not in archive.namelist():
        return {}
    root = ElementTree.fromstring(archive.read(CONTEXT))
    context = {}
    for key in ("course_id", "canvas_domain"):
        element = root.find(f"cccv1p0:{key}", NAMESPACES)
        if element is not None:
            context[key] = element.text
    return context


def parse_files_meta(
    archive: zipfile.ZipFile, now: datetime
) -> dict[str, list[dict[str, Any]]]:
    """Sort the files ``files_meta.xml`` names by whether they are published."""
    status: dict[str, list[dict[str, Any]]] = {"active": [], "unpublished": []}
    if FILES_META not in archive.namelist():
        return status
    resources = resources_by_identifier(archive.read(MANIFEST))
    root = ElementTree.fromstring(archive.read(FILES_META))
    for element in root.findall(".//cccv1p0:file", NAMESPACES):
        meta = dict(element.attrib)
        for child in element:
            nil = child.attrib.get("nil") == "true"
            meta[child.tag.split("}")[-1]] = None if nil else (child.text or "").strip()
        published = is_file_published(meta, now)
        for file in resources.get(meta.get("identifier") or "", {}).get("files", []):
            active = published and not file.startswith(TUTOR_FOLDER)
            status["active" if active else "unpublished"].append(
                {"path": file, "title": meta.get("display_name")}
            )
    return status


def parse_module_meta(archive: zipfile.ZipFile) -> list[dict[str, Any]]:
    """Return every file of every active module item."""
    if MODULE_META not in archive.namelist():
        return []
    resources = resources_by_identifierref(archive.read(MANIFEST))
    items: list[dict[str, Any]] = []
    root = ElementTree.fromstring(archive.read(MODULE_META))
    for module in root.findall(".//cccv1p0:module", NAMESPACES):
        module_title = module.find("cccv1p0:title", NAMESPACES).text
        for item in module.findall("cccv1p0:items/cccv1p0:item", NAMESPACES):
            if item.find("cccv1p0:workflow_state", NAMESPACES).text != "active":
                continue
            reference = item.find("cccv1p0:identifierref", NAMESPACES)
            identifierref = reference.text if reference is not None else None
            for files in resources.get(identifierref or "", []):
                items.extend(
                    {
                        "path": file,
                        "title": item.find("cccv1p0:title", NAMESPACES).text,
                        "module": module_title,
                    }
                    for file in files
                )
    return items


def _embedded_files(html: bytes) -> list[str]:
    """Return the course files a page links to, as paths in the archive."""
    links = []
    for anchor in BeautifulSoup(html, "html.parser").find_all("a", href=True):
        href = anchor["href"]
        if href.startswith("$IMS-CC-FILEBASE$"):
            path = href.split("?")[0].replace("$IMS-CC-FILEBASE$", WEB_RESOURCES)
            links.append(unquote(path))
    return links


def _assignment_state(xml: bytes, now: datetime) -> str:
    """Return ``published`` when a student can see an assignment at ``now``."""
    try:
        root = ElementTree.fromstring(xml)
    except ParseError:
        logger.exception("Could not parse an assignment's settings")
        return "unpublished"

    def text(tag: str) -> str:
        element = root.find(f"cccv1p0:{tag}", NAMESPACES)
        return element.text.strip() if element is not None and element.text else ""

    visible = (
        text("workflow_state") == "published"
        and text("only_visible_to_overrides") != "true"
        and text("hide_in_gradebook") != "true"
        and text("module_locked") != "true"
    )
    locked = is_date_locked(text("lock_at"), text("unlock_at"), now)
    return "published" if visible and not locked else "unpublished"


def _assignment_title(xml: bytes) -> str:
    try:
        root = ElementTree.fromstring(xml)
    except ParseError:
        logger.exception("Could not parse an assignment's settings")
        return ""
    element = root.find("cccv1p0:title", NAMESPACES)
    return element.text.strip() if element is not None and element.text else ""


def parse_web_content(
    archive: zipfile.ZipFile, settings: dict[str, Any], now: datetime
) -> dict[str, list[dict[str, Any]]]:
    """Sort a course's HTML pages and assignments by whether they are published."""
    status: dict[str, list[dict[str, Any]]] = {"active": [], "unpublished": []}
    state: Any
    if MANIFEST not in archive.namelist():
        return status
    # Unset counts as public, as it does in Learn.
    ingest_syllabus = not (
        (settings.get("public_syllabus") or "true").lower() == "false"
        and (settings.get("public_syllabus_to_auth") or "true").lower() == "false"
    )
    for resource in resources_by_identifier(archive.read(MANIFEST)).values():
        href = resource["href"]
        if not href or not href.endswith(".html"):
            continue
        html = archive.read(href)
        settings_file = next(
            (
                file
                for file in reversed(resource["files"])
                if file.endswith("assignment_settings.xml")
            ),
            None,
        )
        if settings_file:
            xml = archive.read(settings_file)
            state = _assignment_state(xml, now)
            title = _assignment_title(xml)
        else:
            soup = BeautifulSoup(html, "html.parser")
            meta = soup.find("meta", attrs={"name": "workflow_state"})
            state = meta.get("content") if meta else None
            title_element = soup.find("title")
            title = title_element.get_text().strip() if title_element else ""

        educational = resource["metadata"].get("lom", {}).get("educational") or {}
        role: str = (educational.get("intendedEndUserRole") or {}).get("value") or ""
        authors_only = bool(role) and role.lower() != "student"
        syllabus = resource["intendeduse"] == "syllabus"
        active = (
            state in {"active", "published"} and not authors_only and not syllabus
        ) or (ingest_syllabus and syllabus)
        status["active" if active else "unpublished"].append(
            {"path": href, "title": title, "embedded_files": _embedded_files(html)}
        )
    return status


def restricted_folders(archive: zipfile.ZipFile) -> set[str]:
    """Return the folders ``files_meta.xml`` marks hidden or locked."""
    if FILES_META not in archive.namelist():
        return set()
    root = ElementTree.fromstring(archive.read(FILES_META))
    restricted = set()
    for folder in root.findall(".//cccv1p0:folder", NAMESPACES):
        path = folder.get("path")
        flags = (
            (folder.findtext(f"cccv1p0:{name}", "", NAMESPACES) or "").strip().lower()
            for name in ("hidden", "locked")
        )
        if path and "true" in flags:
            restricted.add(path)
    return restricted


def parse_course_files(archive: zipfile.ZipFile, now: datetime) -> list[dict[str, Any]]:
    """Return the active files of the manifest's ``webcontent`` resources.

    Canvas gives a file a ``files_meta.xml`` entry only when its metadata is not
    the default, so an ordinary published file appears only in the manifest.
    """
    if MANIFEST not in archive.namelist():
        return []
    restricted = restricted_folders(archive)
    unpublished = {
        item["path"] for item in parse_files_meta(archive, now)["unpublished"]
    }
    active = []
    seen = set()
    for resource in resources_by_identifier(archive.read(MANIFEST)).values():
        if resource["type"] != "webcontent":
            continue
        for file in resource["files"]:
            if not file or not file.startswith(f"{WEB_RESOURCES}/") or file in seen:
                continue
            seen.add(file)
            if file.endswith(".html") or file.startswith(TUTOR_FOLDER):
                continue
            parts = file.removeprefix(f"{WEB_RESOURCES}/").split("/")[:-1]
            in_restricted = any(
                "/".join(parts[: depth + 1]) in restricted
                for depth in range(len(parts))
            )
            if not in_restricted and file not in unpublished:
                active.append({"path": file, "title": posixpath.basename(file)})
    return active


def url_config(metadata: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Index the export's ``.metadata.json`` by what an item is looked up with.

    A file is keyed by its path without the first folder (``/notes/a.pdf``), and
    an assignment or page by its name, so a page can pick up another's URL when
    the two share a title.
    """
    config = {}
    for item in metadata.get("course_files", []):
        path = item["file_path"]
        config[unquote_plus(path.lstrip(path.split("/")[0]))] = item
    for item in metadata.get("assignments", []) + metadata.get("pages", []):
        config[item.get("name", item.get("title"))] = {
            **item,
            "url": item.get("html_url"),
        }
    return config


def _config_key(item: dict[str, Any]) -> str:
    if WEB_RESOURCES in item["path"]:
        return item["path"].split(WEB_RESOURCES)[-1]
    return item.get("title") or ""


def _config_visible(
    config: dict[str, Any] | None, now: datetime, *, allow_hidden: bool = False
) -> bool:
    """Tell whether the Canvas API reported an item as visible to students.

    :param config: The item's entry in the url config; visible when it has none.
    :param now: The time of the read.
    :param allow_hidden: Pass a file that is only hidden, as a file a page links
        to is: students reach it through the page.
    """
    if not config:
        return True
    folder = config.get("folder") or {}
    checks = [
        not config.get("published", True),
        is_date_locked(config.get("lock_at"), config.get("unlock_at"), now),
        config.get("locked"),
        folder.get("locked"),
    ]
    if not allow_hidden:
        checks += [config.get("hidden"), folder.get("hidden")]
    return not any(checks)


def published_items(
    archive: zipfile.ZipFile, config: dict[str, dict[str, Any]], now: datetime
) -> dict[str, dict[str, Any]]:
    """Return what students can reach in a course at ``now``, keyed by path.

    :param archive: The course's ``.imscc``.
    :param config: The export's ``url_config``.
    :param now: The time of the read, for files with availability dates.
    :returns: Each item's ``title`` and, if a module holds it, ``module``.
    """
    settings = parse_settings(archive)
    tabs = settings.get("tab_configuration") or {}
    files_tab_visible = not tabs.get(FILES_TAB_ID, {}).get("hidden", False)
    # Later sources override earlier ones for a path they share, so the generic
    # entries from the manifest come first.
    candidates = (
        parse_course_files(archive, now)
        + parse_module_meta(archive)
        + parse_files_meta(archive, now)["active"]
        + parse_web_content(archive, settings, now)["active"]
    )
    published: dict[str, dict[str, Any]] = {}
    for item in candidates:
        path = posixpath.normpath(item["path"])
        in_files_area = WEB_RESOURCES in path.split("/")
        visible = _config_visible(config.get(_config_key(item)), now)
        if visible and (not in_files_area or files_tab_visible or item.get("module")):
            published[path] = item
        for linked in item.get("embedded_files", []):
            linked_path = posixpath.normpath(linked)
            embedded = {"path": linked_path, "title": ""}
            if linked_path in published or linked.startswith(TUTOR_FOLDER):
                continue
            linked_config = config.get(_config_key(embedded))
            if _config_visible(linked_config, now, allow_hidden=True):
                published[linked_path] = embedded
    return published


def course_checksum(
    archive: zipfile.ZipFile,
    published: dict[str, dict[str, Any]],
    config: dict[str, dict[str, Any]],
    course_fields: list[str | None],
) -> str:
    """Digest what a read of the course depends on, as ``canvas_course_checksum``.

    It changes with a content member's bytes, the published set or a title, the
    url config, or the fields of the course's own row, and not across Canvas's
    no-op exports. Learn's digest leaves the last out because Learn writes the
    course before it checks the digest. Here the digest decides whether the course
    row is written at all, so a new course code has to move it.
    """
    digest = hashlib.md5(usedforsecurity=False)
    digest.update(f"parse_rule\t{PARSE_RULE}\n".encode())
    digest.update(json.dumps(course_fields).encode())
    for info in sorted(archive.infolist(), key=lambda info: info.filename):
        if not info.filename.startswith(CHECKSUM_EXCLUDED):
            digest.update(f"{info.filename}\0{info.file_size}\0{info.CRC}\0".encode())
    for path in sorted(published):
        digest.update(f"{path}\0{published[path].get('title') or ''}\0".encode())
    digest.update(json.dumps(config, sort_keys=True, default=str).encode())
    return digest.hexdigest()
