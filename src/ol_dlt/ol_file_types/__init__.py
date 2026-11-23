"""The course files the platform reads text from, for every content source.

One list for OCW, Open edX and (once its source lands every cartridge member)
Canvas, so what the platform extracts no longer depends on which of MIT Learn's
per-source lists a source copied. Learn's own lists are applied where Learn's
data is shaped: the integrations__learn__* views, which carry each file's
extension.

It is a package of its own in the ol-dlt distribution, outside ol_dlt, and
imports only the standard library, so the Open edX code location can use it
without ol_dlt's import-time setup (PYICEBERG_HOME, the dlt patches).
"""

import json
from pathlib import PurePosixPath

# What Tika reads, plus notebooks, which Tika has no parser for (see
# notebook_text). The union of the lists the sources had: OCW's (Learn's
# VALID_FILE_TYPES), Canvas's (Learn's VALID_TEXT_FILE_TYPES), and every
# extension whose MIME type the Open edX asset sent to Tika under the python:3.14
# runtime's builtin table (.bat to .xsl below, besides Learn's). Transcripts
# (.srt, .sjson, .vtt) are here because OCW and Canvas read them as text; the
# Open edX asset leaves them to its transcript asset. A file is typed by its last
# suffix only, so a compressed handout.pdf.gz is not read.
#
# Changing the list changes what existing snapshots would hold: raise
# TEXT_FILE_RULE (ol_dlt.sources.ocw_content) and DOCUMENT_TEXT_CODE_VERSION
# (the openedx location's assets/content_files.py) with it.
TEXT_FILE_EXTENSIONS = frozenset(
    {
        ".bat",
        ".c",
        ".csv",
        ".doc",
        ".docx",
        ".dot",
        ".epub",
        ".h",
        ".htm",
        ".html",
        ".ipynb",
        ".json",
        ".ksh",
        ".m",
        ".markdown",
        ".mat",
        ".md",
        ".pdf",
        ".pl",
        ".pot",
        ".ppa",
        ".pps",
        ".ppt",
        ".pptx",
        ".ps",
        ".pwz",
        ".py",
        ".r",
        ".rdf",
        ".rtf",
        ".sjson",
        ".srt",
        ".tex",
        ".txt",
        ".vtt",
        ".wiz",
        ".wsdl",
        ".xlb",
        ".xls",
        ".xlsx",
        ".xml",
        ".xpdl",
        ".xsl",
    }
)

NOTEBOOK_EXTENSION = ".ipynb"

# How the platform records a course file it does not read.
STATUS_SKIPPED_UNSUPPORTED = "skipped_unsupported"


def file_extension(path: str) -> str:
    """Return a path's lowercased final suffix, or "" when it has none.

    :param path: A file path or key.
    :returns: The suffix with its dot, e.g. ".pdf".
    :rtype: str
    """
    return PurePosixPath(path).suffix.lower()


def is_text_file(path: str) -> bool:
    """Tell whether the platform reads text from the file at ``path``.

    :param path: A file path or key.
    :returns: True when its extension is in TEXT_FILE_EXTENSIONS.
    :rtype: bool
    """
    return file_extension(path) in TEXT_FILE_EXTENSIONS


def notebook_text(body: bytes) -> str | None:
    """Return the text of a Jupyter notebook's cells, code and markdown alike.

    Outputs are left out: they are rendered results (often base64 images), not
    text a reader wrote. Tika would read the file as JSON and return all of it.

    :param body: The ``.ipynb`` file.
    :returns: Each cell's source, in order, separated by a blank line, or None
        when the cells hold no text.
    :rtype: str | None
    :raises ValueError: The file is not a notebook (not JSON, or no cell list).
    """
    try:
        notebook = json.loads(body)
    # RecursionError: JSON nested deeper than the interpreter's stack.
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as error:
        msg = "Not a JSON notebook"
        raise ValueError(msg) from error
    if not isinstance(notebook, dict):
        msg = "Not a JSON notebook"
        raise ValueError(msg)
    # nbformat 4 lists cells at the top level; nbformat 3 inside worksheets.
    if isinstance(notebook.get("cells"), list):
        cells = notebook["cells"]
    elif isinstance(notebook.get("worksheets"), list):
        cells = [
            cell
            for sheet in notebook["worksheets"]
            if isinstance(sheet, dict) and isinstance(sheet.get("cells"), list)
            for cell in sheet["cells"]
        ]
    else:
        msg = "A notebook with no cell list"
        raise ValueError(msg)
    sources = []
    for cell in cells:
        if not isinstance(cell, dict):
            continue
        # nbformat 4 calls it source; nbformat 3 code cells call it input.
        source = cell.get("source", cell.get("input"))
        text = (
            "".join(part for part in source if isinstance(part, str))
            if isinstance(source, list)
            else source
        )
        if isinstance(text, str) and text.strip():
            sources.append(text.strip())
    return "\n\n".join(sources) or None
