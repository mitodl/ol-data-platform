"""Coverage for the content-hash skip-if-unchanged guard in the archive op.

object_key is a sha256 of the file contents, so an existing object at that
path is guaranteed byte-identical to the local archive_file -- re-uploading
it would just assign S3 a fresh ETag to the same key, racing with any
in-flight edxorg_s3 dlt read (see edxorg_archive.py's
_skip_upload_if_unchanged docstring for the full mechanism).
"""

from edxorg.assets.edxorg_archive import _skip_upload_if_unchanged


def test_skips_and_deletes_when_object_already_exists(tmp_path):
    archive_file = tmp_path / "archive.tsv"
    archive_file.write_text("id\tuser_id\n1\t10\n")
    existing_object = tmp_path / "already-uploaded.tsv"
    existing_object.write_text("id\tuser_id\n1\t10\n")

    assert _skip_upload_if_unchanged(archive_file, str(existing_object)) is True
    assert not archive_file.exists()


def test_does_not_skip_or_delete_when_object_is_new(tmp_path):
    archive_file = tmp_path / "archive.tsv"
    archive_file.write_text("id\tuser_id\n1\t10\n")
    new_object_path = tmp_path / "not-yet-uploaded.tsv"

    assert _skip_upload_if_unchanged(archive_file, str(new_object_path)) is False
    assert archive_file.exists()
