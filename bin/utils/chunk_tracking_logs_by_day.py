#!/usr/bin/env python
"""
Our tracking logs have gone through a few iterations of methods for loading them to S3.
This is largely due to different agents being used for shipping the logs.  As a result,
the path formatting for those logs is not consistent across time boundaries.

This script is designed to take a source bucket and a destination bucket, and process
all files that are in the root of the bucket to be located in path prefixes that are
chunked by date.
"""

import sys
from datetime import UTC, datetime, timedelta
from typing import Annotated, Optional

import typer
from boto3 import client, resource


def date_chunk_files(  # noqa: PLR0913
    source_bucket: Annotated[
        str,
        typer.Argument(
            help="The source bucket that tracking logs will be copied or moved from"
        ),
    ],
    dest_bucket: Annotated[
        str, typer.Argument(help="The bucket that the tracking logs will be written to")
    ],
    start_date: Annotated[
        str,
        typer.Option(
            help="The date of the earliest tracking log to process "
            "(based on the formatted file name). In %Y-%m-%d format"
        ),
    ] = "2017-01-01",
    end_date: Annotated[
        Optional[str],
        typer.Option(
            help="The date of the last tracking log to process "
            "(based on the formatted file name). In %Y-%m-%d format"
        ),
    ] = None,
    dry_run: Annotated[  # noqa: FBT002
        bool,
        typer.Option(
            help="Set to True to just see what the source and destination paths "
            "will be without performing any modifications"
        ),
    ] = True,
    destructive: Annotated[  # noqa: FBT002
        bool,
        typer.Option(
            help="Perform a `move` operation instead of `copy` to clear the source "
            "object out of its original location"
        ),
    ] = False,
    cleanup: Annotated[  # noqa: FBT002
        bool,
        typer.Option(
            help="Run in destructive mode, but don't copy the files to the destination."
            " This is intended to be run after using `--no-dry-run` and "
            "`--no-destructive`. This way we can clear out old files without affecting "
            "the modified timestamp on the destination files so that they don't get "
            "re-processed by Airbyte."
        ),
    ] = False,
):
    s3 = client("s3")
    s3_resource = resource("s3")

    sbucket = s3_resource.Bucket(source_bucket)
    dbucket = s3_resource.Bucket(dest_bucket)

    key_date = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=UTC)
    increment = timedelta(days=1)
    stop_date = (
        datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=UTC)
        if end_date
        else datetime.now(UTC)
    )
    while key_date < stop_date:
        date_str = key_date.strftime("%Y-%m-%d")
        sys.stdout.write(f"Processing files for {date_str}\n\n")
        skeys = s3.list_objects_v2(
            Bucket=source_bucket,
            Delimiter="/",
            Prefix=f"logs/{date_str}",
        )
        copy_map = {}
        for obj in skeys.get("Contents", []):
            obj_key = obj["Key"]
            # Skip keys that are already nested into a per-day directory
            if len(obj_key.split("/")) > 2:  # noqa: PLR2004
                continue
            # Remove the existing `logs/` prefix from the object key for the destination
            copy_map[obj_key] = f"logs/{date_str}/{obj_key.split('/', maxsplit=1)[-1]}"

        if dry_run:
            sys.stdout.writelines(
                ("\n".join((f"{k} -> {v}" for k, v in copy_map.items())), "\n")
            )
        else:
            for srckey, destkey in copy_map.items():
                if not cleanup:
                    sys.stdout.write(
                        f"Copying {source_bucket}/{srckey} to {dest_bucket}/{destkey}\n"
                    )
                    dbucket.copy({"Bucket": source_bucket, "Key": srckey}, destkey)
                if destructive:
                    if cleanup and s3.list_objects_v2(
                        Bucket=dest_bucket, Prefix=destkey
                    ).get("Contents"):
                        sys.stdout.write(f"Deleting {obj_key} from {source_bucket}\n")
                        sbucket.delete_objects(
                            Delete={"Objects": [{"Key": srckey}], "Quiet": False}
                        )
                    else:
                        sys.stdout.write(
                            f"Refusing to delete {source_bucket}/{srckey} because it "
                            f"hasn't yet been copied to {dest_bucket}/{destkey}\n"
                        )
        key_date += increment


if __name__ == "__main__":
    typer.run(date_chunk_files)
