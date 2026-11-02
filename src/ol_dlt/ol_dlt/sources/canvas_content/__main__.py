"""Run one load of the Canvas content source, for the active profile."""

from ol_dlt.sources.canvas_content import build_source, canvas_content_pipeline

if __name__ == "__main__":
    print(canvas_content_pipeline.run(build_source()))  # noqa: T201
