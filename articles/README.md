# Context Center articles

Each file here publishes one markdown document from this repository to
OpenMetadata's Context Center as an article, attached to the tables it
describes. Readers find it on those tables' pages, and OpenMetadata's AI
features read it as context for them. The markdown stays where it is and is
edited in pull requests: an edit made in the OpenMetadata UI is overwritten the
next time the file changes.

Publish documents written for people who query the tables (what a column
means, how models are keyed, a contract a consumer depends on). Contributor,
design and operations docs stay in git only.

## Format

```yaml
name: learn-marts-contract            # the article's name in OpenMetadata, lowercase and dashes
displayName: Learn integration schema contract
source: docs/learn_marts_contract.md  # repository-relative markdown
owners: [{type: team, name: data-platform}]
related:                              # what the article is attached to
- {type: table, dbt_model: "integrations__learn__*"}   # a model, seed or snapshot name, or an fnmatch pattern
- {type: table, dbt_source: ol_warehouse_raw_data.raw__mitlearn__app__postgres__learning_resources_learningresource}
- {type: table, fqn: "Glue.610119931565.ol_warehouse_production_external.some_table"}
```

`displayName` and `owners` are set when the article is created; a later change
to them in this file does not reach OpenMetadata.

## Publishing

The `ol-data-articles-sync` Concourse pipeline runs
`ol-dbt articles sync` when this directory changes and daily after the
manifest upload, as articles-sync-bot. To see what it would do:

```bash
uv run --package ol-dbt-cli ol-dbt articles sync --dry-run \
    --service "Starburst Galaxy" --manifest /path/to/production/manifest.json
```

with `OM_SERVER_URL` and `OM_BOT_JWT_TOKEN` set. An article is updated when
its source file or its list of related tables changes. A change to a file in
this directory publishes within minutes; a change to only the source markdown
is published by the next daily run. A table OpenMetadata has not catalogued yet
is skipped with a warning and attached by a later run, and an article none of
whose tables is catalogued is not published until one is.

Relative links in the markdown are pointed at the file on GitHub, since they
mean nothing in OpenMetadata.
