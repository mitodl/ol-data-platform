# Exposure contracts

Each JSON file here is what one application exposure reads from the warehouse:
the columns of every model it depends on, with each column's
`config.meta.cohort` declaration where the model has one and its
`config.meta.api_description` where the column has one. The files are
generated from the model YAML and committed, so an application's CI can check
itself against them without building the dbt project.

Do not edit them by hand. After changing a model an application exposure
depends on, or the exposure itself, run

```bash
ol-dbt exposures export
```

and commit the result. `ol-dbt validate --only exposure_contract` (part of dbt
PR CI) fails while a file is missing, out of date, or belongs to no exposure.

## Format

```json
{
  "generated_by": "ol-dbt exposures export",
  "exposure": "ol_analytics_api_b2b_dashboard",
  "models": {
    "mv_b2b_contract_utilization": {
      "columns": [
        {"name": "sso_organization_id", "cohort": {"role": "dimension"}},
        {"name": "certified_learners", "cohort": {"role": "secondary", "contained_in": "seats_consumed"}, "api_description": "Learners on the contract who earned a certificate that hasn't been revoked. ..."}
      ]
    }
  }
}
```

Only exposures of `type: application` get a file, and every `depends_on` entry
must be a `ref()`. The cohort roles are described in
`src/ol_dbt/models/b2b_analytics/_b2b_analytics__models.yml` and
`src/ol_dbt_cli/ol_dbt_cli/lib/cohort_policy.py`.

`api_description` is the description the application publishes for the
response field the column feeds, copied as written. A column has one only when
someone declared it, so a consumer has to treat its absence as "not declared"
rather than as an empty description. ol-analytics-api fails its contract test
when a field it returns from a column has a different description, or the
column has none. Its wording therefore changes here first: edit
`config.meta.api_description`, export, merge, then copy the file into the
application. Write a folded YAML block as `>-`: the export refuses the final
newline that `>` keeps.

## Consumers

| File | Read by |
|------|---------|
| `ol_analytics_api_b2b_dashboard.json` | ol-analytics-api `tenants/b2b_dashboard` |
| `ol_analytics_api_b2b_learner_records.json` | ol-analytics-api `tenants/b2b_learner_records` |

A consumer should read the file from `main`
(`https://raw.githubusercontent.com/mitodl/ol-data-platform/main/exposures/<name>.json`).
`main` can be ahead of what is built: a column added here exists in StarRocks
only after the next build of the view.
