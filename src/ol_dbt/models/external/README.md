The purpose of this grouping of models is to separate them from the others (staging, intermediate, and marts). The models contained here are generated for our external consumers including institutional research (IRX).

Source tables will still be defined under their respective source files in the staging directories for each deployment (ex. src/ol_dbt/models/staging/mitxonline/_mitxonline__sources.yml). Any missing raw tables as noted from the errors when running dbt build will be staged in Airbyte.

The models contained here will not be generated in separate stages and are grouped by the external consumer of those models.

The new IRX models were written to match the SQL file output of the edx-analytics-exporter that is being used to ingest our data. This was done because our existing pipelines were written to conform to a legacy interface that is no longer being used. We now need to support a data consumer that is using https://github.com/MIT-IR/simeon/ to process our outputs.
