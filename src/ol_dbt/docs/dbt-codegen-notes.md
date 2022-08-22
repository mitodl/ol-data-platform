# dbt-codegen
dbt-codegen is run from the command line. It does require working dbt code, i.e., you can run this in the middle of writing elements of your dbt project. It outputs .yml (source) and .sql (models) to the command line output. You then copy/paste into a new file (or you could pipe it into a file, but I chose to review then copy/paste). There is probably a lot more we could do to automate aspects of bringing over new data sources.

## How to generate a source
I ran dbt-codegen to create relevant code for the Bootcamps source as it relates to Enrollments.
### Bootcamps source, with tables
`dbt run-operation generate_source --args '{"generate_columns": true, "include_descriptions": true, "schema_name": "ol_warehouse_raw_data", "database_name": "ol_data_lake_qa", "table_names":["raw__bootcamps__app__postgres__klasses_bootcamprunenrollment", "raw__bootcamps__app__postgres__klasses_bootcamprun", "raw__bootcamps__app__postgres__auth_user"]}'`

## How to generate base models
I ran dbt-codegen for the following models.
### klasses_bootcamprun
dbt run-operation generate_base_model --args '{"source_name": "ol_warehouse_raw_data", "table_name": "raw__bootcamps__app__postgres__klasses_bootcamprun"}'

### klasses_bootcamprunenrollment
dbt run-operation generate_base_model --args '{"source_name": "ol_warehouse_raw_data", "table_name": "raw__bootcamps__app__postgres__klasses_bootcamprunenrollment"}'

### auth_user
dbt run-operation generate_base_model --args '{"source_name": "ol_warehouse_raw_data", "table_name": "raw__bootcamps__app__postgres__auth_user"}'
