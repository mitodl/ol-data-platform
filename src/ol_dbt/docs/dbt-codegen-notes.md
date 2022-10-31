# dbt-codegen
dbt-codegen is run from the command line. It requires working dbt code, i.e., you can run this in the middle of writing elements of your dbt project. It generates .yml (source) and .sql (models) to the command line output. You then copy/paste into a new file (or you could pipe it into a file). There is probably a lot more we could do to automate aspects of bringing over new data sources.

## Generate source
Use macro `generate_source` to generate YAML for a source, which can then be copied/pasted into a schema file

Arguments:
- `schema_name` (required): The schema name that contains the source data. e.g. `ol_warehouse_qa_raw`
- `database_name` (optional, default=target.database): The database that the source data is in. e.g. `ol_data_lake_qa`
- `table_names` (optional, default=none): A list of tables to generate the source definitions for. e.g. "table_names":["TABLE1", "TABLE2"]
- `generate_columns` (optional, default=False): Whether you want to add the column names to the source definition.
- `include_descriptions` (optional, default=False): Whether to add description placeholders to the source definition.
- `table_pattern` (optional, default='%'): A table prefix / postfix to subselect from all available tables within a given schema.
- `exclude` (optional, default=''): A string to exclude from the selection criteria
- `name` (optional, default=schema_name): The name of the source

### Usage
```
dbt run-operation generate_source --args '{"generate_columns": True, "include_descriptions": True, "schema_name": "ol_warehouse_qa_raw", "database_name": "ol_data_lake_qa",  "table_names":["raw__bootcamps__app__postgres__klasses_bootcamprun"]}'
```
Note that for multiple arguments, use the dict syntax {arg_name : value}

The YAML for the above source will be logged to the command line like this:
```
version: 2

sources:
  - name: ol_warehouse_qa_raw
    description: ""
    tables:
      - name: raw__bootcamps__app__postgres__klasses_bootcamprun
        description: ""
        columns:
          - name: id
            description: ""
          - name: title
            description: ""
          - name: source
            description: ""
          - name: run_key
            description: ""
          - name: end_date
            description: ""
          - name: start_date
            description: ""
          - name: bootcamp_id
            description: ""
          - name: bootcamp_run_id
            description: ""
          - name: novoed_course_stub
            description: ""
          - name: allows_skipped_steps
            description: ""

```

## Generate base models
Use macro `generate_base_model` to generate the SQL for a base model, which you can then paste into a model

### Usage
Run the following command to generate SQL for staging model from source table `raw__bootcamps__app__postgres__klasses_bootcamprun`
```
dbt run-operation generate_base_model --args '{"source_name": "ol_warehouse_raw_data", "table_name": "raw__bootcamps__app__postgres__klasses_bootcamprun"}'
```

The SQL for staging model will be logged to the command line like this:
```

with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__bootcamps__app__postgres__klasses_bootcamprun') }}

),

renamed as (

    select
        id,
        title,
        source,
        run_key,
        end_date,
        start_date,
        bootcamp_id,
        bootcamp_run_id,
        novoed_course_stub,
        allows_skipped_steps

    from source

)

select * from renamed
```

## Generate model YAML
This macro `generate_model_yaml` generates the YAML for a model, which you can then paste into a schema.yml file

Arguments:
- model_name (required): The model you wish to generate YAML for.
- upstream_descriptions (optional, default=False): Whether you want to include descriptions for identical column names from upstream models.


### Usage
Run the following command to generate YAML for the intermediate MITxOnline course runs model, it inherits the column description from staging model `stg__mitxonline__app__postgres__courses_courserun`
```
dbt run-operation generate_model_yaml --args '{"model_name": "int__mitxonline__course_runs", "upstream_descriptions": true}'
```
The YAML for above model will be logged to the command line like this:
```
version: 2

models:
  - name: int__mitxonline__course_runs
    description: ""
    columns:
      - name: courserun_id
        description: "int, primary key representing a single MITx Online course run"

      - name: course_id
        description: "int, foreign key to courses_course representing a single MITx Online course"

      - name: courserun_title
        description: "str, title of the course run"

      - name: courserun_readable_id
        description: "str, Open edX Course ID formatted as course-v1:{org}+{course code}+{run_tag}"

      - name: courserun_tag
        description: "str, unique string that identifies a single run for a course. E.g. 1T2022"

      - name: courserun_url
        description: "str, url location for the course run in MITx Online"

      - name: courserun_start_on
        description: "timestamp, specifying when the course begins"

      - name: courserun_end_on
        description: "timestamp, specifying when the course ends"

      - name: courserun_enrollment_start_on
        description: "timestamp, specifying when enrollment starts"

      - name: courserun_enrollment_end_on
        description: "timestamp, specifying when enrollment ends"

      - name: courserun_upgrade_deadline
        description: "timestamp, specifying the date time beyond which users can not enroll in paid course mode"

      - name: courserun_is_self_paced
        description: "boolean, indicating whether users can take this course at their own pace and earn certificate at any time"

      - name: courserun_is_live
        description: "boolean, indicating whether the course run is available to users on MITx Online website"

```
