## dbt

This project is configured to use Trino as the warehouse engine. The profile information
is defined in the repository with environment variables for the username and
password. There are separate profiles defined for QA and Production environments.

### Models Structure
dbt models (SQL files) are structured into 3 directories under ol_dbt/models, within each directory, we organize the sql files by product
(e.g. mitxonline, mitxpro, etc.)

- Staging - where we pull in data from sources, clean data and rename field, etc.

- Intermediate - where we do more complex joins and aggregations

- Marts - contains customer facing data that is organized to meet specific needs



### Local Setup
- Clone the ol-data-platform repo, and navigate to dbt working directory
  ```
  git clone git@github.com:mitodl/ol-data-platform.git
  cd ol-data-platform/src/ol_dbt
  ```
- Install dbt-trino adapter

  See [dbt installation from Starburst](https://docs.starburst.io/data-consumer/clients/dbt.html#installation-overview) for detail
  ```
  pip install dbt-trino
  ```
- Setup two environment variables that will be referenced in `src/ol_dbt/profiles.yml` config

  ```
  export DBT_TRINO_USERNAME=<USERNAME>/accountadmin
  export DBT_TRINO_PASSWORD=<PASSWORD>
  ```
  You need Starburst credentials to complete this step, which can be requested from OL Devops

- Test dbt connection
  ```
  $ dbt debug
  21:50:08  Running with dbt=1.2.2
  dbt version: 1.2.2
  python version: 3.9.15
  ---------

  Configuration:
    profiles.yml file [OK found and valid]
    dbt_project.yml file [OK found and valid]

  Required dependencies:
   - git [OK found]

  Connection:
    host: mitol-ol-xxxxxx.trino.galaxy.starburst.io
    port: 443
    user: xxxxx@mit.edu/accountadmin
    database: ol_data_lake_qa
    schema: ol_warehouse_qa
    cert: None
    prepared_statements_enabled: True
    Connection test: [OK connection ok]

  All checks passed!

  ```
  By default, running dbt command from your local machine connects to our QA data lake, but you could specify different environment by passing --target.
  ```
  dbt debug --target <qa|production>
  ```

- Download dependencies

  The `dbt deps` will pull the most recent version of the dependencies listed in `src/ol_dbt/packages.yml`
  ```
  dbt deps
  ```

### Running and testing dbt
It's recommended that when you make a change to a model, you run both `dbt run` and `dbt test` commands to ensure that model is recreated and tests are passed

Note that your working directory should be `ol_dbt`, Otherwise you need to specify `--project-dir` when running dbt command.

- dbt run
  - Run all the models in our open_learning project with either one of the following two commands
     ```
    dbt run
    dbt run --select open_learning
    ```
  - Run specific models
    ```
    # Run a specific model e.g. int__mitxonline__users
    dbt run --select int__mitxonline__users

    # Run all the models in a specific directory
    # e.g. staging models under mitxonline directory
    dbt run --select stagings.mitxonline
    ```
  - Refresh models

    If you are making a change to existing model, you want to reprocess and recreate the model by passing
      --full-refresh flag to the run command
       ```
       dbt run --select int__mitxonline__users --full-refresh
       ```
- dbt test

  `dbt test` runs the tests defined in `.yml` file under model directory. It expects that you have already run the command to create the model you are testing
    ```
        # e.g. to run the tests defined for int__mitxonline__users in src/ol_dbt/models/intermediate/mitxonline/_int_mitxonline__models.yml
        dbt test --select int__mitxonline__users

        # to run all the tests defined in src/ol_dbt/models/staging/mitxonline/_stg_mitxonline__models.yml
        dbt test --select staging.mitxonline
     ```


### Other dbt Commands

- dbt build

  To run a dbt build you can use the command:
  ```
  dbt build --target <qa|production>
  ```

  The `dbt build` command will build and test the following resources
  - run models: `dbt run` builds all the models defined in this project
  - test tests: `dbt test` test all the models defined in this project
  - snapshot snapshots: : `dbt snapshot` executes "snapshot" jobs defined in snapshots directory
  - seed seeds: `dbt seed` loads csv files defined in seeds directory

- dbt clean

  `dbt clean` command will delete all folders specified in the `clean-targets` list specified in `dbt_project.yml`
  ```
  clean-targets:
  - "target"
  - "dbt_packages"
  ```
  So you can use this to delete the dbt_packages and target directories.

- dbt docs

  dbt docs allows you to generate the documentation for this project, and serve it up locally
  ```
  dbt docs generate
  dbt doc serve
  ```
  The local webserver is rooted in your `target/` directory with default port 8080, you can specify a different port using `--port` flag to serve command

See [dbt Command reference](https://docs.getdbt.com/reference/dbt-commands) for all available commands

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
