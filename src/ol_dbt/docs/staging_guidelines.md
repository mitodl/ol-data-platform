# dbt Data Staging Guidelines

The following considerations are all necssary phases of the "staging" process in dbt.
- Selecting tables and fields to stage
- Renaming fields
- Transforming timestamps
- Transforming values
- Testing with the dbt framework
- Documentation
- Addressing null values (or not addressing)

The guidelines below are an attempt to help prepare contributers on what to consider when they begin "staging" data.

### Selecting tables and fields to stage
Decisions on which tables and fields to "stage" should be determined by the issue being addressed. At times, table and field selections will be based upon downstream use, and at other times, part of building out our initial project structure. Guidelines:
- Seek clarity and ask questions via the issue being worked on.
- Explore data to help determine which tables are needed ([Redash](https://bi.odl.mit.edu/queries?order=-created_at&page=1&page_size=20) is a great tool for exploring tables).
- PII (Personally Identifying Information): _we want to ask ourselves internally, are these data worth risking PII exposure?_ If concerned, ask for clarifications.

### Renaming fields
Naming convention guidelines will be highlighted here, but ambiguity should be addressed in the discussion of the issue being worked on. Following [Emily Riederer's "field names as contracts"](https://emilyriederer.netlify.app/post/column-name-contracts/), she establishes a `<type>_<entity>_<column_name/details>`. Fields in the MITx Online users_user table would become:
- id: `id_user`
- username: `txt_user_username`
- email: `txt_user_email`
- is_active: `is_user_active`
- created_on: `tm_user_created_on`
- updated_on: `tm_user_updated_on`

### Transforming timestamps
All timestamp fields should be converted to [ISO8601](https://en.wikipedia.org/wiki/ISO_8601). For example, 2022-09-26T18:51:23Z.

### Transforming values
In some situations, we may want to tranform field values to specific types or to more human interpretable formats. Examples might include:
- Writing "CASE" logic to combine multiple boolean fields into a signle "string" field. Chapter 10 of the [Informed Company](https://learning.oreilly.com/library/view/the-informed-company/9781119748007/c10.xhtml#head-2-1) details some of these types of cases.
- Ints, floats, strings to their proper format where needed. We can likely leverage the dbt testing framework to help with this (as oppossed to making it a manual transformation).

### Testing with dbt
dbt has a [robust testing framework](https://docs.getdbt.com/docs/building-a-dbt-project/tests) that includes four
out of the box tests: `uniqueness`, `not_null`, `accepted_values`, and `relationships`. Guidelines:
- Include at least one test per table, focusing particularly on "primary keys" and the outcomes of transformed data.

### Documentation
Raw tables, staging models, and staging fields can all be documented directly in the `.yml` files maintained in directories containing our model `.sql`. Guidelines:
- All staging models should be documented in `.yml` files maintained in the corresponding model directories.
- Fields should be documented. If you are unable to determine the meaning of a field, seek clarity in the issue discussion or through other channels.

### Addressing Null Values (or not addressing)
For fields that are not transformed, please leave null values in place. When we transform the values of fields (e.g., converting multiple booleans to categorical values), we should account for null values. _We will have future discussions on this topic._

# Addendum
#### What is the Staging Layer
dbt guidelines recommend a three layer project organization for new dbt projects:
1. Staging: intial connection to data source, where we clean, rename, and transform fields. Leads to reusable data models.
2. Intermediate: more complex transformations that typically bring data together (i.e., joins).
3. Marts: customer facing data organized to meet specific business use cases.

Our project contains a "zero" layer that we call `raw`, which is an s3 bucket where we sync all relevant data from our front end apps, open edX instances, and other sources. Choosing to bring in data to the "staging" layer is the initial step that turns data into reusable models within a collaborative project structure.
