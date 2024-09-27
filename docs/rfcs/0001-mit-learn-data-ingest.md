# [RFC Template] Title

**Authors:**

- @blarghmatey

## 1 Executive Summary

The MIT Learn course catalog and discovery tool requires a reliable and maintainable
method for ingesting data pertaining to course offerings and associated resources. This
document addresses the architectural approach for providing that capability.

## 2 Motivation

The current approach to data ingestion and integration for the MIT Learn application is
reliant on the Celery task queue. While it is functional, it has several shortcomings:
- Tracking progress and estimating time to completion for any given task/pipeline is
  limited
- There is no out-of-the-box method of doing any task dependency chaining. This leads to
  either time-based triggers with the hope that a given task run will complete before
  the next task needs that data, or duplication of logic.
- There is no easy method of doing incremental loading of data
- The extra compute load of ETL tasks contribute to cost overruns in the Heroku
  environment, requiring manual management overhead of the worker instanaces to avoid
  that problem.

Another shortcoming of the data loading being owned by the Learn application is that it
limits the applicability of that data. By moving the data flows into the data platform
it broadens the scope of use cases for that informtion. Additionally, there is already
processing of data from many of the source systems that Learn relies on being done inthe
platform, meaning that we are duplicating effort.

Migrating the data flows into the data platform reduces duplication of effort, expands
the possible applications of the gathered data, and provides improved visibility and
cross-task dependency control.

## 3 Proposed Implementation

### Overview
There are two main categories of data ingestion/integration being executed in the
"Learn" application. These are "learning resources" and "news". Ultimately, all of the
data being ingested ends up in a tabular format that is stored in the application
database. There is already an established flow for ingesting and transforming tabular
data in the data platform (namely Airbyte and dbt). From that perspective, the data
platform is a logical place to perform this data ingestion and integration work, with
the added benefit that the integrated data is available for any and all other use cases
without requiring direct integration with the "Learn" application or its database. An
additional benefit of migrating the data ingestion to the data platform is that the ELT
(Extract, Load, and _then_ Transform) approach allows for access to the raw structure of
the data for use cases where that is desired, rather than only providing the already
transformed structure of the data.

The other work that is done by the "Learn" application is to populate a search index in
the associated [OpenSearch](https://opensearch.org/) cluster. In the current
implementation there are frequent bulk rewrites of the index from PostgreSQL to
OpenSearch. This re-index operation is also managed by Celery and places substantial
load on both the database and the search cluster. Because the application doesn't
explicitly track what data changed and when, it necessitates this naive approach. The
data platform is also capable of populating data into an OpenSearch cluster based on
changes in content and structure of the upstream data.

Migrating the data flows from being "owned" by the Learn application to operating via
the data platform consists of:
- Ingest all raw data into the data lakehouse via Airbyte connections
- Produce warehouse tables that are structurally equivalent to the data models currently
  being used by Learn
- Provide access to these tables to the Learn application (this is where most of the
  nuance lies)

Another piece of the overall data flow which is important to address is creation and
maintenance of the search index. This _can_ continue to be managed by the Learn
application, but would also benefit from being controlled through the data
platform. Because the data platform has an understanding of what data is changing and
when, it can more intelligently and incrementally maintain the index.

The key requirement for making use of the collected data in the context of the Learn
application is to have a shared representation between what is created by the data
platform and what is consumed by the application. Broadly speaking, this will be
achieved through a shared library of the schema representation for the data. Whether
this is a JSON Schema, Pydantic model, Python dataclass, etc. is subject to
experimentation, but the conceptual approach is straightforward.

### Ingestion
The majority of the data being processed is structured data sources from databases and
APIs. For data sourced from other applications that we control, there are already
existing data feeds into the warehouse environment. For REST API based sources, a
majority of them can easily be ingested via Airbyte using their [connector
builder](https://docs.airbyte.com/connector-development/connector-builder-ui/overview). For
GraphQL sources we can implement client logic in Dagster code (perhaps using [Ariadne
Codegen](https://github.com/mirumee/ariadne-codegen)).

Another source of content for Learn is from unstructured sources (e.g. PDFs, HTML text,
etc.). For extraction of data from these sources we rely on Tika to retrieve the
text. We will continue using Tika for that extraction process, but the management of
that interaction will be encapsulated as a Dagster
[asset](https://docs.dagster.io/concepts/assets/software-defined-assets).

### Transformation
The transformation of the collected data will largely be managed through creation of dbt
models. The final representation of the table structures can be tailored to meet the
needs of the Learn application. The benefit of this approach is that the Learn-oriented
tables are no longer the only possible use of the collected data, and alternative
structures can be generated to suit other use cases (e.g. business analytics, AI context
embedding, knowledge graphs, etc.). For cases where SQL is insufficiently expressive to perform a
transformation we still have access to Python and other arbitrary compute to manipulate
the data and write it back into the warehouse layer.

### Delivery
The final stage of the data flow is to deliver the processed information to end
users. This is done via the Django implementation of Learn which relies on a combination
of the Postgres tables and OpenSearch indexes that store the information. There are
numerous paths to routing this data, with the most direct being to allow the data
platform to manage the writes to the Postgres and OpenSearch layers.

## 4 Measuring Results

The core metrics that we are driving toward with this migration is to improve visibility
of data flows and reduce the error rates related to incomplete or out of sync data. The
ways that these shortcomings manifest currently are:
- Long and unpredictable execution times for data loads
- Lack of visibility into what data was loaded and when
- Lack of error recovery when long-running tasks are interrupted (e.g. Heroku dyno
  restarts)

We don't currently have any concrete metrics on the rate of occurrence or the impact of
these shortcomings. If desired we can work to quantify these details in a before and
after manner.

## 5 Drawbacks

*Are there any reasons why we should not do this? Here we aim to evaluate risk and check ourselves.*



## 6 Alternatives

*What are other ways of achieving the same outcome?*

## 7 Potential Impact and Dependencies

*Here, we aim to be mindful of our environment and generate empathy towards others who may be impacted by our decisions.*

- *What other systems or teams are affected by this proposal?*
- *How could this be exploited by malicious attackers?*

- need to train developers on use of Dagster and dbt
- developer environment complexity/data access

## 8 Unresolved questions

*What parts of the proposal are still being defined or not covered by this proposal?*
- path for loading data into application
- developer experience - where does dagster and dbt code live?

## 9 Conclusion

*Here, we briefly outline why this is the right decision to make at this time and move forward!*
