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




## 4 Metrics & Dashboards

*What are the main metrics we should be measuring? For example, when interacting with an external system, it might be the external system latency. When adding a new table, how fast would it fill up?*

## 5 Drawbacks

*Are there any reasons why we should not do this? Here we aim to evaluate risk and check ourselves.*

## 6 Alternatives

*What are other ways of achieving the same outcome?*

## 7 Potential Impact and Dependencies

*Here, we aim to be mindful of our environment and generate empathy towards others who may be impacted by our decisions.*

- *What other systems or teams are affected by this proposal?*
- *How could this be exploited by malicious attackers?*

## 8 Unresolved questions

*What parts of the proposal are still being defined or not covered by this proposal?*

## 9 Conclusion

*Here, we briefly outline why this is the right decision to make at this time and move forward!*
