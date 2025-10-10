"""EdxOrg partition definitions."""

from dagster import DynamicPartitionsDefinition

course_and_source_partitions = DynamicPartitionsDefinition(name="course_and_source")
