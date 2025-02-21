from dagster import DailyPartitionsDefinition

# Define daily partitions
TRINO_DAILY_PARTITIONS = DailyPartitionsDefinition(start_date="2023-01-01")
