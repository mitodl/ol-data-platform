from dagster import DynamicPartitionsDefinition

b2b_organization_list_partitions = DynamicPartitionsDefinition(
    name="b2b_organization_list"
)
