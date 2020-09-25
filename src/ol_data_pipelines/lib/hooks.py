from dagster import failure_hook, success_hook


@success_hook(required_resource_keys={'healthchecks'})
def notify_healthchecks_io_on_success(context):
    context.resources.healthchecks.send_update()


@failure_hook(required_resource_keys={'healthchecks'})
def notify_healthchecks_io_on_failure(context):
    context.resources.healthchecks.send_update('fail')
