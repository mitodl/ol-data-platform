"""Which executor lakehouse runs use in each environment.

The default multiprocess executor runs every step inside the run worker pod. When
that pod dies, run monitoring resumes the run in a new pod, which starts the plan
over from its first step: a full dbt build that had already finished is run again,
replacing every table it built. `k8s_job_executor` runs each step in its own pod
instead. A resumed run worker rebuilds the plan's state from the event log and
reattaches to step pods that are still running, so a dbt step outlives the run
worker that launched it.

Step pods inherit the run launcher's and the code location's container config
(service account, secrets, resources, priority class), deep-merged with the
step config below. The user-deployments chart's default Role (rbacEnabled) is
what lets the run worker's dagster-user-code service account create those Jobs.
`enable_owner_references` stays at its default of False: an owner reference to the
run pod would let Kubernetes garbage-collect the step Jobs when the run pod is
deleted, which is the failure this exists to survive.

Sizing, from QA step pods over the 7 days to 2026-11-09: the steps other than
dbt that ran there (Iceberg maintenance, orphan sweeps, QA drift checks) peaked
at 0.84 GB and 0.33 CPU, most of it importing the code location. They and the
Superset steps, which make API calls and did not run in that window, get a
quarter-CPU and 1Gi request instead of the location's 1 CPU / 2Gi, which every
step pod would otherwise inherit. full_dbt_project peaked at 1.33 GB and 1.9
CPU and keeps the location's resources, as do starrocks_dbt_assets and the
daily dbt docs and source-freshness ops, none of which has run as a step pod
yet.

A dbt step is the expensive one to lose: a killed dbt pod fails the step and
run_retries re-runs the whole selection. Run pods use the dagster-run priority
class (value -100), which any ordinary pod can preempt, so the dbt steps run at
DBT_STEP_PRIORITY_CLASS instead. ol-infrastructure defines it, and covers step
pods with the same disruption budget as run workers.

Rolled out per environment, QA first, like DBT_AUTOMATION_ENVIRONMENTS. Production
waits until QA runs have shown step pods come up and clean up as expected.

DAGSTER_ENV alone does not say whether there is a cluster to launch step pods in:
docker-compose runs the local lakehouse with DAGSTER_ENVIRONMENT=qa against the
default run launcher, where k8s_job_executor fails at init trying to load in-cluster
config. So the executor also requires KUBERNETES_SERVICE_HOST, which the kubelet
sets in every pod.
"""

from dagster import ExecutorDefinition
from dagster_k8s import k8s_job_executor

STEP_POD_ENVIRONMENTS: frozenset[str] = frozenset({"qa"})

# The ops of the two @dbt_assets definitions (assets/lakehouse/dbt.py and
# dbt_starrocks.py). per_step_k8s_config is keyed by op name.
DBT_STEP_OPS: tuple[str, ...] = ("full_dbt_project", "starrocks_dbt_assets")
# Daily ops in assets/lakehouse/dbt.py that parse the whole dbt project too
# (docs generate, source freshness). They need a dbt build's memory, but losing
# one costs a day's artifacts, not a rebuild, so they stay preemptible.
DBT_CLI_OPS: tuple[str, ...] = (
    "generate_dbt_docs_artifacts",
    "check_dbt_source_freshness",
)
DBT_STEP_PRIORITY_CLASS = "dagster-dbt-step"

LIGHT_STEP_RESOURCES = {
    "requests": {"cpu": "250m", "memory": "1Gi"},
    "limits": {"cpu": "1", "memory": "2Gi"},
}
# A copy of the lakehouse code location's resources in ol-infrastructure
# (applications/dagster/__main__.py). It has to be spelled out, or the light
# default above would apply to dbt steps too; raise both together.
DBT_STEP_RESOURCES = {
    "requests": {"cpu": "1", "memory": "2Gi"},
    "limits": {"cpu": "4", "memory": "8Gi"},
}

step_pod_executor = k8s_job_executor.configured(
    {
        "step_k8s_config": {"container_config": {"resources": LIGHT_STEP_RESOURCES}},
        "per_step_k8s_config": {
            **{
                op: {"container_config": {"resources": DBT_STEP_RESOURCES}}
                for op in DBT_CLI_OPS
            },
            **{
                op: {
                    "container_config": {"resources": DBT_STEP_RESOURCES},
                    "pod_spec_config": {"priority_class_name": DBT_STEP_PRIORITY_CLASS},
                }
                for op in DBT_STEP_OPS
            },
        },
    },
    name="lakehouse_step_pod_executor",
)


def executor_for_environment(
    dagster_env: str, *, in_kubernetes: bool
) -> ExecutorDefinition | None:
    """Return the executor for ``dagster_env``.

    :param dagster_env: The DAGSTER_ENV the code location is loaded in.
    :param in_kubernetes: Whether the code location runs in a Kubernetes pod.
    :returns: ``step_pod_executor`` in STEP_POD_ENVIRONMENTS when running in
        Kubernetes, otherwise None, which leaves Dagster's default
        multiprocess executor in place.
    :rtype: ExecutorDefinition | None
    """
    if in_kubernetes and dagster_env in STEP_POD_ENVIRONMENTS:
        return step_pod_executor
    return None
