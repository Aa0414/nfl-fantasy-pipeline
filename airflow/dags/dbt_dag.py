"""
Airflow DAG nfl_dbt_build: rebuilds staging + mart with dbt (via Cosmos) and runs all dbt tests.
Runs automatically whenever news, players or stats raw data is loaded.
max_active_runs=1 -> if two ingestion DAGs finish together, builds queue instead of overlapping.
"""
from datetime import timedelta

import pendulum
from airflow.sdk import Asset  # noqa: F401  (keeps Airflow's DAG-file detection happy)
from cosmos import DbtDag

from dbt_config import (
    EXECUTION_CONFIG,
    OPERATOR_ARGS,
    PROFILE_CONFIG,
    PROJECT_CONFIG,
    RAW_NEWS,
    RAW_PLAYERS,
    RAW_STATS,
    RENDER_CONFIG,
)

nfl_dbt_build = DbtDag(
    dag_id="nfl_dbt_build",
    project_config=PROJECT_CONFIG,
    profile_config=PROFILE_CONFIG,
    execution_config=EXECUTION_CONFIG,
    render_config=RENDER_CONFIG,
    operator_args=OPERATOR_ARGS,
    schedule=(RAW_NEWS | RAW_PLAYERS | RAW_STATS),
    start_date=pendulum.datetime(2026, 11, 1, tz="America/New_York"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["nfl", "dbt", "bigquery"],
)
