"""Shared Cosmos settings and data assets for the NFL pipeline."""
import os
from pathlib import Path

from airflow.sdk import Asset
from cosmos import ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import TestBehavior

AIRFLOW_HOME = Path(os.environ.get("AIRFLOW_HOME", "/usr/local/airflow"))

# ---- Assets: each ingestion DAG "publishes" one; nfl_dbt_build listens to all three
RAW_NEWS = Asset("bigquery://ba882-team03-kalshi/raw/news")
RAW_PLAYERS = Asset("bigquery://ba882-team03-kalshi/raw/players_sleeper_raw")
RAW_STATS = Asset("bigquery://ba882-team03-kalshi/raw/stats")

# ---- Cosmos
PROJECT_CONFIG = ProjectConfig(
    dbt_project_path=(Path(__file__).parent / "dbt" / "fantasy_dbt").absolute().as_posix(),
    install_dbt_deps=True,          # installs dbt_utils from packages.yml
)

# Prod target: staging models -> `staging` dataset, marts -> `mart` dataset
PROFILE_CONFIG = ProfileConfig(
    profile_name="fantasy_dbt",
    target_name="prod",
    profiles_yml_filepath=AIRFLOW_HOME / "include" / "dbt" / "profiles.yml",
)

# dbt installed in its own virtualenv by the Dockerfile
EXECUTION_CONFIG = ExecutionConfig(
    dbt_executable_path=str(AIRFLOW_HOME / "dbt_venv" / "bin" / "dbt"),
)

# Each model gets a "run" task followed by a "test" task; a failed test stops downstream models
RENDER_CONFIG = RenderConfig(
    test_behavior=TestBehavior.AFTER_EACH,
)

OPERATOR_ARGS = {
    "install_deps": True,
    "emit_datasets": False,   # we publish our own assets above
    "queue": "dbt",           # run dbt on the A10 "dbt" worker queue (2 vCPU, 4 GiB)
}
