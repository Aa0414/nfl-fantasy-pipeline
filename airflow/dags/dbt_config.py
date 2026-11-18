"""Shared Cosmos settings for running the fantasy_dbt project from Airflow."""
import os
from pathlib import Path

from cosmos import ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import TestBehavior

AIRFLOW_HOME = Path(os.environ.get("AIRFLOW_HOME", "/usr/local/airflow"))

# The dbt project inside the repo: airflow/dags/dbt/fantasy_dbt
PROJECT_CONFIG = ProjectConfig(
    dbt_project_path=AIRFLOW_HOME / "dags" / "dbt" / "fantasy_dbt",
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
