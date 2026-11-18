FROM astrocrpublic.azurecr.io/runtime:3.3-10

# dbt lives in its own virtualenv so its packages don't conflict with Airflow's.
# Cosmos (installed from requirements.txt) calls this dbt executable.
RUN python -m venv /usr/local/airflow/dbt_venv && \
    /usr/local/airflow/dbt_venv/bin/pip install --no-cache-dir \
        "dbt-core==1.12.7" "dbt-bigquery==1.12.3"

# Cosmos reads the dbt project when Airflow parses the DAGs; give it time
ENV AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT=120 \
    AIRFLOW__DAG_PROCESSOR__DAG_FILE_PROCESSOR_TIMEOUT=180
