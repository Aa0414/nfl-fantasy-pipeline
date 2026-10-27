"""
Sleeper Players Pipeline
Tuesday and Saturday at 7:30 AM America/New_York.
"""

from pathlib import Path

import pendulum
from airflow.decorators import dag, task
from google.cloud import bigquery, storage


PROJECT = "ba882-team03-kalshi"
BUCKET = "ba882-nfl-kalshi-raw"
RAW_TABLE = f"{PROJECT}.raw.players_sleeper_raw"

SQL_DIR = Path("/usr/local/airflow/include/sql")


@dag(
    dag_id="nfl_players",
    schedule="30 7 * * 2,6",
    start_date=pendulum.datetime(
        2026, 10, 1, tz="America/New_York"
    ),
    catchup=False,
    max_active_runs=1,
    tags=["nfl", "players", "sleeper"],
)
def players_pipeline():

    @task
    def find_latest_snapshot():
        client = storage.Client(project=PROJECT)
        blobs = client.list_blobs(
            BUCKET, prefix="players/sleeper/"
        )

        files = sorted(
            blob.name
            for blob in blobs
            if blob.name.endswith("/players.ndjson")
        )

        if not files:
            raise ValueError("No Sleeper snapshot found in GCS")

        return f"gs://{BUCKET}/{files[-1]}"

    @task
    def load_players_raw(gcs_uri):
        client = bigquery.Client(project=PROJECT)

        # Preserve the existing BigQuery table schema.
        schema = client.get_table(RAW_TABLE).schema

        config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

        job = client.load_table_from_uri(
            gcs_uri,
            RAW_TABLE,
            job_config=config,
        )
        job.result()
        return client.get_table(RAW_TABLE).num_rows

    @task
    def refresh_players_tables():
        client = bigquery.Client(project=PROJECT)

        sql_files = [
            SQL_DIR / "staging/00_norm_name.sql",
            SQL_DIR / "staging/04_stg_players.sql",
            SQL_DIR / "mart/01_dim_player.sql",
        ]

        for sql_file in sql_files:
            sql = sql_file.read_text()
            client.query(sql).result()

    @task
    def verify_players():
        client = bigquery.Client(project=PROJECT)

        tables = [
            "raw.players_sleeper_raw",
            "staging.stg_players",
            "mart.dim_player",
        ]

        for table in tables:
            sql = f"SELECT COUNT(*) AS n FROM `{PROJECT}.{table}`"
            count = next(iter(client.query(sql).result())).n

            print(f"{table}: {count} rows")

            if count == 0:
                raise ValueError(f"Empty table: {table}")

    snapshot = find_latest_snapshot()
    loaded = load_players_raw(snapshot)
    refreshed = refresh_players_tables()
    verified = verify_players()

    loaded >> refreshed >> verified


players_pipeline()
