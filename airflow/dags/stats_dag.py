
from datetime import timedelta, datetime, timezone
from pathlib import Path

import pendulum
from airflow.sdk import dag, task

PROJECT_ID = "ba882-team03-kalshi"
BUCKET = "ba882-nfl-kalshi-raw"
LOCATION = "us-central1"
FUNCTION_URL = "https://nfl-ingest-cedewu2pea-uc.a.run.app"
SQL_DIR = Path("/usr/local/airflow/include/sql")


@dag(
    dag_id="nfl_weekly_stats",
    start_date=pendulum.datetime(
        2026, 10, 20, tz="America/New_York"
    ),
    schedule="0 8 * * 2",
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
    },
    tags=["nfl", "stats", "bigquery"],
)
def nfl_weekly_stats():

    @task
    def trigger_stats():
        import requests
        from google.auth.transport.requests import Request
        from google.oauth2 import id_token

        token = id_token.fetch_id_token(Request(), FUNCTION_URL)

        response = requests.get(
            FUNCTION_URL,
            params={"job": "stats"},
            headers={"Authorization": f"Bearer {token}"},
            timeout=540,
        )
        response.raise_for_status()

        if (
            "uploaded stats/player_week/" not in response.text
            or "uploaded stats/schedule/" not in response.text
        ):
            raise RuntimeError("Missing stats upload confirmation")

        print(response.text)

    @task
    def find_latest_snapshot():
        from google.cloud import storage

        client = storage.Client(project=PROJECT_ID)
        results = {}

        for name, prefix in {
            "player_week": "stats/player_week/season=2026/",
            "schedule": "stats/schedule/season=2026/",
        }.items():

            blobs = [
                b for b in client.list_blobs(
                    BUCKET, prefix=prefix
                )
                if b.name.endswith(".ndjson")
                and b.size is not None
                and b.size > 0
            ]

            if not blobs:
                raise RuntimeError(f"No valid files: {prefix}")

            latest = max(blobs, key=lambda b: b.name)
            results[name] = f"gs://{BUCKET}/{latest.name}"

            print(f"{name}: {results[name]}")

        return results

    @task
    def load_stats_to_bigquery(files):
        from google.cloud import bigquery

        client = bigquery.Client(
            project=PROJECT_ID,
            location=LOCATION,
        )

        tables = {
            "player_week": "raw.player_week_raw",
            "schedule": "raw.schedule_raw",
        }

        for source, table in tables.items():
            table_id = f"{PROJECT_ID}.{table}"

            config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                autodetect=True,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            )

            job = client.load_table_from_uri(
                files[source],
                table_id,
                job_config=config,
                location=LOCATION,
            )
            job.result()

            count = client.get_table(table_id).num_rows
            print(f"Loaded {table_id}: {count} rows")

            if count == 0:
                raise RuntimeError(f"Empty table: {table_id}")

    @task
    def run_staging_sql():
        from google.cloud import bigquery

        client = bigquery.Client(
            project=PROJECT_ID,
            location=LOCATION,
        )

        files = sorted((SQL_DIR / "staging").glob("*.sql"))

        if len(files) != 6:
            raise RuntimeError(
                f"Expected 6 staging files, found {len(files)}"
            )

        for file in files:
            print(f"Running staging: {file.name}")
            sql = file.read_text(encoding="utf-8")
            client.query(sql, location=LOCATION).result()

    @task
    def run_mart_sql():
        from google.cloud import bigquery

        client = bigquery.Client(
            project=PROJECT_ID,
            location=LOCATION,
        )

        files = sorted((SQL_DIR / "mart").glob("*.sql"))

        if len(files) != 5:
            raise RuntimeError(
                f"Expected 5 mart files, found {len(files)}"
            )

        for file in files:
            print(f"Running mart: {file.name}")
            sql = file.read_text(encoding="utf-8")
            client.query(sql, location=LOCATION).result()

    @task
    def verify_data():
        from google.cloud import bigquery

        client = bigquery.Client(
            project=PROJECT_ID,
            location=LOCATION,
        )

        for table in ["dim_player", "dim_game", "fact_player_week"]:
            table_id = f"{PROJECT_ID}.mart.{table}"
            query = f"SELECT COUNT(*) AS n FROM `{table_id}`"
            count = next(
                iter(client.query(query, location=LOCATION).result())
            ).n

            print(f"{table}: {count} rows")

            if count == 0:
                raise RuntimeError(f"Empty mart table: {table}")

    ingestion = trigger_stats()
    files = find_latest_snapshot()
    load = load_stats_to_bigquery(files)
    staging = run_staging_sql()
    mart = run_mart_sql()
    verify = verify_data()

    ingestion >> files
    load >> staging >> mart >> verify


nfl_weekly_stats()
