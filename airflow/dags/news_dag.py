from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import pendulum
from airflow.sdk import dag, task, CronTriggerTimetable

PROJECT = "ba882-team03-kalshi"
BUCKET = "ba882-nfl-kalshi-raw"
LOCATION = "us-central1"
SQL_DIR = Path(__file__).resolve().parent / "sql"
if not SQL_DIR.exists():
    SQL_DIR = Path("/usr/local/airflow/include/sql")

SOURCES = {
    "espn": "espn_news_raw",
    "google": "google_news_raw",
    "yahoo": "yahoo_news_raw",
}


@dag(
    dag_id="nfl_daily_news",
    start_date=pendulum.datetime(
        2026, 10, 24, tz="America/New_York"
    ),
    schedule=CronTriggerTimetable(
        "30 7 * * *",
        timezone="America/New_York",
    ),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
    },
    tags=["nfl", "news", "bigquery"],
)
def nfl_daily_news():


    def make_trigger(source):

        @task(task_id=f"trigger_{source}")
        def trigger():
            import re
            import requests
            from google.auth.transport.requests import Request
            from google.oauth2 import id_token

            url = "https://nfl-ingest-cedewu2pea-uc.a.run.app"
            token = id_token.fetch_id_token(Request(), url)

            response = requests.get(
                url,
                params={"job": "news", "source": source},
                headers={"Authorization": f"Bearer {token}"},
                timeout=300,
            )
            print(response.text)
            response.raise_for_status()

            match = re.search(
                rf"uploaded\s+(news/{re.escape(source)}/[^\s]+\.ndjson)",
                response.text,
            )
            if not match:
                raise RuntimeError(
                    f"No new GCS file for {source}"
                )

            return f"gs://{BUCKET}/{match.group(1)}"

        return trigger

    def make_load(source):

        @task(task_id=f"load_{source}_raw")
        def load(gcs_uri):
            from google.cloud import bigquery

            if not gcs_uri.startswith(
                f"gs://{BUCKET}/news/{source}/"
            ):
                raise ValueError("Unexpected GCS URI")

            bq = bigquery.Client(
                project=PROJECT,
                location=LOCATION,
            )

            target = f"{PROJECT}.raw.{SOURCES[source]}"
            temp = (
                f"{PROJECT}.raw."
                f"_news_load_{source}_{uuid4().hex[:12]}"
            )

            try:
                schema = bq.get_table(target).schema
                config = bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                    schema=schema,
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                )

                job = bq.load_table_from_uri(
                    gcs_uri,
                    temp,
                    job_config=config,
                    location=LOCATION,
                )
                job.result()

                loaded = bq.get_table(temp).num_rows
                if loaded == 0:
                    raise RuntimeError(f"Empty load: {source}")

                merge_sql = f"""
                MERGE `{target}` T
                USING (
                    SELECT * EXCEPT(rn)
                    FROM (
                        SELECT *,
                            ROW_NUMBER() OVER (
                                PARTITION BY link
                                ORDER BY SAFE_CAST(
                                    ingested_at AS TIMESTAMP
                                ) DESC
                            ) AS rn
                        FROM `{temp}`
                        WHERE link IS NOT NULL
                    )
                    WHERE rn = 1
                ) S
                ON T.link = S.link
                WHEN NOT MATCHED THEN INSERT ROW
                """

                bq.query(merge_sql, location=LOCATION).result()
                total = bq.get_table(target).num_rows

                print(
                    f"{source}: loaded={loaded}, "
                    f"raw_total={total}"
                )
                return {"source": source, "loaded": loaded}

            finally:
                bq.delete_table(temp, not_found_ok=True)

        return load

    @task(trigger_rule="all_done_min_one_success")
    def refresh_news_staging():
        from google.cloud import bigquery

        client = bigquery.Client(
            project=PROJECT,
            location=LOCATION,
        )

        for name in [
            "00_norm_name.sql",
            "01_stg_headlines.sql",
            "02_stg_headline_athlete_tags.sql",
        ]:
            path = SQL_DIR / "staging" / name

            print("Running:", name)

            client.query(
                path.read_text(encoding="utf-8"),
                location=LOCATION,
            ).result()

    @task
    def refresh_news_mart():
        from google.cloud import bigquery

        client = bigquery.Client(
            project=PROJECT,
            location=LOCATION,
        )

        for name in [
            "03_fact_headline.sql",
            "04_bridge_headline_player.sql",
        ]:
            path = SQL_DIR / "mart" / name

            print("Running:", name)

            client.query(
                path.read_text(encoding="utf-8"),
                location=LOCATION,
            ).result()

    @task
    def verify_news():
        from google.cloud import bigquery

        client = bigquery.Client(
            project=PROJECT,
            location=LOCATION,
        )

        for table in [
            "staging.stg_headlines",
            "mart.fact_headline",
            "mart.bridge_headline_player",
        ]:
            query = (
                f"SELECT COUNT(*) AS n "
                f"FROM `{PROJECT}.{table}`"
            )

            count = next(iter(
                client.query(
                    query, location=LOCATION
                ).result()
            )).n

            print(f"{table}: {count} rows")

            if table != "mart.bridge_headline_player" and count == 0:
                raise RuntimeError(
                    f"Empty table: {table}"
                )


    espn = make_load("espn")(make_trigger("espn")())
    google = make_load("google")(make_trigger("google")())
    yahoo = make_load("yahoo")(make_trigger("yahoo")())

    staging = refresh_news_staging()
    mart = refresh_news_mart()
    verify = verify_news()

    [espn, google, yahoo] >> staging
    staging >> mart >> verify


nfl_daily_news()
