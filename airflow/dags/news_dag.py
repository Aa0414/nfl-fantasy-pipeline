from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import pendulum
from airflow.sdk import dag, task, CronTriggerTimetable

PROJECT = "ba882-team03-kalshi"
BUCKET = "ba882-nfl-kalshi-raw"
LOCATION = "us-central1"
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

    @task
    def trigger_news():
        """Trigger the existing news ingestion Cloud Function."""
        import requests
        from google.auth.transport.requests import Request
        from google.oauth2 import id_token

        url = "https://nfl-ingest-cedewu2pea-uc.a.run.app"

        token = id_token.fetch_id_token(Request(), url)

        response = requests.get(
            url,
            params={"job": "news"},
            headers={"Authorization": f"Bearer {token}"},
            timeout=300,
        )
        response.raise_for_status()

        print("News extraction completed")
        print(response.text[:1000])

    @task
    def load_news_raw():
        from google.cloud import storage, bigquery

        gcs = storage.Client(project=PROJECT)
        bq = bigquery.Client(
            project=PROJECT,
            location=LOCATION,
        )

        results = {}

        for source, table_name in SOURCES.items():
            prefix = f"news/{source}/"

            blobs = [
                blob for blob in gcs.list_blobs(
                    BUCKET, prefix=prefix
                )
                if blob.name.endswith(".ndjson")
                and blob.size is not None
                and blob.size > 0
            ]

            if not blobs:
                raise RuntimeError(
                    f"No GCS files for {source}"
                )

            uris = [
                f"gs://{BUCKET}/{blob.name}"
                for blob in sorted(
                    blobs, key=lambda b: b.name
                )
            ]

            latest_file = max(b.name for b in blobs)

            print(
                f"{source}: {len(uris)} GCS files"
            )
            print(f"Latest: {latest_file}")

            target = f"{PROJECT}.raw.{table_name}"
            temp = (
                f"{PROJECT}.raw."
                f"_news_load_{source}_{uuid4().hex[:12]}"
            )

            try:
                target_schema = bq.get_table(target).schema

                config = bigquery.LoadJobConfig(
                    source_format=(
                        bigquery.SourceFormat
                        .NEWLINE_DELIMITED_JSON
                    ),
                    schema=target_schema,
                    write_disposition=(
                        bigquery.WriteDisposition
                        .WRITE_TRUNCATE
                    ),
                )

                job = bq.load_table_from_uri(
                    uris,
                    temp,
                    job_config=config,
                    location=LOCATION,
                )
                job.result()

                loaded = bq.get_table(temp).num_rows

                if loaded == 0:
                    raise RuntimeError(
                        f"Empty News load: {source}"
                    )

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
                WHEN NOT MATCHED THEN
                  INSERT ROW
                """

                bq.query(
                    merge_sql,
                    location=LOCATION,
                ).result()

                final_count = bq.get_table(
                    target
                ).num_rows

                print(
                    f"{source}: loaded={loaded}, "
                    f"raw_total={final_count}"
                )

                results[source] = {
                    "files": len(uris),
                    "loaded": loaded,
                    "raw_total": final_count,
                }

            finally:
                bq.delete_table(
                    temp, not_found_ok=True
                )

        return results

    @task
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

    extraction = trigger_news()
    raw = load_news_raw()
    staging = refresh_news_staging()
    mart = refresh_news_mart()
    verify = verify_news()

    extraction >> raw >> staging >> mart >> verify


nfl_daily_news()
