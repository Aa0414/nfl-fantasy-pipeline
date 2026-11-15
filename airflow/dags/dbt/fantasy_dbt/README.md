# fantasy_dbt

dbt project for the NFL fantasy pipeline. Replaces the Phase 1 SQL files in
`airflow/include/sql/` (staging 00-05, mart 01-05) with dbt models.

## Layers

| Layer | Models | Prod dataset | Materialized |
|---|---|---|---|
| staging | stg_headlines, stg_headline_athlete_tags, stg_player_week, stg_players, stg_schedule | `staging` | view |
| intermediate | int_player_sleeper_match, int_headline_player_matches | `staging` | view |
| marts | dim_player, dim_game, fact_headline, bridge_headline_player, fact_player_week | `mart` | table |

Sources: the 6 tables in `raw`. Prod table names are the same as Phase 1,
so existing analysis queries keep working.

Changes vs Phase 1:
- `staging.norm_name` UDF is now the `norm_name()` macro
- dim_player links Sleeper players by **gsis_id** first, name+team only as fallback
  (fixes duplicate player rows)

## Run it

```bash
dbt deps                          # installs dbt_utils (first time / after packages.yml changes)
dbt build                         # dev: everything goes to your own dataset (dbt_<name>)
dbt build --target prod           # prod: writes to staging + mart
bash validation/compare_to_phase1.sh dbt_allen   # dbt vs Phase 1 tables
dbt docs generate && dbt docs serve --port 8080  # lineage graph
```

## Airflow handoff (for whoever owns the DAGs)

1. **Install dbt** in the Astro image: add `dbt-bigquery` (separate virtualenv recommended,
   so it doesn't conflict with Airflow's packages).
2. **Run `dbt deps`** before builds (Cosmos does this automatically if `install_deps=True`;
   with a BashOperator run `dbt deps && dbt build --target prod`).
3. **Credentials:** target `prod`, project `ba882-team03-kalshi`, location `us-central1`,
   using the same GCP service account the DAGs already use.
4. **Replace** the SQL-file tasks in `news_dag.py` / `stats_dag.py` (00-05 staging, 01-05 mart)
   with one dbt task group that runs after ingestion.
5. dbt tests run as part of `dbt build`; a failing test stops downstream models.
