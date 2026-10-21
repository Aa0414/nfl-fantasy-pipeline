# NFL Fantasy News Pipeline (BA882)

## Project Overview

This project is developed as part of BA882: Deploying Analytics Pipelines at Boston University Questrom School of Business.

Our goal is to build an automated cloud-based analytics pipeline that integrates NFL news, player information, and weekly performance statistics into a centralized data warehouse.

The pipeline is designed to support exploratory analysis of the relationship between pregame news coverage and subsequent fantasy football performance, while establishing a foundation for future machine learning and NLP applications.

## Business Problem

NFL fantasy football performance is influenced by multiple factors, including player injuries, roster changes, team developments, and recent performance trends.

However, relevant information is distributed across different news providers and statistical databases. This fragmentation makes it difficult to systematically analyze whether news activity is associated with future player performance.

Our project addresses this challenge by creating an integrated and continuously updated analytics infrastructure.

The primary research question is:

**To what extent is pregame NFL news coverage associated with subsequent fantasy football performance, and can news-derived features improve predictive models?**

## Data Sources

Our pipeline integrates several data sources:

| Source | Data Collected | Current Collection Schedule |
|---|---|---|
| ESPN | NFL headlines, publication times, player and team tags | Daily |
| Google News | Team-related NFL headlines | Daily |
| Yahoo Sports | NFL news headlines | Daily |
| Sleeper API | Player profiles, teams, positions, and injury status | Tuesday and Saturday |
| nflverse | Weekly player statistics and NFL schedules | Weekly, planned through Airflow |

Raw data is stored in Google Cloud Storage using newline-delimited JSON (NDJSON).

Google BigQuery is the target analytics warehouse, with separate raw, staging, and mart layers planned for downstream analysis.

## Technology Stack

- **Cloud Platform:** Google Cloud Platform
- **Data Ingestion:** Python and Google Cloud Functions
- **Raw Storage:** Google Cloud Storage
- **Orchestration:** Apache Airflow / Astronomer (planned integration)
- **Data Warehouse:** Google BigQuery
- **Analytics & Visualization:** Looker Studio (planned)
- **Version Control:** GitHub
- **Project Management:** GitHub Projects

## Project Status

Phase 1 focuses on deploying the ingestion infrastructure, developing warehouse models, orchestrating scheduled updates, and establishing initial reporting capabilities.

Future phases will extend this foundation toward machine learning predictions and NLP-based analysis of NFL news.
