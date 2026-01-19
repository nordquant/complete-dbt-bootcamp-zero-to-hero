# Advanced Dagster Implementation

This is the advanced Dagster implementation as presented by Georg and Alexandar.

## Features

- **Daily Partitioned Assets**: The `fct_reviews` model is configured with daily partitions starting from 2022-01-24
- **Custom DagsterDbtTranslator**: Adds partition metadata to dbt resources
- **Partition-aware dbt execution**: Passes `start_date` and `end_date` as dbt vars for incremental processing
- **Daily Schedule**: Automatically materializes dbt models at midnight

## Usage

Set the environment variable `DAGSTER_DBT_PARSE_PROJECT_ON_LOAD` to parse the dbt project at runtime, or ensure a pre-built `manifest.json` exists in the dbt project's `target/` directory.
