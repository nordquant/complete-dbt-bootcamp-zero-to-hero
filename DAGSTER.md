# Installation
```
dagster-dbt project scaffold --project-name dbt_dagster_project --dbt-project-dir=dbtlearn
```

# Start dev server
```
cd dbt_dagster_project
DAGSTER_DBT_PARSE_PROJECT_ON_LOAD=1 dagster dev
```
