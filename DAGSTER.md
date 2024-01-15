# Orchestrating dbt with Dagster

## Set up your environment
Let's create a virtualenv and install dbt and dagster. These packages are located in [requirements.txt](requirements.txt).
```
virutalenv venv -p python3.11
pip install -r requirements.txt
```

## Create a dagster project
Dagster has a command for creating a dagster project from an existing dbt project: 
```
dagster-dbt project scaffold --project-name dbt_dagster_project --dbt-project-dir=dbtlearn
```

_At this point in the course, open [schedules.py](dbt_dagster_project/dbt_dagster_project/schedules.py) and uncomment the schedule logic._

# Start dagster
Now that our project is created, start the Dagster server:

```
cd dbt_dagster_project
DAGSTER_DBT_PARSE_PROJECT_ON_LOAD=1 dagster dev
```

We will continue our work on the dagster UI at [http://localhost:3000/](http://localhost:3000) 
