# Setup snowflake
Create a snowflake account and a database.
* Choose your Snowflake edition: Standard
* Choose your cloud provider: AWS
* Click the link from email to activate. (keep the link at the bottom of the email)
* Username: admin
* Password: somthing you want
* Startup tutorial: click `Skip for now`
* Convert your public key into snowflake format: `ssh-keygen -f ~/.ssh/id_rsa.pub -e -m PKCS8 | awk 'NR>1 && !/END PUBLIC KEY/' | tr -d '\n'`
* Follow the file [Course Resources](/_course_resources/course-resources.md), replace the public key in the SQL scripts (note already replaced):
  * `ssh-keygen -e -m PKCS8 -f ~/.ssh/id_rsa.pub | grep -v "^---" | tr -d '\n'`

Go to location `course/airbnb`.
* `cp -rf ../../profiles.yml.sample ./profiles.yml`:
  * copy the profiles.yml file,
  * update the private key.
  * update account

# Install dbt and dagster
Install dbt fusion, the latest command should be available from their website:
`curl -fsSL https://public.cdn.getdbt.com/fs/install/install.sh | sh -s -- --update`.

A few useful commands:
* `dbtf system`: dbt installation configuration.
* `dbtf system update`: update dbt fusion.

Install dbt-autofix: `pip install dbt-autofix`.
It is a utility that helps during dbt core to dbt fusion migration.
Run commands like:
* `dbt-autofix deprecations`: fix deprecations in the dbt project.

## Install dbt and dagster by `uv`
In a directory where you have `pyproject.toml` (root project directory):
* first install uv
* `uv sync`: it will install .venv, the virtual python environment.

## Install dbt manually
* `python -m venv venv`: create a python virtual environment.
* `source venv/bin/activate`, or `. venv/bin/activate`: activate the venv.
* `which python`: verify the venv.
* `deactivate`: exit by venv.
* `pip install dbt-snowflake==1.11.0`: install dbt.

## Install Dagster manually
* `pip install dagster-dbt`: it is dbt integration for dagster, as dependency, it will install dagster core.
* `pip install dagster-webserver`
* `dagster-dbt project scaffold --project-name my_dbt_dagster_project --dbt-project-dir=airbnb`
* `dagster dev --port=3002`

# Useful dbt commands
* `dbt deps`: install packages defined in the packages.yml.
* `dbt seed`: copy seeds to snowflake, seeds are a CSV file in seeds folder defined in the dbt_project.yml
* `dbt build --full-refresh`: whole package of `dbt seed` -> `dbt run` -> `dbt snapshot` -> tests
* `dbt --version`: dbt command available?
* `dbt debug`: verify dbt configurations, specifically the data warehouse connection.
* `dbt clean`: Clear target folder
* `dbt ls --resource-type model`: what models can dbt see
* `dbt ls -s tag:fact`: select models with tag 'fact'
* `dbt init --skip-profile-setup airbnb`: create project.
* `dbt run`: materialize the models
* `dbt run --debug`: shows every SQL that is executed against the data warehouse, also grant SQLs.
* `dbt run --full-refresh`: to rebuild the whole model.
* `dbt run -s dim_listings_w_hosts --sample "3 days"`: materialize with a limit condition to include data of the last 3 days.
* `dbt run -s tag:fact` materialize all models with tag 'fact'
* `dbt run --help`
* `dbt run-operation --help`: execute a macro in itself, not as part of a test.
* `dbt run-operation learn_variables --vars '{user_name: wlei07}'`
* `dbt parse`: validate yaml, write manifest.json
* `dbt compile`: check if all models are connected correctly. It renders from Jinja -> SQL. For DBT Fusion, it also checks SQL syntax.
* `dbt compile --inline '{# This is a comment #}{% set my_name = "Lei" %}{{ my_name }}'`: ompile the whole project, but also this Jinja code and put result to the screen.
* `dbt compile --inline '{{ select_positive_values("dim_listings_cleansed", "minimum_nights") }}'`: another example of the above
* `dbt show --inline '{{ select_positive_values("dim_listings_cleansed", "minimum_nights") }}'`: execute the query
* `dbt show --inline 'select * from {{ ref("dim_listings_cleansed") }} where {{ no_empty_strings(ref("dim_listings_cleansed")) }}'`: another example of the above
* `dbt source freshness`: check the freshness of the sources defined in the sources.yml: `sources.tables[].config[].freshness`.
* `dbt snapshot`
* `dbt test`
* `dbt test -x`: stop test execution after the first failure so it fails earlier.
* `dbt test -s dim_listings_minimum_nights`: select a single test by name

## dbt documentation
* `dbt docs generate`: generate documentation, taking input form the *.yml files in the models folder.
* `dbt docs serve`: start an HTTP server for the generated documentation.

# Setup Preset
* go to https://10minutemail.com/ get the temporary email address
* go to the https://preset.io/ click free trial 

Connect to the database:
* select use `SQLAlchemy URI`
* Display Name: preset
* snowflake://preset@XGFOFSM-EH42249/airbnb?role=reporter&warehouse=compute_wh
* Advanced tab -> Security:
```json
{
  "auth_method": "keypair",
  "auth_params": {
    "privatekey_body": "the RSA private key, in one line separated by \n"
  }
}
```
## Create a new Preset chart
* goto the dataset tab, create new dataset
* Database: preset
* Schema: dev
* Table: mart_fullmoon_reviews -> Create and explore dataset
* Create a new chart -> # Bar -> Create a new chart
* X-axis: IS_FULL_MOON
* Metrics: fx COUNT(*)
* Dimensions: REVIEW_SENTIMENT
* Click `Create chart`
* Contribution Mode: Row -> Update chart
* Customize tab:
* Stacked Style: Stack -> Update chart
* Left upper corner: `Add the name of the chart` -> `Full Moon vs Reviews` -> Save
## Create a new Dashboard
* goto the Dashboards tab from the top
* `+ Dashboard` button from the upper right corner
* drag the chart `Full Moon vs Reviews` into the dashboard from the right panel
* make it larger by dragging the right border
* Click the `Layout elements` from the right panel
* Drag the Header and put it above the chart
* Put some text in the header, for example, "Executive Dashboard"
* Replace the `unnamed dashboard` from the top with some text, for example, "Executive Dashboard" -> Save

# About Jinja:
* `{# This is a comment #}`
* `{% set my_name = "Lei" %}`: statement: assignments, if statements, macro calls, etc.
* `{{ my_name }}`: expressions
