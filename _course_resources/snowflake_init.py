#pip install sqlalchemy snowflake-sqlalchemy markdown pyyaml

import sqlalchemy as sa
from sqlalchemy import create_engine
from markdown import markdown
from markdown.extensions.codehilite import CodeHiliteExtension
from bs4 import BeautifulSoup
import yaml
import argparse
import os

CURRENT_DIR=os.path.dirname(os.path.abspath(__file__))

def load_dbt_profiles(profiles_path, user, password):
    with open(profiles_path, 'r') as f:
        profiles = yaml.safe_load(f)
    # overwrite the username and password from command line arguments
    for profile in profiles.values():
        for output in profile['outputs'].values():
            output['user'] = user
            output['password'] = password
    return profiles

def get_snowflake_connection(profile):
    user = profile['user']
    password = profile['password']
    account = profile['account']
    warehouse = profile['warehouse']
    database = profile['database']
    schema = profile['schema']

    engine = create_engine(f'snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}')
    connection = engine.connect()

    return connection

def read_sql_from_md(file_path):
    with open(file_path, 'r') as f:
        md_text = f.read()

    html = markdown(md_text, extensions=[CodeHiliteExtension()])
    soup = BeautifulSoup(html, 'html.parser')

    code_blocks = soup.find_all('code')
    sql_blocks = [block.text for block in code_blocks if block.get('class') and 'sql' in block.get('class')]

    return sql_blocks

def execute_sql(connection, sql_blocks):
    for sql in sql_blocks:
        if "-- END OF SNOWFLAKE DATA IMPORT" in sql:
            break
        result = connection.execute(sql)
        for row in result:
            print(row)

def main():
    parser = argparse.ArgumentParser(description='Execute SQL from markdown file against Snowflake.')
    parser.add_argument('--username', required=True, help='Snowflake username')
    parser.add_argument('--password', required=True, help='Snowflake password')
    args = parser.parse_args()

    profiles_path = os.path.expanduser('~/.dbt_profiles.yml')
    md_file_path = os.path.join(CURRENT_DIR, 'course-resources.md')

    profiles = load_dbt_profiles(profiles_path, args.username, args.password)
    snowflake_profile = profiles['your_profile_name']['outputs']['your_target_name']

    connection = get_snowflake_connection(snowflake_profile)

    sql_blocks = read_sql_from_md(md_file_path)

    execute_sql(connection, sql_blocks)

if __name__ == '__main__':
    main()
