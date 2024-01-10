# %%

#pip install sqlalchemy snowflake-sqlalchemy markdown pyyaml

import streamlit as st
import re
import sqlalchemy as sa
from sqlalchemy import create_engine
from markdown import markdown
from markdown.extensions.codehilite import CodeHiliteExtension
from bs4 import BeautifulSoup
import yaml
import argparse
import os
from sqlalchemy.dialects import registry

registry.register('snowflake', 'snowflake.sqlalchemy', 'dialect')


CURRENT_DIR=os.path.dirname(os.path.abspath(__file__))

# %%
def load_dbt_profiles(profiles_path, user, password):
    with open(profiles_path, 'r') as f:
        profiles = yaml.safe_load(f)
    # overwrite the username and password from command line arguments
    for profile in profiles.values():
        for output in profile['outputs'].values():
            output['user'] = user
            output['password'] = password
    return profiles

def get_snowflake_connection(account, username, password):

    engine = create_engine(f'snowflake://{username}:{password}@{account}/AIRBNB/DEV?warehouse=COMPUTE_WH&role=ACCOUNTADMIN&account_identifier={account}')
    connection = engine.connect()

    return connection


def execute_sqls(connection, sql_blocks):
    for sql in sql_blocks:
        st.write(sql)


def main():
    pw = os.environ.get("SNOWFLAKE_PASSWORD") if os.environ.get("SNOWFLAKE_PASSWORD") else ""
    hostname = st.text_input('Hostname', 'laimquw-pfb79199')
    username = st.text_input('USername', 'admin')
    password = st.text_input('Password', pw)
    if st.button("check"):
        st.write(f"megklikkelte {hostname} {username} {password}")

        #connection = get_snowflake_connection(hostname, username, password)

        with open(CURRENT_DIR + "/course-resources.md", 'r') as file:
            md = file.read().rstrip()
        import_pattern = r'sql {#snowflake_import}(.*?)```'; 
        match = re.search(import_pattern, md, re.DOTALL);
        import_sqls = [row.strip() for row in match.group(1).split(';')]
        st.write(import_sqls)
        #execute_sql(connection, sql_blocks)

if __name__ == '__main__':
    main()
