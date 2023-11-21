# %%

#pip install sqlalchemy snowflake-sqlalchemy markdown pyyaml

import streamlit as st

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


def read_sql_from_md(file_path):
    with open(file_path, 'r') as f:
        md_text = f.read()

    html = markdown(md_text, extensions=[CodeHiliteExtension()])
    st.write(html)
    soup = BeautifulSoup(html, 'html.parser')

    code_blocks = soup.find_all('#snowflake_setup')
    #sql_blocks = [block.text for block in code_blocks if block.get('class') and 'sql' in block.get('class')]
    st.write(code_blocks)
    st.write(code_blocks.text)
    return code_blocks

    return sql_blocks

def execute_sql(connection, sql_blocks):
    for sql in sql_blocks:
        if "-- END OF SNOWFLAKE DATA IMPORT" in sql:
            break
        st.write(sql)
        # result = connection.execute(sql)
        # for row in result:
        #     st.write(row)


def main():
    pw = os.environ.get("SNOWFLAKE_PASSWORD") if os.environ.get("SNOWFLAKE_PASSWORD") else ""
    hostname = st.text_input('Hostname', 'laimquw-pfb79199')
    username = st.text_input('USername', 'admin')
    password = st.text_input('Password', 'ps')
    if st.button("check"):
        st.write(f"megklikkelte {hostname} {username} {password}")

        connection = get_snowflake_connection(hostname, username, password)

        sql_blocks = read_sql_from_md(CURRENT_DIR + "/course-resources.md")
        #execute_sql(connection, sql_blocks)

if __name__ == '__main__':
    main()
