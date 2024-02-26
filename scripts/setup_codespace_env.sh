#!/usr/bin/env bash
set -x

mkdir -p ~/.dbt
ln -sf $(readlink -f profiles.yml) ~/.dbt/profiles.yml
pip3 install --user -r requirements.txt

code profiles.yml
