#!/usr/bin/env bash
set -x

mkdir -p ~/.dbt
ln -sf $(readlink -f profiles.yml) dbtlearn/profiles.yml
pip3 install --user -r requirements.txt

code dbtlearn/profiles.yml
