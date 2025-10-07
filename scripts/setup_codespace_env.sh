#!/usr/bin/env bash
set -x

ln -sf $(readlink -f profiles.yml) dbtlearn/profiles.yml
pip3 install --user -r requirements.txt
