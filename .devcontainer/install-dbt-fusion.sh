#!/bin/bash
set -e
export SHELL=/bin/bash
curl -fsSL https://public.cdn.getdbt.com/fs/install/install.sh | sh -s -- --update
