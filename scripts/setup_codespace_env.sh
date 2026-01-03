#!/usr/bin/env bash
set -e 

uv sync
echo 'if [ -f .venv/bin/activate ]; then source .venv/bin/activate; fi' >> ~/.bashrc \
  && echo 'if [ -f .venv/bin/activate ]; then source .venv/bin/activate; fi' >> ~/.zshrc
