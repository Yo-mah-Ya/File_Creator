#!/usr/bin/env bash

SOURCE_DIR=$(cd $(dirname ${BASH_SOURCE:-0}) && pwd)
cd ${SOURCE_DIR}/..


# run with coverage
poetry run pytest -v --cov=tests --cov-branch
# run withou coverage
poetry run pytest -vv
