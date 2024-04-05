#!/bin/sh
set -e

python3 /progress_timestamps.py /raw_data.csv /data.csv

# Append additional args from docker compose / kubernetes
ARGS="--loadhist=/data.csv $@"
exec circuitbreaker $ARGS
