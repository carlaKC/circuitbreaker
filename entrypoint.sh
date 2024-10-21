#!/bin/sh
set -e

# Append additional args from docker compose / kubernetes
ARGS="--loadhist=/data.csv $@"
exec circuitbreaker $ARGS
