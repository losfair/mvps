#!/bin/bash

set -e

cargo run -p nbdstress --release -- \
  --export-name eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpbWFnZV9pZCI6IlNUUkVTUy0wMS5tdnBzLWltYWdlIiwiaW1hZ2Vfc2l6ZSI6MjE0NzQ4MzY0OCwicGFnZV9zaXplX2JpdHMiOjEyLCJleHAiOjE5MDExNTQ5NDEsImNsaWVudF9pZCI6ImM5ZWZkOTZjLWRhNTktNDM1MS1iMzExLTM1MTI5NWZhOThlMiJ9.G5n8F9_rbpQA0doZZY-vjguyM8WYxu1YcLsoht24MgQ \
  --remote 127.0.0.1:10809 \
  --write-percentage 0.2 --reconnect-interval-ms 10000
