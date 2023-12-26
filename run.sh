#!/bin/bash

set -e

export RUST_LOG=info
export MVPS_TE_JWT_SECRET="insecure_token"
export TMPDIR="/tmp/mvpstest"
export MVPS_TE_ROOT_KEY="xchacha20poly1305:qpy7OFKzgJZ2f1xm733LvWHPTl+6V0yNY8VFNjOOFas="

if [ "$1" == "s3" ]; then
  export AWS_ACCESS_KEY_ID=minioadmin
  export AWS_SECRET_ACCESS_KEY=minioadmin
  export AWS_REGION=us-east-1
  export S3_ENDPOINT=http://localhost:8355

  cargo run -p mvps-te --release -- \
    --image-store s3 --image-store-s3-bucket mvps-image-store --image-store-s3-prefix test/ \
    --wal-path ./testdata/s3_wal_store
  exit 0
fi

cargo run -p mvps-te --release -- \
  --image-store local --image-store-local-path ./testdata/image_store \
  --wal-path ./testdata/wal_store

# test jwts:
#
# eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpbWFnZV9pZCI6IlRFU1RJTUctMDEubXZwcy1pbWFnZSIsImltYWdlX3NpemUiOjEwNzM3NDE4MjQsInBhZ2Vfc2l6ZV9iaXRzIjoxNCwiZXhwIjoxOTAxMTU0OTQxfQ.gOSfLBtgmP-oRQBl-a46pqmJPNKdyYUondjDMxSacN0
# {
#   "image_id": "TESTIMG-01.mvps-image",
#   "image_size": 1073741824,
#   "page_size_bits": 14,
#   "exp": 1901154941
# }
#
# eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpbWFnZV9pZCI6IlRFU1RJTUctMDIubXZwcy1pbWFnZSIsImltYWdlX3NpemUiOjEwNzM3NDE4MjQsInBhZ2Vfc2l6ZV9iaXRzIjoxMiwiZXhwIjoxOTAxMTU0OTQxfQ.NwRJ05WJbmAAXw5jVrLO28qtzB3eVqzPAEekWLwvzV0
# {
#   "image_id": "TESTIMG-02.mvps-image",
#   "image_size": 1073741824,
#   "page_size_bits": 12,
#   "exp": 1901154941
# }
#
# eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpbWFnZV9pZCI6IlRFU1RJTUctMDIubXZwcy1pbWFnZSIsImltYWdlX3NpemUiOjEwNzM3NDE4MjQsInBhZ2Vfc2l6ZV9iaXRzIjoxMiwiZXhwIjoxOTAxMTU0OTQxLCJjbGllbnRfaWQiOiJjOWVmZDk2Yy1kYTU5LTQzNTEtYjMxMS0zNTEyOTVmYTk4ZTIifQ.j-nR5Opn-pK6whrm8pJIgyItO5XvlYf0oy7RzR00T1Y
# {
#   "image_id": "TESTIMG-02.mvps-image",
#   "image_size": 1073741824,
#   "page_size_bits": 12,
#   "exp": 1901154941,
#   "client_id": "c9efd96c-da59-4351-b311-351295fa98e2"
# }
#
# eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpbWFnZV9pZCI6IlNUUkVTUy0wMS5tdnBzLWltYWdlIiwiaW1hZ2Vfc2l6ZSI6MjE0NzQ4MzY0OCwicGFnZV9zaXplX2JpdHMiOjEyLCJleHAiOjE5MDExNTQ5NDEsImNsaWVudF9pZCI6ImM5ZWZkOTZjLWRhNTktNDM1MS1iMzExLTM1MTI5NWZhOThlMiJ9.G5n8F9_rbpQA0doZZY-vjguyM8WYxu1YcLsoht24MgQ
# {
#   "image_id": "STRESS-01.mvps-image",
#   "image_size": 2147483648,
#   "page_size_bits": 12,
#   "exp": 1901154941,
#   "client_id": "c9efd96c-da59-4351-b311-351295fa98e2"
# }
