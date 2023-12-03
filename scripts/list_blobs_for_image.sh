#!/bin/bash

set -e

jq --raw-output .layers[] | sed 's#^#testdata/image_store/blobs/#' | xargs ls -lh
