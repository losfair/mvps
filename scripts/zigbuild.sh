#!/bin/bash

set -e

cargo zigbuild -p mvps-te --release --target x86_64-unknown-linux-gnu.2.19
cargo zigbuild -p mvps-s3-gc --release --target x86_64-unknown-linux-gnu.2.19
