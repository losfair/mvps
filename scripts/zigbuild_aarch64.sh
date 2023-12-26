#!/bin/bash

set -e

cargo zigbuild -p mvps-te --release --target aarch64-unknown-linux-gnu.2.19
cargo zigbuild -p mvps-s3-gc --release --target aarch64-unknown-linux-gnu.2.19
cargo zigbuild -p nbdstress --release --target aarch64-unknown-linux-gnu.2.19
