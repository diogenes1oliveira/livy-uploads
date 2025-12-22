#!/usr/bin/env bash

set -euo pipefail

THIS_DIR="$(dirname "$(realpath "$0")")"

set -x
curl -v -X POST -H "Content-Type: application/json" -d @"$THIS_DIR/livy-session.json" http://localhost:8998/sessions
