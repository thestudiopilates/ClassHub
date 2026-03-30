#!/usr/bin/env bash
set -euo pipefail

MODE="${OPS_AUTOMATION_MODE:-intraday}"
DAY="${OPS_TARGET_DAY:-today}"

exec python -m app.ops_runner --mode "${MODE}" --day "${DAY}"
