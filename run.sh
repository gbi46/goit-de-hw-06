#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Usage: ./run.sh {produce_sensors|average_sensors}"
    exit 1
fi

[ -d .venv ] || python3 -m venv .venv
source .venv/bin/activate
if ! python3 - <<'PY'
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

try:
    for requirement in Path("requirements.txt").read_text().splitlines():
        name, expected = requirement.split("==")
        if version(name) != expected:
            raise SystemExit(1)
except PackageNotFoundError:
    raise SystemExit(1)
PY
then
    python3 -m pip install -r requirements.txt || exit 1
fi

case "$1" in
    produce_sensors|average_sensors)
        exec python3 "$1.py"
        ;;
    *)
        echo "Unknown command: $1"
        echo "Usage: ./run.sh {produce_sensors|average_sensors}"
        exit 1
        ;;
esac
