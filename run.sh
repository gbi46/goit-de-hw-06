#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Usage: ./run.sh {produce_sensors}"
    exit 1
fi

python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

case "$1" in
    produce_sensors)
        exec python3 "$1.py"
        ;;
    *)
        echo "Unknown command: $1"
        echo "Usage: ./run.sh {produce_sensors}"
        exit 1
        ;;
esac
