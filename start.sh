#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
export PYTHONUTF8=1
export PYTHONIOENCODING=utf-8

if [ -x "$SCRIPT_DIR/.venv/bin/python" ]; then
    PYTHON_BIN="$SCRIPT_DIR/.venv/bin/python"
elif [ -x "$SCRIPT_DIR/env/bin/python" ]; then
    PYTHON_BIN="$SCRIPT_DIR/env/bin/python"
elif [ -x "$SCRIPT_DIR/../.venv/bin/python" ]; then
    PYTHON_BIN="$SCRIPT_DIR/../.venv/bin/python"
else
    PYTHON_BIN="python3"
fi

exec "$PYTHON_BIN" "$SCRIPT_DIR/main.py" "$@"
