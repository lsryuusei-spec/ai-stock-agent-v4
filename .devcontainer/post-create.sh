#!/usr/bin/env bash
set -euo pipefail

python -m pip install --upgrade pip
python -m pip install -e .

mkdir -p data
python -m ai_stock_agent.cli --db data/agent.db bootstrap-demo
