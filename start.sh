#!/usr/bin/env bash
# ─── NSE Stock Analyzer — macOS / Linux launcher ───
set -e
cd "$(dirname "$0")"

echo
echo " ====================================================="
echo "  NSE Stock Analyzer"
echo " ====================================================="
echo

if ! command -v python3 >/dev/null 2>&1; then
  echo " [ERROR] python3 is not installed."
  echo " macOS: brew install python"
  echo " Linux: sudo apt install python3 python3-pip"
  exit 1
fi

echo " Checking dependencies..."
python3 -m pip install -q -r requirements.txt

# Open browser (Mac uses 'open', Linux uses 'xdg-open')
( sleep 3 && ( open http://localhost:5001 2>/dev/null || xdg-open http://localhost:5001 2>/dev/null ) ) &

echo
echo " Starting server on http://localhost:5001"
echo " Press Ctrl+C to stop."
echo

python3 run.py
