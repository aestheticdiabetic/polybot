#!/usr/bin/env bash
# calibrate_weekly.sh — Run weekly by cron to keep BOND_CITY_BIAS_CORRECTIONS current.
# Runs the calibration script inside the polybot Docker container so it has access
# to all dependencies. Writes corrections to the persistent override file, then
# restarts the container so the new values are loaded immediately.
#
# Cron entry (runs every Sunday at 03:00 UTC):
#   0 3 * * 0 /home/angus/polybot/bot/calibrate_weekly.sh >> /home/angus/polybot/logs/calibrate.log 2>&1

set -euo pipefail

CONTAINER="polybot"
TRADES="/app/logs/paper_trades.jsonl"
OVERRIDE_FILE="/app/data/config.override.env"

echo ""
echo "==== calibrate_weekly $(date -u '+%Y-%m-%d %H:%M:%S UTC') ===="

if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
    echo "ERROR: container '${CONTAINER}' is not running"
    exit 1
fi

docker exec "$CONTAINER" python3 /app/bot/calibrate_forecasts.py \
    --trades "$TRADES" \
    --override-file "$OVERRIDE_FILE" \
    --apply

echo "Restarting ${CONTAINER} to apply new corrections …"
docker restart "$CONTAINER"
echo "Restarted."

echo "==== done ===="
