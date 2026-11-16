#!/usr/bin/env bash
# Start the three independent costpipe processes in the background.
# Logs and databases go under ./data. Stop with: kill %1 %2 %3 (or pkill -f costpipe).
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p data

python3 -m costpipe.seed --db data/costpipe.db "$@"
python3 -m costpipe.ingest  --db data/costpipe.db > data/ingest.log 2>&1 &
python3 -m costpipe.worker  --db data/costpipe.db > data/worker.log 2>&1 &
python3 -m costpipe.subscriber --store data/subscriber.db > data/subscriber.log 2>&1 &

echo "ingest      http://127.0.0.1:8791/   (log: data/ingest.log)"
echo "worker      log: data/worker.log"
echo "subscriber  log: data/subscriber.log"
echo "stop: pkill -f 'costpipe.(ingest|worker|subscriber)'"
wait
