#!/usr/bin/env bash
set -euo pipefail
image="$1"
fixture=$(mktemp -d)
container="processing-smoke-$$"
cleanup() {
  code=$?
  if [ "$code" -ne 0 ]; then
    python - "$container" <<'PYCODE'
import subprocess,sys
r=subprocess.run(['docker','logs',sys.argv[1]],capture_output=True,text=True)
message=(r.stdout+r.stderr)[-6000:]
message=message.replace('%','%25').replace('\r','%0D').replace('\n','%0A')
print('::error title=Container startup logs::'+message)
PYCODE
  fi
  docker rm -f "$container" >/dev/null 2>&1 || true
  rm -rf "$fixture" || sudo rm -rf -- "$fixture"
}
trap cleanup EXIT
mkdir -p "$fixture/data" "$fixture/tennis/.processing" "$fixture/tennis/metadata" "$fixture/originals"
cp server.example.json "$fixture/server.json"
chmod -R a+rwX "$fixture"
docker run -d --name "$container" --read-only --tmpfs /tmp --cap-drop ALL --security-opt no-new-privileges:true -p 127.0.0.1::8000 -v "$fixture/server.json:/config/server.json:ro" -v "$fixture/data:/data" -v "$fixture/originals:/originals:ro" -v "$fixture/tennis:/tennis" "$image"
for i in {1..30}; do
  if docker exec "$container" python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/healthz')"; then break; fi
  sleep 1
done
docker exec "$container" python -c "import urllib.request,json; assert json.load(urllib.request.urlopen('http://127.0.0.1:8000/api/v1/readyz'))['database']"
docker restart "$container"
for i in {1..30}; do
  if docker exec "$container" python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/healthz')"; then break; fi
  sleep 1
done
docker exec "$container" python -c "import sqlite3; assert sqlite3.connect('/data/processing.sqlite3').execute('pragma integrity_check').fetchone()[0]=='ok'"
