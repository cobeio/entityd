#!/bin/sh

set -e

do_curl() {
    curl -sS --output /dev/null "$@" || :
}

uuid() {
    python -c "import uuid; print(uuid.uuid$1())"
}

BEACON_SCOPE="$COBE_BEACON_SCOPE"
if [ -z "$BEACON_SCOPE" ]; then
    BEACON_SCOPE=$(uuid 1)
fi
BEACON_ID=$(uuid 4)
BEACON="A:$BEACON_SCOPE:$BEACON_ID"
BEACON_URL="https://beacon.cobe.io/$BEACON"
echo "BEACON: $BEACON_URL"
entityd "$@" &
AGENT_PID=$!
trap "kill $AGENT_PID" INT TERM EXIT
while [ -e "/proc/$AGENT_PID" ]; do
    do_curl -X PUT "$BEACON_URL?expires-after=12"
    sleep 5s
done
do_curl -X DELETE "$BEACON_URL"
