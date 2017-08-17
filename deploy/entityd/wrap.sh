#!/bin/sh

set -e

mkdir -p /opt/entityd/etc/entityd/keys
cp /opt/entityd/keys/entityd.key-secret /opt/entityd/etc/entityd/keys/entityd.key_secret
cp /opt/entityd/keys/modeld.key /opt/entityd/etc/entityd/keys/modeld.key

do_curl() {
    curl -sS --output /dev/null "$@" || :
}

BEACON_SCOPE="$BEACON_SCOPE"
if [ -z "$BEACON_SCOPE" ]; then
    BEACON_SCOPE=$(uuid -v1)
fi
BEACON_ID=$(uuid -v4)
BEACON="A:$BEACON_SCOPE:$BEACON_ID"
BEACON_URL="https://beacon.cobe.io/$BEACON"
echo "BEACON: $BEACON_URL"
entityd "$@" &
AGENT_PID=$!
trap "kill $AGENT_PID" INT TERM EXIT
while [ -e "/proc/$AGENT_PID" ]; do
    do_curl -X PUT "$BEACON_URL"
    sleep 1s
done
do_curl -X DELETE "$BEACON_URL"

