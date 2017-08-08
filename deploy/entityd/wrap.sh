#!/bin/sh

set -e

mkdir -p /opt/entityd/etc/entityd/keys
cp /opt/entityd/keys/entityd.key-secret /opt/entityd/etc/entityd/keys/entityd.key_secret
cp /opt/entityd/keys/modeld.key /opt/entityd/etc/entityd/keys/modeld.key
exec /opt/entityd/bin/entityd "$@"
