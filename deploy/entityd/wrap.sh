#!/bin/sh

set -e

mkdir -p /usr/etc/entityd/keys
cp /opt/entityd/keys/entityd.key-secret /usr/etc/entityd/keys/entityd.key_secret
cp /opt/entityd/keys/modeld.key /usr/etc/entityd/keys/modeld.key
exec /usr/local/bin/entityd "$@"
