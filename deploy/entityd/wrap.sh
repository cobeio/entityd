#!/bin/sh

set -e

mkdir -p /venvs/entityd/etc/entityd/keys
cp /opt/entityd/keys/entityd.key-secret /venvs/entityd/etc/entityd/keys/entityd.key_secret
cp /opt/entityd/keys/modeld.key /venvs/entityd/etc/entityd/keys/modeld.key
exec /venvs/entityd/bin/entityd "$@"
