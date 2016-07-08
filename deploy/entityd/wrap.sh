#!/bin/sh

cp /usr/etc/entityd/keys/entityd.key-secret /usr/etc/entityd/keys/entityd.key_secret
exec /usr/local/bin/entityd "$@"
