#!/bin/bash

for path in conda/config/* ; do
    config-files build "$path" "share/recipes/entityd-config/$(basename $path)"
done
