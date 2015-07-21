#!/bin/bash

for path in conda/config/* ; do
    config-files build $path etc/recipes/entityd-config/$(basename $path)
done
