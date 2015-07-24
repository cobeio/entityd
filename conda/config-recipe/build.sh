#!/bin/bash

dest=$PREFIX/share/recipes/entityd-config/
mkdir -p $dest

for path in conda/config/* ; do
    cp -f "$path" "$dest/$(basename $path)"
done
