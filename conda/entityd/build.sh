#!/bin/bash

$PYTHON setup.py install
mkdir -p $PREFIX/etc/supervisord.conf.d
sed -e "s,%PREFIX%,$PREFIX,g" \
    <$RECIPE_DIR/entityd.supervisord.conf \
    >$PREFIX/etc/supervisord.conf.d/entityd.conf
