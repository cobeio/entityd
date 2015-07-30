#!/bin/bash

edd template conda/config/entityd.supervisord.conf > conda/config/entityd.conf
config-files build \
    conda/config/entityd.conf etc/supervisord.conf.d/entityd.conf
