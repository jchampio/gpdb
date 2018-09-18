#!/usr/bin/env bash

# openssl configuration file path
cat <<EOF
OPENSSL_CONF=\$GPHOME/etc/openssl.cnf
EOF

cat <<EOF
export OPENSSL_CONF
EOF
