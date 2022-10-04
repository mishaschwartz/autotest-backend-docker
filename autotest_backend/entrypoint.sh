#!/usr/bin/env sh

set -e

cp -TRf /tmp/files /workspace
find /tmp/scripts -mindepth 1 -maxdepth 1 -exec cp -Rf {} /workspace/ \;

exec /tester "$@"
