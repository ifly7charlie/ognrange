#!/bin/bash


set -eu
set -o pipefail

exec 1> >(logger -t "ognrange" -p local1.info) 2> >(logger -t "ognrange" -p local1.err)

node --enable-source-maps dist/bin/aprs.js


