#!/bin/bash

CACHE_FLAG=""
if [[ "$1" == "--no-cache" ]]; then
  CACHE_FLAG="--no-cache"
fi

bash "$(dirname "$0")/build-dashboard.sh"
bash "$(dirname "$0")/build-server.sh" $CACHE_FLAG
