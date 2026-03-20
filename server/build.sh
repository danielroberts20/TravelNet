#!/bin/bash

CACHE_FLAG=""
if [[ "$1" == "--no-cache" ]]; then
  CACHE_FLAG="--no-cache"
fi

sudo docker stop travelnet travelnet-dashboard
docker rm travelnet travelnet-dashboard
docker compose build $CACHE_FLAG
docker compose up -d