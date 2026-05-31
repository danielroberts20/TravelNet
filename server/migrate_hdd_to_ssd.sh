SRC=/mnt/linux/docker/services/travelnet/data
DST=/mnt/ssd/docker/services/travelnet/data

cp $SRC/travel.db $SRC/travel.db-shm $SRC/travel.db-wal $DST/
cp $SRC/config_overrides.json $SRC/cron_runs.json $DST/
cp $SRC/flow_results.json $SRC/flow_results.lock $DST/
cp $SRC/journal_latest.json $SRC/retroactive_location_scan.marker $SRC/app_start_time $DST/
mkdir -p $DST/compute
cp -r $SRC/docs $DST/
cp -r $SRC/jobs $DST/

# Trevor Chroma
cp /mnt/linux/docker/services/Trevor/chroma/chroma.sqlite3 /mnt/ssd/docker/services/Trevor/chroma/