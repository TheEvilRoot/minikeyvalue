#!/bin/bash
#trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT
kill $(pgrep -f nginx)

PORT=3001 ./volume /data/volume1/ &
PORT=3002 ./volume /data/volume2/ &
PORT=3003 ./volume /data/volume3/ &
PORT=3004 ./volume /data/volume4/ &
PORT=3005 ./volume /data/volume5/ &

HOSTNAME=$(hostname)
./mkv -port 3000 -volumes ${HOSTNAME}:3001,${HOSTNAME}:3002,${HOSTNAME}:3003,${HOSTNAME}:3004,${HOSTNAME}:3005 -db /data/indexdb/ server

