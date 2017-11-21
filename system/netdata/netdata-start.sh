#!/bin/bash

pid=`pgrep -f sbin/netdata`
if [ -z "$pid" ];
then
    echo "Netdata is not started yet. Start netdata..."
    /opt/netdata/usr/sbin/netdata &
    sleep 1
else
    echo "Netdata is already started."
fi
