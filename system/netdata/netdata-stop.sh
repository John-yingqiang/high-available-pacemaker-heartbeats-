#!/bin/bash 
pid=`pgrep -f sbin/netdata`
if [ -z "$pid" ];
then
	echo "Netdata is not running."
else
	echo "Stop netdata..."
	pgrep -f '/netdata/' | grep -v $$ | xargs kill -9
	exit 0
fi
