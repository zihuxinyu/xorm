#!/bin/bash
if [ -z $1 ];then
infile=./pg_reserved.txt
fi

if [ -f $infile ];then
	cat $infile | awk '{
		if ($2 == "reserved" || $3 == "reserved" || $4 == "reserved" || $5 == "reserved") {
			printf("\""$1"\":true,\n")
		}
	}' 
else
	echo "argument $1 if not a file!"
fi
