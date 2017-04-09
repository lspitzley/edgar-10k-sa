#!/bin/bash

ntotal=$(wc -l < year2014-2016.10k.csv)
while true; do 
	ncurr=$(ls txt | wc -l)
	echo "Download text progress: $ncurr/$ntotal"
	sleep 300
done
