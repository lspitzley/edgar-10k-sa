#!/bin/bash

nform=$(ls txt | wc -l)
ntext=$(ls mda | wc -l)
while true; do 
	ntext=$(ls mda | wc -l)
	echo "Text file progress: $ntext/$nform"
	sleep 5
done
