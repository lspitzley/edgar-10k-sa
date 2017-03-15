#!/bin/bash

nform=$(ls form | wc -l)
ntext=$(ls txt | wc -l)
while true; do 
	echo "Text file progress: $ntext/$nform"
	ntext=$(ls txt | wc -l)
	sleep 300
done
