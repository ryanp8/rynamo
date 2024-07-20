#!/bin/sh

CLUSTER_SIZE=3

for ((i = 0; i < $CLUSTER_SIZE; i++))
    do
        if [ $i == 0 ]; then
            docker stop rynamo-seed
        else
            NAME=rynamo-$i
            docker stop $NAME
        fi

done