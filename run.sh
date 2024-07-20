#!/bin/sh

docker build -t rynamo .

CLUSTER_SIZE=3

for ((i = 0; i < $CLUSTER_SIZE; i++))
    do
        NAME=rynamo-$i
        if [ $i == 0 ]; then
            NAME=rynamo-seed
        fi
        docker run -d --network rynamo-network --name $NAME --rm rynamo mvn exec:java -Dexec.args="$NAME 8000 3000"
done