#!/bin/sh

# docker build -t rynamo .

CLUSTER_SIZE=3

for ((i = 0; i < $CLUSTER_SIZE; i++))
    do
        let "CLIENT_PORT=3000+$i"
        let "GRPC_PORT=8000+$i"
        NAME=rynamo-$GRPC_PORT
        if [ $i == 0 ]; then
            NAME=rynamo-seed
        fi
        docker run -d --network rynamo-network -p $GRPC_PORT -p $CLIENT_PORT --name $NAME --rm rynamo mvn exec:java -Dexec.args="$NAME $GRPC_PORT $CLIENT_PORT"
done