#!/bin/sh

docker build -t rynamo-client -f ClientDockerfile .

docker run --rm --network=rynamo-network -it rynamo-client