# rynamo

## Distributed key value store
A replicated and highly available key value store built to learn more about distributed systems and to gain experience in Java and gRPC. Inspired by the [Dynamo paper](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf). 

## API
- `GET /{key}`
- `PUT /{key}/{value}`

## Design
- Currently, `localhost:8000` is used as a seed node. Its client server runs on `localhost:3000` All new nodes know it exists, so they are able to communicate with it to exchange membership data.
- Every 3 seconds, a node will contact another node that it is aware of and try to exchange/merge membership histories based on the timestamp of the last update.
- Each node has a gRPC server and a http server ([Javalin](https://javalin.io/))
    - gRPC is used to communicate within the cluster
        - Forward GET/PUT requests to the appropriate coordinator
        - Send get and put operations to replicas once the coordinator has started
    - http is used to communicate with the cluster from an outside client
        - The client can contact any node, which will use gRPC to forward the operation to the appropriate one.


Improved upon [initial version](https://github.com/ryanp8/distributed-kv-store)
