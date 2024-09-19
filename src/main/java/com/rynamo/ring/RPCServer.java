package com.rynamo.ring;


import com.google.protobuf.ByteString;
import com.rynamo.db.Row;
import com.rynamo.grpc.storage.*;
import com.rynamo.grpc.membership.*;
import com.rynamo.ring.coordinate.CoordinateResponse;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class RPCServer implements Runnable {
    private final Server server;
    private boolean serverStatus;
    private final Node node;

    public RPCServer(int port, Node node) {
        this.node = node;
        this.serverStatus = false;
        ServerBuilder<?> serverBuilder = Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create());
        this.server = serverBuilder.addService(new MembershipService()).addService(new StorageService()).build();
    }

    @Override
    public void run() {
        try {
            this.start();
        } catch (IOException | InterruptedException e) {
            System.err.println(e.getMessage());
        }
    }

    public void start() throws java.io.IOException, InterruptedException {
        this.server.start();
        this.setServerStatus(true);
        System.out.println("Started RPC server in separate thread");

        this.server.awaitTermination();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Use stderr here since the logger may have been reset by its JVM shutdown hook.
            System.err.println("*** shutting down gRPC server since JVM is shutting down");
            try {
                this.stop();
            } catch (InterruptedException e) {
                e.printStackTrace(System.err);
            }
            System.err.println("*** server shut down");
        }));
    }

    public void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
            this.setServerStatus(false);
        }
    }

    synchronized public boolean getServerStatus() {
        return this.serverStatus;
    }

    synchronized private void setServerStatus(boolean started) {
        this.serverStatus = started;
    }

    private class MembershipService extends ExchangeMembershipGrpc.ExchangeMembershipImplBase {
        @Override
        public void exchange(ClusterMessage request, StreamObserver<ClusterMessage> responseObserver) {
            ConsistentHashRing receiverRing = RPCServer.this.node.getRing();
            responseObserver.onNext(receiverRing.getClusterMessage());
            receiverRing.merge(ConsistentHashRing.clusterMessageToRing(request));
            responseObserver.onCompleted();
        }

        @Override
        public void getMembership(ClusterMessage request, StreamObserver<ClusterMessage> responseObserver) {
            responseObserver.onNext(RPCServer.this.node.getRing().getClusterMessage());
            responseObserver.onCompleted();
        }

        @Override
        public void getVersion(RingEntryMessage request, StreamObserver<VersionMessage> responseObserver) {
            String id = String.format("%s:%d", request.getHost(), request.getPort());
            VersionMessage response = VersionMessage.newBuilder()
                    .setVersion(RPCServer.this.node.getRing().getNode(id).getVersion())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    private class StorageService extends StorageGrpc.StorageImplBase {
        @Override
        public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
            GetResponse.Builder responseBuilder = GetResponse.newBuilder();
            try {
                Row results = RPCServer.this.node.db().get(request.getKey());

                for (byte[] result : results.values()) {
                    responseBuilder.addValue(ByteString.copyFrom(result));
                }
            } catch (RocksDBException ignored) {
            }
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        }

        @Override
        public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
            PutResponse.Builder responseBuilder = PutResponse.newBuilder();
            try {
                long version = RPCServer.this.node.db().put(request.getKey(),
                        request.getVersion(),
                        request.getValue().toByteArray());
                responseBuilder.setVersion(version);
            } catch (RocksDBException ignored){}
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        }

        @Override
        public void coordinateGet(GetRequest request, StreamObserver<GetResponse> responseObserver) {
            int R = RPCServer.this.node.R;
            System.out.println("running coordinate get rpc");
            CoordinateResponse result = RPCServer.this.node.coordinateGet(request.getKey());
            GetResponse.Builder responseBuilder = GetResponse.newBuilder();
            System.out.println(result.R());
            if (result.R() >= R) {
                for (byte[] value : result.values()) {
                    responseBuilder.addValue(ByteString.copyFrom(value));
                }
            }
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        }

        @Override
        public void coordinatePut(PutRequest request, StreamObserver<PutResponse> responseObserver) {
            int W = RPCServer.this.node.W;
            long currentVersion = RPCServer.this.node.db().getVersion(request.getKey());
            System.out.println(currentVersion);
            CoordinateResponse result = RPCServer.this.node.coordinatePut(request.getKey(),
                    currentVersion,
                    request.getValue().toByteArray());
            PutResponse.Builder responseBuilder = PutResponse.newBuilder();
            if (result.W() >= W) {
                responseBuilder.setVersion(currentVersion + 1);
            }
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        }
    }
}
