package com.rynamo;


import com.google.protobuf.ByteString;
import com.rynamo.coordinate.CoordinateResponse;
import com.rynamo.grpc.keyval.*;
import com.rynamo.grpc.membership.*;
import com.rynamo.ring.ConsistentHashRing;
import com.rynamo.ring.Node;
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
        this.server = serverBuilder.addService(new MembershipService()).addService(new KeyValService()).build();
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
        System.out.println("Started server in separate thread");

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
            responseObserver.onNext(receiverRing.createClusterMessage());
            receiverRing.mergeRings(request);
            responseObserver.onCompleted();
        }

        @Override
        public void getMembership(ClusterMessage request, StreamObserver<ClusterMessage> responseObserver) {
            responseObserver.onNext(RPCServer.this.node.getRing().createClusterMessage());
            responseObserver.onCompleted();
        }
    }

    private class KeyValService extends KeyValGrpc.KeyValImplBase {
        @Override
        public void get(KeyMessage request, StreamObserver<ValueMessage> responseObserver) {
            ValueMessage.Builder builder = ValueMessage.newBuilder();
            try {
                byte[] dbResponse = RPCServer.this.node.getDB(request.getKey());
                boolean isEmpty = dbResponse == null;
                ByteString responseBytes = isEmpty ? ByteString.EMPTY : ByteString.copyFrom(dbResponse);
                responseObserver.onNext(builder.setSuccess(!isEmpty).setValue(responseBytes).build());
            } catch (RocksDBException e) {
                responseObserver.onNext(builder.setSuccess(false).build());
            }
            responseObserver.onCompleted();
        }

        @Override
        public void put(KeyValMessage request, StreamObserver<ValueMessage> responseObserver) {
            ValueMessage.Builder builder = ValueMessage.newBuilder();
            try {
                RPCServer.this.node.putDB(request.getKey(), request.getValue().toByteArray());
                responseObserver.onNext(builder.setSuccess(true).build());
            } catch (RocksDBException e) {
                responseObserver.onNext(builder.setSuccess(false).build());
            }
            responseObserver.onCompleted();
        }

        @Override
        public void forwardCoordinateGet(KeyMessage request, StreamObserver<ValueMessage> responseObserver) {
            System.out.println("running forwardCoordinateGet");
            CoordinateResponse response = RPCServer.this.node.coordinateGet(request.getKey());
            boolean success = response.R >= RPCServer.this.node.R;
            System.out.println(success + " W: " + response.W + ", R: " + response.R);
            responseObserver.onNext(ValueMessage.newBuilder().setSuccess(success).setValue(ByteString.copyFrom(response.result)).build());
            responseObserver.onCompleted();
        }

        @Override
        public void forwardCoordinatePut(KeyValMessage request, StreamObserver<ValueMessage> responseObserver) {
            CoordinateResponse response = RPCServer.this.node.coordinatePut(request.getKey(), request.getValue().toByteArray());
            boolean success = response.W >= RPCServer.this.node.W;
            System.out.println(success + " W: " + response.W + ", R: " + response.R);
            responseObserver.onNext(ValueMessage.newBuilder().setSuccess(success).build());
            responseObserver.onCompleted();
        }
    }
}
