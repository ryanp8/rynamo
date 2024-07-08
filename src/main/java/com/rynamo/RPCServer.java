package com.rynamo;

import com.rynamo.grpc.keyval.KeyMessage;
import com.rynamo.grpc.keyval.*;
import com.rynamo.grpc.membership.ClusterMessage;
import com.rynamo.grpc.membership.ExchangeMembershipGrpc;
import com.rynamo.ring.ConsistentHashRing;
import com.rynamo.ring.Node;
import io.grpc.*;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class RPCServer implements Runnable {
    private final Server server;
    private boolean serverStatus;
    private final Node node;

    public RPCServer(String host, int port, Node node) {
        this.node = node;
        this.serverStatus = false;
        ServerBuilder<?> serverBuilder = Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create());
        this.server = serverBuilder.addService(new MembershipService()).build();
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

        }

        @Override
        public void put(KeyValMessage request, StreamObserver<ValueMessage> responseObserver) {

        }
    }
}
