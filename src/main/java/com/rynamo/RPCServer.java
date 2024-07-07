package com.rynamo;

import com.rynamo.grpc.membership.ClusterMessage;
import com.rynamo.grpc.membership.ExchangeMembershipGrpc;
import com.rynamo.grpc.membership.RingEntryMessage;
import com.rynamo.ring.RingEntry;
import io.grpc.*;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class RPCServer implements Runnable {
    private final Server server;
    private final String host;
    private final int port;
    private boolean serverStatus;
    private Node node;

    public RPCServer(String host, int port, Node node) {
        this.host = host;
        this.port = port;
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
        this.server.awaitTermination();
        this.setServerStatus(true);
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

    public class MembershipService extends ExchangeMembershipGrpc.ExchangeMembershipImplBase {
        @Override
        public void exchange(ClusterMessage request, StreamObserver<ClusterMessage> responseObserver) {

            responseObserver.onNext(RPCServer.this.node.getRing().createClusterMessage());
            for (int i = 0; i < request.getNodeCount(); i++) {
                RingEntryMessage r = request.getNode(i);
                RingEntry myEntry = RPCServer.this.node.getRing().getEntry(i);
                if (r.getTimestamp() > myEntry.getTimestamp().getEpochSecond()) {
                    if (!(r.getHost().equals(myEntry.getHost()) && r.getPort() == myEntry.getPort())) {
                        RPCServer.this.node.getRing().insertNode(r.getHost(), r.getPort());
                    }
                }
            }
            System.out.printf("After receiving exchange: %s\n", RPCServer.this.node.getRing());
            responseObserver.onCompleted();
        }
    }
}
