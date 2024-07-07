package com.rynamo;

import com.rynamo.grpc.membership.ClusterMessage;
import com.rynamo.grpc.membership.ExchangeMembershipGrpc;
import com.rynamo.ring.ConsistentHashRing;
import com.rynamo.ring.RingEntry;
import io.grpc.*;

import java.util.Timer;
import java.util.TimerTask;

public class Node {
    private final RPCServer server;
    private ConsistentHashRing ring;
    private final String host;
    private final int port;

    public Node(String host, int port) {
        this.host = host;
        this.port = port;

        this.server = new RPCServer(this.host, this.port, this);

        this.ring = new ConsistentHashRing(10);

        // Add self to ring
        this.ring.addNode(this.host, this.port);

        // Seed node
        this.ring.addNode("localhost", 3000);
    }

    public void startRPCServer() {
        Thread serverThread = new Thread(this.server);
        serverThread.start();
        System.out.println("Started server in separate thread");
    }

    public void startMembershipGossip() {
        TimerTask exchangeTimerTask = new TimerTask() {
            @Override
            public void run() {
                Node.this.exchangeRings();
            }
        };
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(exchangeTimerTask, 1000, 1000);
    }

    public ConsistentHashRing getRing() {
        return this.ring;
    }

    private void exchangeRings() {
        RingEntry srcRingEntry = this.ring.getEntry(this.host, this.port);
        if (srcRingEntry.getBlockingStub() == null) {
            ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 3000).usePlaintext().build();
            srcRingEntry.setBlockingStub(ExchangeMembershipGrpc.newBlockingStub(channel));
            srcRingEntry.setAsyncStub(ExchangeMembershipGrpc.newStub(channel));
        }

        ClusterMessage cm = this.ring.createClusterMessage();
        ClusterMessage other = srcRingEntry.getBlockingStub().exchange(cm);
        System.out.printf("Received %s\n", other);
    }
}
