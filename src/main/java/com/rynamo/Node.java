package com.rynamo;

import com.rynamo.grpc.membership.ClusterMessage;
import com.rynamo.grpc.membership.ExchangeMembershipGrpc;
import com.rynamo.grpc.membership.RingEntryMessage;
import com.rynamo.ring.ConsistentHashRing;
import com.rynamo.ring.RingEntry;
import io.grpc.*;

import java.util.Random;
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

        this.ring = new ConsistentHashRing(4);

        // Add self to ring
        this.ring.insertNode(this.host, this.port);

        // Seed node
        this.ring.insertNode("localhost", 3000);
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
        timer.scheduleAtFixedRate(exchangeTimerTask, 3000, 3000);
    }

    public ConsistentHashRing getRing() {
        return this.ring;
    }

    private void exchangeRings() {
        RingEntry srcRingEntry = this.ring.getEntry(this.host, this.port);

        Random rand = new Random();
        int idx = (int) (rand.nextLong() & 0xffff) % 4;
        RingEntry entry = this.ring.getEntry(idx);
        int iters = 0;
        while (entry.getHost().isEmpty() && iters < 4) {
            entry = this.ring.getEntry((idx++) % 4);
            iters++;
        }
        if (iters >= 4) {
            return;
        }

        if (srcRingEntry.getBlockingStub() == null) {
            ManagedChannel channel = ManagedChannelBuilder.forAddress(entry.getHost(), entry.getPort()).usePlaintext().build();
            srcRingEntry.setBlockingStub(ExchangeMembershipGrpc.newBlockingStub(channel));
            srcRingEntry.setAsyncStub(ExchangeMembershipGrpc.newStub(channel));
        }

        ClusterMessage cm = this.ring.createClusterMessage();

//        System.out.printf("Sending to %d %s\n", srcRingEntry.getPort(), cm);
        ClusterMessage other = srcRingEntry.getBlockingStub().exchange(cm);

        for (int i = 0; i < other.getNodeCount(); i++) {
            RingEntryMessage r = other.getNode(i);
            RingEntry myEntry = this.ring.getEntry(i);
            if (r.getTimestamp() > myEntry.getTimestamp().getEpochSecond()) {
                if (!(r.getHost().equals(myEntry.getHost()) && r.getPort() == myEntry.getPort())) {
                    this.ring.insertNode(r.getHost(), r.getPort());
                }
            }
        }
        System.out.println(this.ring);
    }
}
