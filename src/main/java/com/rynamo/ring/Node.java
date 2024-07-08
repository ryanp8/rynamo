package com.rynamo.ring;

import com.rynamo.RPCServer;
import com.rynamo.db.DBClient;
import com.rynamo.grpc.membership.ClusterMessage;
import com.rynamo.grpc.membership.ExchangeMembershipGrpc;
import io.grpc.*;

import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

public class Node {
    private final RPCServer server;
    private ConsistentHashRing ring;
    private final String host;
    private final int port;
    private final DBClient db;

    public Node(String host, int port) throws org.rocksdb.RocksDBException {
        this.host = host;
        this.port = port;
        this.db = new DBClient(port);
        this.server = new RPCServer(this.host, this.port, this);
        this.ring = new ConsistentHashRing(4);
    }

    public void startRPCServer() {
        Thread serverThread = new Thread(this.server);
        serverThread.start();
        // Wait until the server has started before we initialize the ring
        while (!this.server.getServerStatus()) {

        }
        this.ring.init(this.host, this.port);
    }

    public void startMembershipGossip() {
        TimerTask exchangeTimerTask = new TimerTask() {
            @Override
            public void run() {
                Node.this.exchangeRings();
                System.out.println(Node.this.ring);
            }
        };
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(exchangeTimerTask, 3000, 3000);
    }

    public ConsistentHashRing getRing() {
        return this.ring;
    }

    private void exchangeRings() {
        int ringSize = this.ring.getSize();
        Random rand = new Random();
        int idx = (int) (rand.nextLong() & Integer.MAX_VALUE) % ringSize;
        RingEntry dst = this.ring.getEntry(idx);

        int iters = 0;
        while (!dst.getActive() && iters < ringSize) {
            dst = this.ring.getEntry((idx++) % ringSize);
            iters++;
        }
        if (iters >= ringSize || dst.getPort() == this.port) {
            return;
        }
        this.exchangeRings(dst);
    }

    private void exchangeRings(RingEntry dst) {

        if (dst.getBlockingStub() == null) {
            ManagedChannel channel = ManagedChannelBuilder.forAddress(dst.getHost(), dst.getPort()).usePlaintext().build();
            dst.setBlockingStub(ExchangeMembershipGrpc.newBlockingStub(channel));
            dst.setAsyncStub(ExchangeMembershipGrpc.newStub(channel));
        }

        ClusterMessage cm = this.ring.createClusterMessage();
        ClusterMessage dstEntries = dst.getBlockingStub().exchange(cm);
        this.ring.mergeRings(dstEntries);
    }
}
