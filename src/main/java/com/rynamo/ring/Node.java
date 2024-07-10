package com.rynamo.ring;

import com.rynamo.RPCServer;
import com.rynamo.coordinate.CoordinateResponse;
import com.rynamo.db.DBClient;
import com.rynamo.grpc.membership.ClusterMessage;
import com.rynamo.grpc.membership.ExchangeMembershipGrpc;
import io.grpc.*;
import org.rocksdb.RocksDBException;

import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

public class Node {
    private final RPCServer server;
    private ConsistentHashRing ring;
    private final String host;
    private final int rpcPort;
    private final DBClient db;
    private final ClientServer clientServer;

    public Node(String host, int rpcPort, int clientPort) throws org.rocksdb.RocksDBException {
        this.host = host;
        this.rpcPort = rpcPort;
        this.db = new DBClient(rpcPort);
        this.server = new RPCServer(this.host, this.rpcPort, this);
        this.clientServer = new ClientServer(clientPort, this);
        this.ring = new ConsistentHashRing(4);
    }

    public void startRPCServer() throws InterruptedException {
        Thread serverThread = new Thread(this.server);
        serverThread.start();
        // Wait until the server has started before we initialize the ring
        while (!this.server.getServerStatus()) {
            TimeUnit.SECONDS.sleep(1);
        }
        this.ring.init(this.host, this.rpcPort);
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
        if (iters >= ringSize || dst.getPort() == this.rpcPort) {
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

    public byte[] getDB(String key) throws RocksDBException {
        return this.db.get(key);
    }

    public void putDB(String key, byte[] val) throws RocksDBException {
        this.db.put(key, val);
    }

    public CoordinateResponse coordinateGet(String key) {
        return null;
    }

    public CoordinateResponse coordinatePut(String key, byte[] val) {
        return null;
    }
}
