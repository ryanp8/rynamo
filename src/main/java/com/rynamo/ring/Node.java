package com.rynamo.ring;

import com.google.protobuf.ByteString;
import com.rynamo.RPCServer;
import com.rynamo.coordinate.CoordinateResponse;
import com.rynamo.db.DBClient;
import com.rynamo.grpc.keyval.*;
import com.rynamo.grpc.membership.ClusterMessage;
import com.rynamo.grpc.membership.ExchangeMembershipGrpc;
import io.grpc.*;
import org.rocksdb.RocksDBException;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class Node {
    private final RPCServer server;
    private final int N;
    private ConsistentHashRing ring;
    private final String host;
    private final int rpcPort;
    private final int clientPort;
    private final DBClient db;
    private final ClientServer clientServer;

    public Node(String host, int N, int rpcPort, int clientPort) throws org.rocksdb.RocksDBException {
        this.host = host;
        this.N = N;
        this.rpcPort = rpcPort;
        this.clientPort = clientPort;
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

    synchronized public List<RingEntry> getPreferenceList(String key) {
        int start = this.ring.getIdx(key);
        List<RingEntry> preferenceList = new ArrayList<>();
        int iters = 0;
        while (iters < this.ring.getSize()) {
            preferenceList.add(this.ring.getEntry((start + iters) % this.ring.getSize()));
            iters++;
        }
        return preferenceList;
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
        ClusterMessage cm = this.ring.createClusterMessage();
        ClusterMessage dstEntries = dst.getExchangeBlockingStub().exchange(cm);
        this.ring.mergeRings(dstEntries);
    }

    public byte[] getDB(String key) throws RocksDBException {
        return this.db.get(key);
    }

    public void putDB(String key, byte[] val) throws RocksDBException {
        this.db.put(key, val);
    }

    public CoordinateResponse coordinateGet(String key) {
        List<RingEntry> preferenceList = this.getPreferenceList(key);
        int R = 0;
        byte[] oneResponse = {};
        for (var entry : preferenceList) {
            if (entry.getActive()) {
                KeyValGrpc.KeyValBlockingStub stub = entry.getKeyValBlockingStub();
                ValueMessage response = stub.get(KeyMessage.newBuilder().setKey(key).build());
                System.out.println("coordinateGet: " + response);
                if (response.getSuccess()) {
                    oneResponse = response.getValue().toByteArray();
                    R++;
                    break;
                }
            }
        }
        return new CoordinateResponse(R, 0, oneResponse);
    }

    public CoordinateResponse coordinatePut(String key, byte[] val) {
        List<RingEntry> preferenceList = this.getPreferenceList(key);
        int W = 0;
        for (var entry : preferenceList) {
            if (entry.getActive()) {
                KeyValGrpc.KeyValBlockingStub stub = entry.getKeyValBlockingStub();
                ValueMessage response = stub.put(KeyValMessage.newBuilder().setKey(key).setValue(ByteString.copyFrom(val)).build());
                if (response.getSuccess()) {
                    W++;
                }
            }
        }
        return new CoordinateResponse(0, W, null);
    }
}
