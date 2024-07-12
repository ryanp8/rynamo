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
    public final int N;
    public final int R;
    public final int W;
    private ConsistentHashRing ring;
    private final String host;
    private final int rpcPort;
    private final int clientPort;
    private final DBClient db;
    private final ClientServer clientServer;

    public Node(String host, int N, int R, int W, int rpcPort, int clientPort) throws org.rocksdb.RocksDBException {
        this.host = host;
        this.N = N;
        this.R = R;
        this.W = W;
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
                System.out.println(Node.this.getRing());
            }
        };
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(exchangeTimerTask, 1000, 1000);
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

        // If the first node in the preference list is inactive, then the idx will be
        // incremented in the while loop, so we need to subtract one from the final idx
        // if the while loop was entered
        if (iters == 0) {
            this.exchangeRings(idx, dst);
        } else {
            this.exchangeRings(idx - 1, dst);
        }
    }


    private void exchangeRings(int idx, RingEntry dst) {
        ClusterMessage cm = this.ring.createClusterMessage();
        try {
            ClusterMessage dstEntries = dst.getExchangeBlockingStub().exchange(cm);
            this.ring.mergeRings(dstEntries);
        } catch (StatusRuntimeException e) {
            System.out.println(idx);
            this.ring.getEntry(idx).setActive(false);
        }
    }

    public byte[] getDB(String key) throws RocksDBException {
        return this.db.get(key);
    }

    public void putDB(String key, byte[] val) throws RocksDBException {
        this.db.put(key, val);
    }

    public CoordinateResponse coordinateGet(String key) {
        List<RingEntry> preferenceList = this.getPreferenceList(key);
        int reads = 0;
        byte[] oneResponse = {};
        for (var entry : preferenceList) {
            if (entry.getActive()) {
                KeyValGrpc.KeyValBlockingStub stub = entry.getKeyValBlockingStub();
                ValueMessage response = stub.get(KeyMessage.newBuilder().setKey(key).build());
                if (response.getSuccess()) {
                    oneResponse = response.getValue().toByteArray();
                    if (reads++ == this.R) {
                        break;
                    }
                }
            }
        }
        return new CoordinateResponse(R, 0, oneResponse);
    }

    public CoordinateResponse coordinatePut(String key, byte[] val) {
        List<RingEntry> preferenceList = this.getPreferenceList(key);
        int writes = 0;
        for (var entry : preferenceList) {
            if (entry.getActive()) {
                KeyValGrpc.KeyValBlockingStub stub = entry.getKeyValBlockingStub();
                ValueMessage response = stub.put(KeyValMessage.newBuilder().setKey(key).setValue(ByteString.copyFrom(val)).build());
                if (response.getSuccess()) {
                    if (writes++ == this.W) {
                        break;
                    }
                }
            }
        }
        return new CoordinateResponse(0, writes, null);
    }
}
