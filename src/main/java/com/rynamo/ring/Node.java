package com.rynamo.ring;

import com.rynamo.db.Row;
import com.rynamo.ring.coordinate.CoordinateResponse;
import com.rynamo.ring.coordinate.Coordinator;
import com.rynamo.db.StorageLayer;
import com.rynamo.grpc.membership.ClusterMessage;
import com.rynamo.ring.entry.*;
import io.grpc.*;
import org.rocksdb.RocksDBException;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class Node {
    private final RPCServer server;
    public final int N;
    public final int R;
    public final int W;
    private final ConsistentHashRing ring;
    private final String host;
    private final int rpcPort;
    private final String id;
    private final int clientPort;
    private final StorageLayer db;
    private final ClientServer clientServer;
    private final Coordinator coordinator;

    public Node(int N, int R, int W, String host, int rpcPort, int clientPort, String seedNode) throws org.rocksdb.RocksDBException {
        this.N = N;
        this.R = R;
        this.W = W;
        this.host = host;
        this.rpcPort = rpcPort;
        this.id = String.format("%s:%d", host, rpcPort);
        this.clientPort = clientPort;
        this.db = new StorageLayer(this.id);
        this.server = new RPCServer(this.rpcPort, this);
        this.clientServer = new ClientServer(this);
        this.ring = new ConsistentHashRing(10, seedNode);
        this.coordinator = new Coordinator(this);
    }

    public void start() throws InterruptedException {
        this.startRPCServer();
        this.startMembershipGossip();
        this.clientServer.start(this.clientPort);
        this.ring.init(host, this.rpcPort);
    }

    public void startRPCServer() throws InterruptedException {
        Thread serverThread = new Thread(this.server);
        serverThread.start();
        // Wait until the server has started before we initialize the ring
        while (!this.server.getServerStatus()) {
            TimeUnit.SECONDS.sleep(1);
        }
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
        timer.scheduleAtFixedRate(exchangeTimerTask, 0, 3000);
    }

    public StorageLayer db() {
        return this.db;
    }

    public String getId() {
        return this.id;
    }

    public ConsistentHashRing getRing() {
        return this.ring;
    }

    public List<RingEntry> getPreferenceList(String key) {
        return this.ring.getPreferenceList(key);
    }

    private void exchangeRings() {
        Optional<ActiveEntry> other = this.ring.getRandomEntry();
        other.ifPresent(this::exchangeRings);
    }

    private void exchangeRings(ActiveEntry dst) {
        ClusterMessage cm = this.ring.getClusterMessage();
        try {
            ConsistentHashRing recv = dst.exchange(cm);
            this.ring.merge(recv);
            recv.killRing();
        } catch (StatusRuntimeException e) {
            System.err.printf("Tried to exchange with %s but dst was unavailable\n", dst);
            this.ring.kill(this.ring.getNodeIndex(dst), dst.getVersion() + 1);
        }
    }

    public CoordinateResponse coordinateGet(String key) {
        return this.coordinator.coordinateGet(key);
    }


    public CoordinateResponse coordinatePut(String key, long version, byte[] val) {
        return this.coordinator.coordinatePut(key, version, val);
    }
}
