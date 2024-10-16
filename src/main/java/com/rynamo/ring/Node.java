package com.rynamo.ring;

import com.rynamo.ring.coordinate.CoordinateResponse;
import com.rynamo.ring.coordinate.Coordinator;
import com.rynamo.db.StorageLayer;
import com.rynamo.grpc.membership.ClusterMessage;
import com.rynamo.ring.entry.*;
import io.grpc.*;

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
    private final HttpServer httpServer;
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
        this.httpServer = new HttpServer(this);
        this.ring = new ConsistentHashRing(10, seedNode);
        this.coordinator = new Coordinator(this);
    }

    /*
    * Starts the components of the node
    * */
    public void start() throws InterruptedException {
        this.startRPCServer(); // This blocks because the RPC server needs to be running before membership can be sent
        this.startMembershipGossip();
        this.httpServer.start(this.clientPort);
        this.ring.init(host, this.rpcPort);
    }

    /*
    * Starts the RPC server in another thread, so the main thread is not blocked
    * while listening for requests
    * */
    public void startRPCServer() throws InterruptedException {
        Thread serverThread = new Thread(this.server);
        serverThread.start();
        // Wait until the server has started before we initialize the ring
        while (!this.server.getServerStatus()) {
            TimeUnit.SECONDS.sleep(1);
        }
    }

    /*
    * Creates a background timer that exchanges this node's cluster membership list
    * with another random node.
    * */
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

    /*
    * Tries to exchange membership data with a random node in the ring. Does
    * nothing if the random entry is inactive.
    * */
    private void exchangeRings() {
        Optional<ActiveEntry> other = this.ring.getRandomEntry();
        other.ifPresent(this::exchangeRings);
    }

    /*
    * Exchanges membership data with a node that is known to be active.
    * */
    private void exchangeRings(ActiveEntry dst) {
        // Build the message containing ring membership data, so it can be sent as an RPC
        ClusterMessage cm = this.ring.getClusterMessage();
        try {
            // Send my ring to the other node
            ConsistentHashRing recv = dst.exchange(cm);
            this.ring.merge(recv);

            // Clean up channels from the incoming ring
            // TODO: Might not be the best idea to store the incoming as a full ConsistentHashRing
            // object since it creates all these unnecessary channels
            recv.killRing();
        } catch (StatusRuntimeException e) {
            System.err.printf("Tried to exchange with %s but dst was unavailable\n", dst);
            this.ring.kill(this.ring.getNodeIndex(dst), dst.getVersion() + 1);
        }
    }

    // Wrappers to the coordinator methods
    public CoordinateResponse coordinateGet(String key) {
        return this.coordinator.coordinateGet(key);
    }


    public CoordinateResponse coordinatePut(String key, long version, byte[] val) {
        return this.coordinator.coordinatePut(key, version, val);
    }
}
