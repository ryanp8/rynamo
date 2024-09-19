package com.rynamo.ring.entry;

import com.rynamo.grpc.membership.ClusterMessage;
import com.rynamo.grpc.membership.ExchangeMembershipGrpc;
import com.rynamo.grpc.membership.RingEntryMessage;
import com.rynamo.grpc.storage.KeyMessage;
import com.rynamo.grpc.storage.KeyValMessage;
import com.rynamo.grpc.storage.StorageGrpc;
import com.rynamo.grpc.storage.ValueMessage;
import com.rynamo.ring.ConsistentHashRing;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.List;

public class ActiveEntry extends RingEntry {
    private String host;
    private int port;
    private String id;
    private ExchangeMembershipGrpc.ExchangeMembershipBlockingStub exchangeStub;
    private StorageGrpc.StorageBlockingStub storageStub;
    private ManagedChannel chan;

    public ActiveEntry(String host, int port, long version) {
        this.host = host;
        this.port = port;
        this.id = String.format("%s:%d", host, port);
        this.version = version;
        this.chan = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        this.exchangeStub = ExchangeMembershipGrpc.newBlockingStub(this.chan);
        this.storageStub = StorageGrpc.newBlockingStub(this.chan);
    }

    public ActiveEntry(RingEntryMessage msg) {
        this(msg.getHost(), msg.getPort(), msg.getVersion());
    }

    public String getHost() {
        return this.host;
    }

    public int getPort() {
        return this.port;
    }

    public void kill() {
        this.chan.shutdownNow();
    }

    public List<RingEntry> exchange(ClusterMessage src) {
        ClusterMessage recv = this.exchangeStub.exchange(src);
        return ConsistentHashRing.clusterMessageToRing(recv);
    }

    public ValueMessage coordinatePut(KeyValMessage request) {
        return this.storageStub.coordinatePut(request);
    }

    public ValueMessage coordinateGet(KeyMessage request) {
        return this.storageStub.coordinateGet(request);
    }

    public ValueMessage put(KeyValMessage request) {
        return this.storageStub.put(request);
    }

    public ValueMessage get(KeyMessage request) {
        return this.storageStub.get(request);
    }

    @Override
    public String toString() {
        return String.format("(%s:%d, %d)", this.getHost(), this.getPort(), this.getVersion());
    }
}
