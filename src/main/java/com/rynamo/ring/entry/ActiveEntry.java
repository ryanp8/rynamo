package com.rynamo.ring.entry;

import com.google.protobuf.ByteString;
import com.rynamo.grpc.membership.ClusterMessage;
import com.rynamo.grpc.membership.ExchangeMembershipGrpc;
import com.rynamo.grpc.membership.RingEntryMessage;
import com.rynamo.grpc.membership.VersionMessage;
import com.rynamo.grpc.storage.*;
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

    public ActiveEntry(String id, int version) {
        String[] parts = id.split(":");
        this.host = parts[0];
        this.port = Integer.parseInt(parts[1]);
        this.id = id;
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

    public String getId() {
        return this.id;
    }

    public void kill() {
        this.chan.shutdownNow();
    }

    public ConsistentHashRing exchange(ClusterMessage src) {
        ClusterMessage recv = this.exchangeStub.exchange(src);
        return ConsistentHashRing.clusterMessageToRing(recv);
    }

    public PutResponse coordinatePut(String key, byte[] value) {
        PutRequest request = PutRequest.newBuilder()
                .setKey(key).setValue(ByteString.copyFrom(value)).build();
        return this.storageStub.coordinatePut(request);
    }

    public GetResponse coordinateGet(String key) {
        GetRequest request = GetRequest.newBuilder().setKey(key).build();
        return this.storageStub.coordinateGet(request);
    }

    public PutResponse put(String key, long version, byte[] value) {
        PutRequest request = PutRequest.newBuilder()
                .setKey(key)
                .setValue(ByteString.copyFrom(value))
                .setVersion(version)
                .build();
        return this.storageStub.put(request);
    }

    public GetResponse get(String key) {
        GetRequest request = GetRequest.newBuilder().setKey(key).build();
        return this.storageStub.get(request);
    }

    public long getRemoteEntryVersion(ActiveEntry entry) {
        RingEntryMessage request = RingEntryMessage.newBuilder()
                .setHost(entry.getHost())
                .setPort(entry.getPort())
                .setVersion(entry.getVersion())
                .setActive(true)
                .build();
        VersionMessage response = this.exchangeStub.getVersion(request);
        return response.getVersion();
    }

    @Override
    public String toString() {
        return String.format("(%s:%d, %d)", this.getHost(), this.getPort(), this.getVersion());
    }
}
