package com.rynamo.ring.membership;

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


/*
* Active entry is the gateway for nodes to communicate with each other
* Each active entry has rpc stubs and a channel that can be used to
* communicate with the node represented by the entry.
*
* Ex:
* ActiveEntry A(...);
* A.get("key1")
*
* The above will send a get RPC to the node represented by A.
* */
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

    // Extra constructor to allow for creation given host:port as a single
    // string instead of as separate arguments
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

    /*
    * Wrapper around the exchange stub that returns the exchange rpc result as ConsistentHashRing
    * */
    public ConsistentHashRing exchange(ClusterMessage src) {
        ClusterMessage recv = this.exchangeStub.exchange(src);
        return ConsistentHashRing.clusterMessageToRing(recv);
    }

    /*
    * Wrapper around the coordinatePut stub that handles marshalling of key and value
    * */
    public PutResponse coordinatePut(String key, byte[] value) {
        PutRequest request = PutRequest.newBuilder()
                .setKey(key).setValue(ByteString.copyFrom(value)).build();
        return this.storageStub.coordinatePut(request);
    }

    /*
    * Wrapper around coordinateGet stub that marshals the target key
    * */
    public GetResponse coordinateGet(String key) {
        GetRequest request = GetRequest.newBuilder().setKey(key).build();
        return this.storageStub.coordinateGet(request);
    }

    /*
    * Wrapper around the put stub that marshals the key, version, and value
    * */
    public PutResponse put(String key, long version, byte[] value) {
        PutRequest request = PutRequest.newBuilder()
                .setKey(key)
                .setValue(ByteString.copyFrom(value))
                .setVersion(version)
                .setIntendedNode(this.id)
                .build();
        return this.storageStub.put(request);
    }

    // Used for hinted handoff. Send a request to a node that may not be the correct one
    // when the desired one is unavailable
    public PutResponse put(String key, long version, byte[] value, String dst) {
        PutRequest request = PutRequest.newBuilder()
                .setKey(key)
                .setValue(ByteString.copyFrom(value))
                .setVersion(version)
                .setIntendedNode(dst)
                .build();
        return this.storageStub.put(request);
    }

    /*
    * Wrapper around the get stub that marshals the target key
    * */
    public GetResponse get(String key) {
        GetRequest request = GetRequest.newBuilder().setKey(key).build();
        return this.storageStub.get(request);
    }

    /*
    * Wrapper around the getVersion stub that marshals the target entry and
    * returns the version of the target entry in the node represented by this active entry
    * */
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
