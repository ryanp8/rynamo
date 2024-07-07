package com.rynamo.ring;

import com.rynamo.Node;
import com.rynamo.grpc.membership.ClusterMessage;
import com.rynamo.grpc.membership.ExchangeMembershipGrpc;
import com.rynamo.grpc.membership.ExchangeMembershipGrpc.*;
import com.rynamo.grpc.membership.RingEntryMessage;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.net.URL;
import java.security.*;
import java.util.*;
import java.nio.ByteBuffer;

public class ConsistentHashRing {
    private List<RingEntry> ring;
    private int size;

    public ConsistentHashRing(int size) {
        this.size = size;
        this.ring = Arrays.asList(new RingEntry[size]);
    }
    public ConsistentHashRing(ConsistentHashRing copy) {
        this.size = copy.size;
        this.ring = copy.ring;
    }

    public List<RingEntry> getRing() {
        return this.ring;
    }

    public int getEntryIdx(String host, int port) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            return (int) (ByteBuffer.wrap(md.digest((host+port).getBytes())).getLong() & 0xffff) % this.size;
        } catch (NoSuchAlgorithmException e) {
            System.err.println("No such algorithm");
            return -1;
        }
    }

    public RingEntry getEntry(String host, int port) {
        return this.ring.get(this.getEntryIdx(host, port));
    }


    public void addNode(String host, int port) {
        int idx = this.getEntryIdx(host, port);
        RingEntry newEntry = new RingEntry(host, port);
        this.ring.set(idx, newEntry);
    }

    public void removeNode(String host, int port) {
        RingEntry targetRing = this.getEntry(host, port);
        if (targetRing != null) {
            int targetRingIdx = this.getEntryIdx(host, port);
            this.ring.set(targetRingIdx, null);
        }
    }

    public ClusterMessage createClusterMessage() {
        ClusterMessage.Builder builder = ClusterMessage.newBuilder();
        List<RingEntryMessage> entries = new ArrayList<>();
        for (RingEntry entry : this.ring) {
            if (entry != null) {
                String host = entry.getHost();
                int port = entry.getPort();
                long timestamp = entry.getUpdateTime().getEpochSecond();
                entries.add(RingEntryMessage.newBuilder().setHost(host).setPort(port).setTimestamp(timestamp).build());
            }
        }
        builder.addAllNode(entries);
        return builder.build();
    }

    @Override
    public String toString() {
        return this.ring.toString();
    }
}
