package com.rynamo.ring;

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
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConsistentHashRing {
    private List<RingEntry> ring;
    private int size;

    public ConsistentHashRing(int size) {
        this.size = size;
        this.ring = Stream.generate(RingEntry::new)
                .limit(size)
                .collect(Collectors.toList());
    }

    public void init(String host, int port) {
        System.out.println("Initializing ring");
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 8000).usePlaintext().build();
        ExchangeMembershipBlockingStub blockingStub = ExchangeMembershipGrpc.newBlockingStub(channel);
        ClusterMessage dstEntries = blockingStub.getMembership(ClusterMessage.newBuilder().build());
        for (RingEntryMessage node : dstEntries.getNodeList()) {
            if (!node.getHost().isEmpty()) {
                this.insertNode(node.getHost(), node.getPort());
            }
        }
        this.insertNode(host, port);
    }

    public List<RingEntry> getRing() {
        return this.ring;
    }

    public int getSize() {
        return this.ring.size();
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

    synchronized public RingEntry getEntry(String host, int port) {
        return this.ring.get(this.getEntryIdx(host, port));
    }

    synchronized public RingEntry getEntry(int idx) {
        return this.ring.get(idx);
    }


    synchronized public void insertNode(String host, int port) {
        RingEntry target = this.getEntry(host, port);
        target.setHost(host);
        target.setPort(port);
        target.setActive(true);
    }

    synchronized public void removeNode(String host, int port) {
        RingEntry target = this.getEntry(host, port);
        target.setActive(false);
    }

    synchronized public ClusterMessage createClusterMessage() {
        ClusterMessage.Builder builder = ClusterMessage.newBuilder();
        List<RingEntryMessage> entries = new ArrayList<>();
        for (RingEntry entry : this.ring) {
            String host = entry.getHost();
            int port = entry.getPort();
            long timestamp = entry.getTimestamp().getEpochSecond();
            entries.add(RingEntryMessage.newBuilder().setHost(host).setPort(port).setTimestamp(timestamp).build());
        }
        builder.addAllNode(entries);
        return builder.build();
    }

    public void mergeRings(ClusterMessage dstEntries) {
        for (int i = 0; i < dstEntries.getNodeCount(); i++) {
            RingEntryMessage r = dstEntries.getNode(i);
            RingEntry myEntry = this.getEntry(i);
            if (r.getTimestamp() > myEntry.getTimestamp().getEpochSecond()) {
                if (!(r.getHost().equals(myEntry.getHost()) && r.getPort() == myEntry.getPort())) {
                    this.insertNode(r.getHost(), r.getPort());
                }
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        for (RingEntry entry : this.ring) {
            str.append(entry.toString());
        }
        return str.toString();
    }
}
