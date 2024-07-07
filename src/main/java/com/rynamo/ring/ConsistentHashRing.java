package com.rynamo.ring;

import com.rynamo.grpc.membership.ClusterMessage;
import com.rynamo.grpc.membership.RingEntryMessage;

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
    public ConsistentHashRing(ConsistentHashRing copy) {
        this.size = copy.size;
        this.ring = copy.ring;
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

    public RingEntry getEntry(String host, int port) {
        return this.ring.get(this.getEntryIdx(host, port));
    }

    public RingEntry getEntry(int idx) {
        return this.ring.get(idx);
    }


    public void insertNode(String host, int port) {
        int idx = this.getEntryIdx(host, port);
        RingEntry newEntry = new RingEntry(host, port);
        this.ring.set(idx, newEntry);
    }

    public void removeNode(String host, int port) {
        RingEntry target = this.getEntry(host, port);
        target.setHost("");
        target.setPort(-1);
    }

    public ClusterMessage createClusterMessage() {
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

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder("[");
        for (RingEntry entry : this.ring) {
            if (entry.getHost().isEmpty()) {
                str.append("{null, ").append(entry.getTimestamp().getEpochSecond()).append("}");
            } else {
                str.append("{").append(entry.getHost()).append(":").append(entry.getPort()).append(",").append(entry.getTimestamp().getEpochSecond()).append("}");
            }
            str.append(", ");
        }
        return str.toString();
    }
}
