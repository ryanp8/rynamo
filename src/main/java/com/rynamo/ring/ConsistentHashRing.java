package com.rynamo.ring;

import com.rynamo.grpc.membership.ClusterMessage;
import com.rynamo.grpc.membership.RingEntryMessage;
import com.rynamo.ring.entry.ActiveEntry;
import com.rynamo.ring.entry.InactiveEntry;
import com.rynamo.ring.entry.RingEntry;

import java.security.*;
import java.util.*;
import java.nio.ByteBuffer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConsistentHashRing {
    private List<RingEntry> ring;
    private int size;
    private String seedNodeId;

    public ConsistentHashRing(int size, String seedNodeId) {
        this.size = size;
        this.ring = Stream.generate(InactiveEntry::new)
                .limit(size)
                .collect(Collectors.toList());
        this.seedNodeId = seedNodeId;
    }

    public ConsistentHashRing(List<RingEntry> ring) {
        this.size = ring.size();
        this.ring = ring;
    }

    public void init(String host, int rpcPort) {
        ActiveEntry self = new ActiveEntry(host, rpcPort, 1);
        ActiveEntry seed = new ActiveEntry(seedNodeId, 1);
        long currentVersion = seed.getRemoteEntryVersion(self);
        self.setVersion(currentVersion + 1);
        this.ring.set(this.getNodeIndex(self.getId()), self);
        this.ring.set(this.getNodeIndex(seed.getId()), seed);
        ConsistentHashRing recv = seed.exchange(this.getClusterMessage());
        this.merge(recv);
        recv.killRing();
    }

    public static ConsistentHashRing clusterMessageToRing(ClusterMessage recv) {
        List<RingEntry> otherRing = new ArrayList<>();
        for (RingEntryMessage msg : recv.getNodeList()) {
            if (msg.getActive()) {
                otherRing.add(new ActiveEntry(msg));
            } else {
                otherRing.add(new InactiveEntry());
            }
        }
        return new ConsistentHashRing(otherRing);
    }

    synchronized public int getNodeIndex(String key) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            return (int) (ByteBuffer.wrap(md.digest((key).getBytes())).getLong() & 0xffff) % this.size;
        } catch (NoSuchAlgorithmException e) {
            System.err.println("No such algorithm");
            return -1;
        }
    }

    synchronized public int getNodeIndex(RingEntry entry) {
        for (int i = 0; i < this.ring.size(); i++) {
            if (this.ring.get(i) == entry) {
                return i;
            }
        }
        return -1;
    }

    synchronized public RingEntry getNode(String id) {
        int i = this.getNodeIndex(id);
        return this.ring.get(i);
    }


    synchronized public List<RingEntry> getPreferenceList(String key) {
        int start = this.getNodeIndex(key);
        List<RingEntry> preferenceList = new ArrayList<>();
        for (int i = 0; i < this.size; i++) {
            preferenceList.add(this.ring.get(start + i % this.size));
        }
        return preferenceList;
    }

    synchronized public Optional<ActiveEntry> getRandomEntry() {
        Random rand = new Random();
        int start = (int) (rand.nextLong() & Integer.MAX_VALUE) % this.size;
        RingEntry entry = this.ring.get(start);

        // Keep checking until an active entry is found
        for (int i = 0; i < this.size && !(entry instanceof ActiveEntry); i++) {
            entry = this.ring.get((start + i) % this.size);
        }
        if (entry instanceof ActiveEntry active) {
            return Optional.of(active);
        }
        return Optional.empty();
    }

    synchronized public ClusterMessage getClusterMessage() {
        ClusterMessage.Builder builder = ClusterMessage.newBuilder();
        for (RingEntry entry : this.ring) {
            long version = entry.getVersion();
            if (entry instanceof ActiveEntry active) {
                String host = active.getHost();
                int port = active.getPort();
                builder.addNode(RingEntryMessage.newBuilder()
                        .setActive(true)
                        .setHost(host)
                        .setPort(port)
                        .setVersion(version)
                        .build());
            } else {
                builder.addNode(RingEntryMessage.newBuilder()
                        .setActive(false)
                        .setVersion(version)
                        .build());
            }
        }
        return builder.build();
    }

    synchronized void merge(ConsistentHashRing recv) {
        List<RingEntry> otherRing = recv.ring;
        for (int i = 0; i < otherRing.size(); i++) {
            RingEntry local = this.ring.get(i);
            RingEntry other = otherRing.get(i);
            if (local.getVersion() < other.getVersion()) {
                // local is older than other
                if (local instanceof ActiveEntry && other instanceof InactiveEntry) {
                    this.kill(i, other.getVersion());
                } else if (local instanceof InactiveEntry && other instanceof ActiveEntry active) {
                    this.ring.set(i, new ActiveEntry(active.getHost(), active.getPort(), active.getVersion()));
                } else {
                    local.setVersion(other.getVersion());
                }
            } else if (local.getVersion() == other.getVersion()) {
                if (local instanceof ActiveEntry && other instanceof InactiveEntry) {
                    this.kill(i, other.getVersion());
                }
            }
        }
    }

    synchronized void kill(int idx) {
        RingEntry target = this.ring.get(idx);
        if (target instanceof ActiveEntry) {
            ((ActiveEntry) target).kill();
            this.ring.set(idx, new InactiveEntry());
        }
    }

    public synchronized void kill(int idx, long version) {
        this.kill(idx);
        this.ring.get(idx).setVersion(version);
    }

    synchronized void killRing() {
        for (int i = 0; i < this.ring.size(); i++) {
            this.kill(i);
        }
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        for (RingEntry entry : this.ring) {
            str.append(entry.toString()).append(", ");
        }
        return str.toString();
    }
}
