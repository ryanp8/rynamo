package com.rynamo.ring;

import com.rynamo.grpc.membership.ClusterMessage;
import com.rynamo.grpc.membership.RingEntryMessage;
import com.rynamo.ring.membership.ActiveEntry;
import com.rynamo.ring.membership.InactiveEntry;
import com.rynamo.ring.membership.RingEntry;

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

    /*
    * Initializes the ring by connecting to the seed node
    * */
    public void init(String host, int rpcPort) {
        ActiveEntry self = new ActiveEntry(host, rpcPort, 1);
        ActiveEntry seed = new ActiveEntry(seedNodeId, 1);

        // Check if this node is already in the seed-node's ring
        long currentVersion = seed.getRemoteEntryVersion(self);

        // Increment this node's version, so it is not overridden
        self.setVersion(currentVersion + 1);

        // Insert this node's and the seed's entries into the ring
        this.set(this.getNodeIndex(self.getId()), self);
        this.set(this.getNodeIndex(seed.getId()), seed);

        // Exchange the ring with the seed
        ConsistentHashRing recv = seed.exchange(this.getClusterMessage());

        // Merge the seed's ring with mine
        this.merge(recv);
        recv.killRing();
    }

    /*
    * Sets an index of the ring and kills the channel of the existing entry if it is active.
    * */
    void set(int i, RingEntry newEntry) {
        if (this.ring.get(i) instanceof ActiveEntry active) {
            active.kill();
        }
        this.ring.set(i, newEntry);
    }

    /*
    * Returns the index of the ring that corresponds to the hashed version of the
    * provided key
    * */
    synchronized public int getNodeIndex(String key) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            return (int) (ByteBuffer.wrap(md.digest((key).getBytes())).getLong() & 0xffff) % this.size;
        } catch (NoSuchAlgorithmException e) {
            System.err.println("No such algorithm");
            return -1;
        }
    }

    /*
    * Returns the index of the provided RingEntry. Returns -1 if the entry is not in the ring.
    * */
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

    /*
    * Returns the preference list for the provided key. The preference list contains the same
    * elements as the ring but starts at the node that is responsible for handling the key and
    * loops around from there.
    * */
    synchronized public List<RingEntry> getPreferenceList(String key) {
        int start = this.getNodeIndex(key);
        List<RingEntry> preferenceList = new ArrayList<>();
        for (int i = 0; i < this.size; i++) {
            preferenceList.add(this.ring.get((start + i) % this.size));
        }
        return preferenceList;
    }

    /*
    * Returns an Optional. The optional is set to a random active entry in the ring.
    * If there are no active entries, the optional is empty.
    * */
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

    /*
    * Marshals a ConsistentHashRing into a ClusterMessage, so it can be sent using gRPC.
    * */
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

    /*
     * Unmarshalls gRPC ClusterMessage type into a ConsistentHashRing
     * */
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


    /*
    * Merges the ring passed as the argument into the caller's ring
    * */
    synchronized void merge(ConsistentHashRing recv) {
        List<RingEntry> otherRing = recv.ring;

        // Element-wise compare since the rings are in the same order
        for (int i = 0; i < otherRing.size(); i++) {
            RingEntry local = this.ring.get(i);
            RingEntry other = otherRing.get(i);

            // Check if local version is outdated
            if (local.getVersion() < other.getVersion()) {
                // The first two cases account for when the entry is active in one
                // ring and inactive in the other
                if (local instanceof ActiveEntry && other instanceof InactiveEntry) {
                    // Kill the ring to clean up the channels and update the version
                    this.kill(i, other.getVersion());
                } else if (local instanceof InactiveEntry && other instanceof ActiveEntry active) {
                    // Create a new active entry to populate the old inactive one
                    this.set(i, new ActiveEntry(active.getHost(), active.getPort(), active.getVersion()));
                } else {
                    local.setVersion(other.getVersion());
                }
            } else if (local.getVersion() == other.getVersion()) {
                // Edge case caused by timing.
                // Always just assume the node is active because it is possible that is actually active
                // If it's actually inactive, we'll learn about it eventually and increment its version
                // to accurate reflect that
                if (local instanceof InactiveEntry && other instanceof ActiveEntry active) {
                    this.set(i, new ActiveEntry(active.getHost(), active.getPort(), active.getVersion()));
                }
            }
        }
    }

    /*
    * Cleanly kills the entry of the ring at the provided index.
    * If the entry is active, close the channels. If the entry
    * is inactive, do nothing
    * */
    synchronized void kill(int idx) {
        RingEntry target = this.ring.get(idx);
        if (target instanceof ActiveEntry) {
            ((ActiveEntry) target).kill();
            this.ring.set(idx, new InactiveEntry());
        }
    }

    /*
    * Kills an entry and updates its version
    * */
    public synchronized void kill(int idx, long version) {
        this.kill(idx);
        this.ring.get(idx).setVersion(version);
    }

    /*
    * Kills all entries in the ring.
    * */
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
