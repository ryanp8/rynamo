package com.rynamo.ring.entry;

import com.rynamo.grpc.membership.ClusterMessage;
import com.rynamo.grpc.membership.RingEntryMessage;
import com.rynamo.grpc.storage.StorageGrpc;
import com.rynamo.grpc.storage.StorageGrpc.*;
import com.rynamo.grpc.membership.ExchangeMembershipGrpc;
import com.rynamo.grpc.membership.ExchangeMembershipGrpc.*;
import com.rynamo.ring.ConsistentHashRing;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.List;

public abstract class RingEntry {

    protected long version;

    public long getVersion() {
        return this.version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

}