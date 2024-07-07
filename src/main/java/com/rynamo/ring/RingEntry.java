package com.rynamo.ring;

import com.rynamo.grpc.membership.ExchangeMembershipGrpc;
import com.rynamo.grpc.membership.ExchangeMembershipGrpc.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.time.*;
import java.net.URL;

public class RingEntry {

    private String host;
    private int port;
    private Instant updateTime;

    private ExchangeMembershipBlockingStub blockingStub;
    private ExchangeMembershipStub asyncStub;

    public RingEntry() {
        this.updateTime = Instant.now();
    }

    public RingEntry(String host, int port) {
        this.host = host;
        this.port = port;
        this.updateTime = Instant.now();
    }

    public String getHost() {
        return this.host;
    }

    public void setHost(String host) {
        this.host = host;
        this.updateTime = Instant.now();
    }

    public int getPort() {
        return this.port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public Instant getUpdateTime() {
        return this.updateTime;
    }

    public ExchangeMembershipBlockingStub getBlockingStub() {
        return this.blockingStub;
    }

    public ExchangeMembershipStub getAsyncStub() {
        return this.asyncStub;
    }

    public void setBlockingStub(ExchangeMembershipBlockingStub blockingStub) {
        this.blockingStub = blockingStub;
    }

    public void setAsyncStub(ExchangeMembershipStub asyncStub) {
        this.asyncStub = asyncStub;
    }
}