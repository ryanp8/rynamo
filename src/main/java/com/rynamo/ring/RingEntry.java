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
        this.host = "";
        this.port = -1;
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

    public Instant getTimestamp() {
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

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        if (this.host.isEmpty()) {
            str.append("{null, ").append(this.updateTime.getEpochSecond()).append("}");
        } else {
            str.append("{").append(this.host).append(":").append(this.port).append(",").append(this.updateTime.getEpochSecond()).append("}");
        }
        return str.toString();
    }
}