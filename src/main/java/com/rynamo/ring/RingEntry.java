package com.rynamo.ring;

import com.rynamo.grpc.keyval.KeyValGrpc.*;
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
    private boolean active;

    private ExchangeMembershipBlockingStub exchangeBlockingStub;
    private KeyValBlockingStub keyValBlockingStub;
    public RingEntry() {
        this.host = "";
        this.port = -1;
        this.active = false;
        this.updateTime = Instant.now();
    }

    public RingEntry(String host, int port) {
        this.host = host;
        this.port = port;
        this.active = false;
        this.updateTime = Instant.now();
    }

    public boolean getActive() {
        return this.active;
    }

    public void setActive(boolean active) {
        this.active = active;
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
        this.updateTime = Instant.now();
    }

    public Instant getTimestamp() {
        return this.updateTime;
    }

    public ExchangeMembershipBlockingStub getExchangeBlockingStub() {
        return this.exchangeBlockingStub;
    }

//    public ExchangeMembershipStub getAsyncStub() {
//        return this.asyncStub;
//    }

    public void setExchangeBlockingStub(ExchangeMembershipBlockingStub blockingStub) {
        this.exchangeBlockingStub = blockingStub;
    }

    public KeyValBlockingStub getKeyValBlockingStub() {
        return this.keyValBlockingStub;
    }

    public void setKeyValBlockingStub(KeyValBlockingStub stub) {
        this.keyValBlockingStub = stub;
    }

//    public void setAsyncStub(ExchangeMembershipStub asyncStub) {
//        this.asyncStub = asyncStub;
//    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        if (this.getActive()) {
            str.append("{").append(this.host).append(":").append(this.port).append(",").append(this.updateTime.getEpochSecond()).append("}");
        } else {
            str.append("{null, ").append(this.updateTime.getEpochSecond()).append("}");
        }
        return str.toString();
    }
}