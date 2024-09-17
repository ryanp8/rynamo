package com.rynamo.coordinate;

import com.google.protobuf.ByteString;
import com.rynamo.grpc.keyval.KeyMessage;
import com.rynamo.grpc.keyval.KeyValGrpc;
import com.rynamo.grpc.keyval.KeyValMessage;
import com.rynamo.grpc.keyval.ValueMessage;
import com.rynamo.ring.Node;
import com.rynamo.ring.RingEntry;
import io.grpc.StatusRuntimeException;

import java.util.*;

public class Coordinator {
    private final Node node;
    public Coordinator(Node node) {
        this.node = node;
    }

    public CoordinateResponse coordinateGet(String key) {
        List<RingEntry> preferenceList = this.node.getPreferenceList(key);
        int reads = 0;
        int nodesReached = 0;
        byte[] oneResponse = {};
        for (int i = 0; i < preferenceList.size() && nodesReached < this.node.N; i++) {
            var entry = preferenceList.get(i);
            try {
                if (entry.getActive()) {
                    nodesReached++;
                    KeyValGrpc.KeyValBlockingStub stub = entry.getKeyValBlockingStub();
                    ValueMessage response = stub.get(KeyMessage.newBuilder().setKey(key).build());
                    if (response.getSuccess()) {
                        oneResponse = response.getValue().toByteArray();
                        if (reads++ == this.node.R) {
                            break;
                        }
                    }
                }
            } catch (StatusRuntimeException e) {
                entry.closeConn();
            }

        }
        return new CoordinateResponse(reads, 0, oneResponse);
    }

    public CoordinateResponse coordinatePut(String key, byte[] val) {
        List<RingEntry> preferenceList = this.node.getPreferenceList(key);
        int writes = 0;
        int nodesReached = 0;
        for (int i = 0; i < preferenceList.size() && nodesReached < this.node.N; i++) {
            var entry = preferenceList.get(i);
            try {
                if (entry.getActive()) {
                    nodesReached++;
                    KeyValGrpc.KeyValBlockingStub stub = entry.getKeyValBlockingStub();
                    ValueMessage response = stub.put(KeyValMessage.newBuilder().setKey(key).setValue(ByteString.copyFrom(val)).build());
                    if (response.getSuccess()) {
                        if (writes++ == this.node.W) {
                            break;
                        }
                    }
                }
            } catch (StatusRuntimeException e) {
                entry.closeConn();
            }

        }
        return new CoordinateResponse(0, writes, null);
    }
}
