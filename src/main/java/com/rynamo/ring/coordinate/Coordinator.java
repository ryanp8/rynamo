package com.rynamo.ring.coordinate;

import com.google.protobuf.ByteString;
import com.rynamo.grpc.storage.KeyMessage;
import com.rynamo.grpc.storage.StorageGrpc;
import com.rynamo.grpc.storage.KeyValMessage;
import com.rynamo.grpc.storage.ValueMessage;
import com.rynamo.ring.Node;
import com.rynamo.ring.entry.ActiveEntry;
import com.rynamo.ring.entry.RingEntry;
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
                if (entry instanceof ActiveEntry active) {
                    nodesReached++;
                    ValueMessage response = active.get(KeyMessage.newBuilder().setKey(key).build());
                    if (response.getSuccess()) {
                        oneResponse = response.getValue().toByteArray();
                        if (reads++ == this.node.R) {
                            break;
                        }
                    }
                }
            } catch (StatusRuntimeException e) {
                this.node.getRing().kill(i, entry.getVersion() + 1);
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
                if (entry instanceof ActiveEntry active) {
                    nodesReached++;
                    ValueMessage response = active.put(KeyValMessage.newBuilder()
                            .setKey(key).setValue(ByteString.copyFrom(val)).build());
                    if (response.getSuccess()) {
                        if (writes++ == this.node.W) {
                            break;
                        }
                    }
                }
            } catch (StatusRuntimeException e) {
                this.node.getRing().kill(i, entry.getVersion() + 1);
            }

        }
        return new CoordinateResponse(0, writes, null);
    }
}
