package com.rynamo.ring.coordinate;

import com.google.protobuf.ByteString;
import com.rynamo.db.Row;
import com.rynamo.grpc.storage.GetResponse;
import com.rynamo.grpc.storage.PutResponse;
import com.rynamo.grpc.storage.StorageGrpc;
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
        System.out.println(key);
        List<RingEntry> preferenceList = this.node.getPreferenceList(key);
        List<byte[]> results = new ArrayList<>();
        int reads = 0;
        for (int i = 0; i < preferenceList.size() && reads < this.node.R; i++) {
            RingEntry entry = preferenceList.get(i);
            if (entry instanceof ActiveEntry active) {
                try {
                    GetResponse response = active.get(key);
                    for (ByteString value : response.getValueList()) {
                        results.add(value.toByteArray());
                    }
                    reads++;
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return new CoordinateResponse(reads, 0, results);
    }

    public CoordinateResponse coordinatePut(String key, long version, byte[] val) {
        List<RingEntry> preferenceList = this.node.getPreferenceList(key);
        System.out.println(preferenceList);
        int writes = 0;
        for (int i = 0; i < preferenceList.size() && writes < this.node.W; i++) {
            RingEntry entry = preferenceList.get(i);
            if (entry instanceof ActiveEntry active) {
                try {
                    PutResponse response = active.put(key, version, val);
                    writes++;
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        if (writes >= this.node.W) {
            this.node.db().setVersion(key, version + 1);
        }
        return new CoordinateResponse(0, writes, null);
    }
}
