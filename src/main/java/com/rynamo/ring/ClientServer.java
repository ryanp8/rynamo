package com.rynamo.ring;

import com.google.protobuf.ByteString;
import com.rynamo.coordinate.CoordinateResponse;
import com.rynamo.grpc.keyval.KeyMessage;
import com.rynamo.grpc.keyval.KeyValMessage;
import com.rynamo.grpc.keyval.ValueMessage;
import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import org.rocksdb.RocksDBException;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class ClientServer {
    final private Node node;
    public ClientServer(int port, Node node) {
        this.node = node;
        Javalin.create()
                .get("/{key}", this::handleGet)
                .put("/{key}/{val}", this::handlePut)
                .start(port);
    }

    private void handleGet(Context ctx) {
        String key = ctx.pathParam("key");
        List<RingEntry> preferenceList = this.node.getPreferenceList(key);
        for (var entry : preferenceList) {
            if (entry.getActive()) {
                ValueMessage response = entry.getKeyValBlockingStub().forwardCoordinateGet(KeyMessage.newBuilder().setKey(key).build());
                if (response.getSuccess()) {
                    ctx.status(200);
                    ctx.result(response.getValue().toByteArray());
                }
                return;
            }
        }
        ctx.status(400);
    }

    private void handlePut(Context ctx) {
        String key = ctx.pathParam("key");
        String val = ctx.pathParam("val");
        List<RingEntry> preferenceList = this.node.getPreferenceList(key);
        for (var entry : preferenceList) {
            if (entry.getActive()) {
                KeyValMessage request = KeyValMessage.newBuilder()
                        .setKey(key).setValue(ByteString.copyFrom(val, StandardCharsets.UTF_8)).build();
                ValueMessage response = entry.getKeyValBlockingStub().forwardCoordinatePut(request);
                if (response.getSuccess()) {
                    ctx.status(200);
                    ctx.result(response.getValue().toByteArray());
                }
                return;
            }
        }
        ctx.status(400);
    }

}
