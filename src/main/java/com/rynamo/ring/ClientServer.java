package com.rynamo.ring;

import com.google.common.primitives.Longs;
import com.google.protobuf.ByteString;
import com.rynamo.grpc.storage.GetResponse;
import com.rynamo.grpc.storage.PutResponse;
import com.rynamo.ring.entry.ActiveEntry;
import com.rynamo.ring.entry.RingEntry;
import io.javalin.Javalin;
import io.javalin.http.Context;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class ClientServer {
    final private Node node;
    final private Javalin server;
    public ClientServer(Node node) {
        this.node = node;
        this.server = Javalin.create()
                .get("/health", this::handleHealth)
                .get("/{key}", this::handleGet)
                .put("/{key}/{val}", this::handlePut);
    }

    public void start(int port) {
        this.server.start(port);
    }

    private void handleHealth(Context ctx) {
        ctx.result("Success");
        ctx.status(200);
    }

    private void handleGet(Context ctx) {
        String key = ctx.pathParam("key");
        List<RingEntry> preferenceList = this.node.getPreferenceList(key);
        for (var entry : preferenceList) {
            if (entry instanceof ActiveEntry activeEntry) {
                try {
                    GetResponse response = activeEntry.coordinateGet(key);
                    if (!response.getValueList().isEmpty()) {
                        ctx.status(200);
                        StringBuilder body = new StringBuilder();
                        for (ByteString value : response.getValueList()) {
                            body.append(new String(value.toByteArray())).append(", ");
                        }
                        ctx.result(body.toString().getBytes(StandardCharsets.UTF_8));
                        return;
                    }
                } catch (Exception e) {}
            }
        }
        ctx.status(400);
    }

    private void handlePut(Context ctx) {
        String key = ctx.pathParam("key");
        String val = ctx.pathParam("val");
        List<RingEntry> preferenceList = this.node.getPreferenceList(key);
        for (var entry : preferenceList) {
            if (entry instanceof ActiveEntry activeEntry) {
                try {
                    PutResponse response = activeEntry.coordinatePut(key, val.getBytes(StandardCharsets.UTF_8));
                    ctx.status(200);
                    ctx.result(Long.toString(response.getVersion()).getBytes(StandardCharsets.UTF_8));
                    return;
                } catch (Exception e) {}
            }
        }
        ctx.status(400);
    }

}
