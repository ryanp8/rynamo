package com.rynamo.ring;

import com.google.protobuf.ByteString;
import com.rynamo.grpc.storage.KeyMessage;
import com.rynamo.grpc.storage.KeyValMessage;
import com.rynamo.grpc.storage.ValueMessage;
import com.rynamo.ring.entry.ActiveEntry;
import com.rynamo.ring.entry.RingEntry;
import io.javalin.Javalin;
import io.javalin.http.Context;

import java.nio.charset.StandardCharsets;
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
                ValueMessage response = activeEntry.coordinateGet(key);
                System.out.println("response: " + response.getSuccess());
                if (response.getSuccess()) {
                    ctx.status(200);
                    ctx.result(response.getValue().toByteArray());
                    return;
                }
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
                ValueMessage response = activeEntry.coordinatePut(key, val.getBytes(StandardCharsets.UTF_8));
                if (response.getSuccess()) {
                    ctx.status(200);
                    ctx.result(response.getValue().toByteArray());
                    return;
                }
            }
        }
        ctx.status(400);
    }

}
