package com.rynamo.ring;

import com.rynamo.coordinate.CoordinateResponse;
import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import org.rocksdb.RocksDBException;

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
        CoordinateResponse response = this.node.coordinateGet(key);
        if (response.R >= 2) {
            ctx.result(response.result);
            ctx.status(200);
            return;
        }
        ctx.status(400);
    }

    private void handlePut(Context ctx) {
        String key = ctx.pathParam("key");
        String val = ctx.pathParam("val");
        CoordinateResponse response = this.node.coordinatePut(key, val.getBytes());
        if (response.W >= 2) {
            ctx.status(200);
            return;
        }
        ctx.status(400);
    }

}
