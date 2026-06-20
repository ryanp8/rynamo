package com.rynamo.ring.coordinator;

import java.util.List;
import com.rynamo.grpc.storage.Record;

public record CoordinateResponse (int R, int W, List<Record> records) {
    @Override
    public String toString() {
        return this.R + " " + this.W + " " + records;
    }
}
