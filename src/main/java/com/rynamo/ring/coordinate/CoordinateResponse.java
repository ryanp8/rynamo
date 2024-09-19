package com.rynamo.ring.coordinate;

import com.rynamo.db.Row;

import java.util.List;

public record CoordinateResponse (int R, int W, List<byte[]> values) {
    @Override
    public String toString() {
        return this.R + " " + this.W + " " + values;
    }
}
