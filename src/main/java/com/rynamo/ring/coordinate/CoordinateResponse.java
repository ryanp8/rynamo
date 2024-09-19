package com.rynamo.ring.coordinate;

public class CoordinateResponse {
    public final int R;
    public final int W;
    public final byte[] result;

    public CoordinateResponse(int R, int W, byte[] result) {
        this.R = R;
        this.W = W;
        this.result = result;
    }

    @Override
    public String toString() {
        return this.R + " " + this.W + " " + this.result;
    }
}
