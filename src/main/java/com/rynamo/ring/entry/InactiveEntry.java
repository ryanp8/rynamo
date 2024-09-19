package com.rynamo.ring.entry;

public class InactiveEntry extends RingEntry {
    public InactiveEntry() {
        this.version = 0;
    }

    @Override
    public String toString() {
        return String.format("(null, %d)", this.version);
    }
}
