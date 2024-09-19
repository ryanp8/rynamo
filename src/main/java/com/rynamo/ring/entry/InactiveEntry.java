package com.rynamo.ring.entry;

public class InactiveEntry extends RingEntry {
    public InactiveEntry() {
        this.version = 0;
    }

    @Override
    public String toString() {
        return "no entry";
    }
}
