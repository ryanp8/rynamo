package com.rynamo.ring.entry;

public abstract class RingEntry {

    protected long version;

    public long getVersion() {
        return this.version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

}