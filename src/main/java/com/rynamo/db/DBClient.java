package com.rynamo.db;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class DBClient {

    private final RocksDB db;
    public DBClient(int port) throws org.rocksdb.RocksDBException {
        Options options = new Options().setCreateIfMissing(true);
        this.db = RocksDB.open(options,String.format("./tmp/%d", port));
    }

    public byte[] get(String key) {
        byte[] keyBytes = key.getBytes();
        try {
            return this.db.get(keyBytes);
        } catch (RocksDBException e) {
            System.err.println(e.getMessage());
            return null;
        }
    }

    public void put(String key, byte[] value) throws RocksDBException{
        byte[] keyBytes = key.getBytes();
        this.db.put(keyBytes, value);
    }
}
